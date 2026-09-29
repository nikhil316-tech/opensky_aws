
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, LongType


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "silver_database",
        "silver_table",
        "gold_bucket",
        "gold_database"
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# Read Silver
# --------------------------------------------------

silver_df = (
    glueContext.create_dynamic_frame.from_catalog(
        database=args["silver_database"],
        table_name=args["silver_table"]
    )
    .toDF()
)

gold_database = args["gold_database"]
gold_bucket = args["gold_bucket"].rstrip("/")

# --------------------------------------------------
# SCD TYPE 2 FUNCTION
# --------------------------------------------------

def apply_scd2(df, table_name, key_columns, tracked_columns):
    """
    Maintains an Iceberg Gold table using SCD Type 2.

    Changed record:
        Expire the current version and insert a new version.

    Unchanged record:
        Do nothing.

    New record:
        Insert version 1.

    All changes are applied using MERGE INTO.
    """

    table = f"glue_catalog.{gold_database}.{table_name}"
    location = f"{gold_bucket}/{table_name}/"

    # Add SCD metadata to the incoming aggregate.
    source = (
        df
        .withColumn("effective_from", F.current_timestamp())
        .withColumn(
            "effective_to",
            F.lit(None).cast(TimestampType())
        )
        .withColumn("is_current", F.lit(True))
        .withColumn("version", F.lit(1).cast(LongType()))
    )

    # Create an empty Iceberg table on its first run.
    # The empty table has the same schema as the source.
    initial = source.limit(0)
    initial.createOrReplaceTempView("scd_initial")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING iceberg
        LOCATION '{location}'
        AS SELECT * FROM scd_initial
    """)

    current = (
        spark.table(table)
        .filter(F.col("is_current") == True)
    )

    # Join incoming aggregates with current versions.
    join_condition = None

    for key in key_columns:
        condition = F.col(f"n.`{key}`").eqNullSafe(
            F.col(f"c.`{key}`")
        )
        join_condition = (
            condition if join_condition is None
            else join_condition & condition
        )

    joined = source.alias("n").join(
        current.alias("c"),
        join_condition,
        "left"
    )

    # Identify new records and changed records.
    changed_condition = None

    for column in tracked_columns:
        condition = ~F.col(f"n.`{column}`").eqNullSafe(
            F.col(f"c.`{column}`")
        )
        changed_condition = (
            condition if changed_condition is None
            else changed_condition | condition
        )

    is_new = F.col("c.version").isNull()

    # Expire only existing records whose tracked values changed.
    changed_existing = joined.filter(
        (~is_new) & changed_condition
    )

    expire_columns = [
        F.col(f"c.`{column}`").alias(column)
        for column in df.columns
    ]

    expire_rows = (
        changed_existing
        .select(*expire_columns)
        .withColumn("_action", F.lit("EXPIRE"))
    )

    # Insert new records and new versions of changed records.
    new_or_changed = joined.filter(
        is_new | changed_condition
    )

    insert_columns = [
        F.col(f"n.`{column}`").alias(column)
        for column in df.columns
    ]

    insert_rows = (
        new_or_changed
        .select(
            *insert_columns,
            F.current_timestamp().alias("effective_from"),
            F.lit(None).cast(TimestampType()).alias("effective_to"),
            F.lit(True).alias("is_current"),
            (
                F.coalesce(
                    F.col("c.version"),
                    F.lit(0)
                ) + F.lit(1)
            ).cast(LongType()).alias("version")
        )
        .withColumn("_action", F.lit("INSERT"))
    )

    staged = expire_rows.unionByName(insert_rows)

    # Nothing has changed. No MERGE is necessary.
    if staged.limit(1).count() == 0:
        print(f"No changes detected for {table_name}")
        return

    staged.createOrReplaceTempView("scd_staged")

    # Match only EXPIRE actions to existing current records.
    # INSERT actions deliberately do not match, so they are
    # inserted as new SCD Type 2 versions.
    merge_condition = " AND ".join(
        [
            f"t.`{key}` <=> s.`{key}`"
            for key in key_columns
        ]
    )

    merge_condition += " AND s._action = 'EXPIRE'"

    spark.sql(f"""
        MERGE INTO {table} AS t
        USING scd_staged AS s
        ON {merge_condition}

        WHEN MATCHED AND t.is_current = true
            THEN UPDATE SET
                t.effective_to = s.effective_from,
                t.is_current = false
    """)

    # Insert new and changed versions.
    # The INSERT action rows do not match the MERGE condition.
    spark.sql(f"""
        MERGE INTO {table} AS t
        USING scd_staged AS s
        ON {merge_condition}

        WHEN NOT MATCHED AND s._action = 'INSERT'
            THEN INSERT (
                {", ".join(f"`{c}`" for c in df.columns)},
                effective_from,
                effective_to,
                is_current,
                version
            )
            VALUES (
                {", ".join(f"s.`{c}`" for c in df.columns)},
                s.effective_from,
                s.effective_to,
                s.is_current,
                s.version
            )
    """)

    print(f"SCD Type 2 completed for {table_name}")


# --------------------------------------------------
# 1. FLIGHTS BY COUNTRY
# --------------------------------------------------

country_df = (
    silver_df
    .groupBy("origin_country")
    .agg(
        F.count("*").alias("total_flights"),
        F.avg("velocity").alias("avg_speed"),
        F.avg("geo_altitude").alias("avg_altitude"),
        F.sum(
            F.when(F.col("is_moving"), 1).otherwise(0)
        ).alias("moving_flights"),
        F.sum(
            F.when(
                F.col("flight_status") == "On Ground", 1
            ).otherwise(0)
        ).alias("grounded_flights")
    )
)

apply_scd2(
    country_df,
    "flights_by_country",
    ["origin_country"],
    [
        "total_flights",
        "avg_speed",
        "avg_altitude",
        "moving_flights",
        "grounded_flights"
    ]
)


# --------------------------------------------------
# 2. SPEED SUMMARY
# --------------------------------------------------

speed_df = (
    silver_df
    .groupBy("speed_category")
    .agg(
        F.count("*").alias("flight_count")
    )
)

apply_scd2(
    speed_df,
    "speed_summary",
    ["speed_category"],
    ["flight_count"]
)


# --------------------------------------------------
# 3. ALTITUDE SUMMARY
# --------------------------------------------------

altitude_df = (
    silver_df
    .groupBy("altitude_category")
    .agg(
        F.count("*").alias("flight_count")
    )
)

apply_scd2(
    altitude_df,
    "altitude_summary",
    ["altitude_category"],
    ["flight_count"]
)


# --------------------------------------------------
# 4. FLIGHT STATUS
# --------------------------------------------------

status_df = (
    silver_df
    .groupBy("flight_status")
    .agg(
        F.count("*").alias("flight_count")
    )
)

apply_scd2(
    status_df,
    "flight_status",
    ["flight_status"],
    ["flight_count"]
)


# --------------------------------------------------
# 5. DAILY SUMMARY
# --------------------------------------------------

day_df = (
    silver_df
    .groupBy("flight_day")
    .agg(
        F.count("*").alias("flight_count")
    )
)

apply_scd2(
    day_df,
    "daily_summary",
    ["flight_day"],
    ["flight_count"]
)

job.commit()