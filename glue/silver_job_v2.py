
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    IntegerType
)

# ----------------------------------------------------
# Job Arguments
# ----------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bronze_database",
        "bronze_table"
    ]
)

# ----------------------------------------------------
# Spark and Glue
# ----------------------------------------------------

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


target_table = "glue_catalog.opensky_iceberg.flight_silver"

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS glue_catalog.opensky_iceberg
""")

# ----------------------------------------------------
# Flight Schema
# ----------------------------------------------------

flight_schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("sensors", StringType()),
    StructField("geo_altitude", DoubleType()),
    StructField("squawk", StringType()),
    StructField("spi", BooleanType()),
    StructField("position_source", IntegerType()),
    StructField("record_id", StringType()),
    StructField("source", StringType()),
    StructField("pipeline_version", StringType()),
    StructField("ingestion_timestamp", StringType())
])

# ----------------------------------------------------
# Read Bronze Table
# ----------------------------------------------------

bronze_df = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=args["bronze_database"],
        table_name=args["bronze_table"]
    )
    .toDF()
)

# ----------------------------------------------------
# Parse JSON
# ----------------------------------------------------

parsed_df = (
    bronze_df
    .select(
        F.from_json(
            F.col("$json$data_infer_schema$_temporary$"),
            flight_schema
        ).alias("flight"),
        F.col("processed_timestamp")
    )
    .select("flight.*", "processed_timestamp")
)

# ----------------------------------------------------
# Data Quality Filtering
# ----------------------------------------------------

silver_df = parsed_df.filter(
    F.col("icao24").isNotNull()
    & F.col("last_contact").isNotNull()
    & F.col("latitude").isNotNull()
    & F.col("longitude").isNotNull()
)

# ----------------------------------------------------
# Deduplicate
# ----------------------------------------------------
# One source record per aircraft is required for MERGE.
# Retain the latest observation in the current batch.

window_spec = (
    Window
    .partitionBy("icao24")
    .orderBy(
        F.col("last_contact").desc(),
        F.col("processed_timestamp").desc_nulls_last(),
        F.col("record_id").desc_nulls_last()
    )
)

silver_df = (
    silver_df
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# ----------------------------------------------------
# Transformations
# ----------------------------------------------------

silver_df = silver_df.withColumn(
    "callsign",
    F.trim(F.col("callsign"))
)

silver_df = silver_df.withColumn(
    "flight_date",
    F.to_date("ingestion_timestamp")
)

silver_df = silver_df.drop(
    "sensors",
    "position_source"
)

silver_df = silver_df.withColumn(
    "speed_category",
    F.when(F.col("velocity").isNull(), None)
     .when(F.col("velocity") < 100, "Low Speed")
     .when(F.col("velocity") < 300, "Cruise")
     .otherwise("High Speed")
)

silver_df = silver_df.withColumn(
    "altitude_category",
    F.when(F.col("geo_altitude").isNull(), None)
     .when(F.col("geo_altitude") < 3000, "Low")
     .when(F.col("geo_altitude") < 9000, "Medium")
     .otherwise("High")
)

silver_df = silver_df.withColumn(
    "flight_day",
    F.date_format("ingestion_timestamp", "EEEE")
)

silver_df = silver_df.withColumn(
    "is_moving",
    F.col("velocity") > 0
)

silver_df = silver_df.withColumn(
    "flight_status",
    F.when(F.col("on_ground"), "On Ground")
     .otherwise("In Air")
)

silver_df = silver_df.withColumn(
    "speed_kmh",
    F.round(F.col("velocity") * 3.6, 2)
)

# ----------------------------------------------------
# Create Source View
# ----------------------------------------------------

silver_df.createOrReplaceTempView("silver_source")

record_count = silver_df.count()

print("========================================")
print(f"Records after transformations: {record_count}")
print("========================================")

silver_df.printSchema()

# ----------------------------------------------------
# Create Iceberg Table
# ----------------------------------------------------
# Create an empty Iceberg table on the first run.
# Subsequent runs preserve the existing table.

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table}
    USING iceberg
    AS
    SELECT *
    FROM silver_source
    WHERE 1 = 0
""")

# ----------------------------------------------------
# MERGE INTO - UPSERT
# ----------------------------------------------------
# Match by aircraft ICAO24.
#
# Update only when the incoming observation is newer.
# Insert when the aircraft does not exist.
#
# Ignore older observations to prevent stale updates.

if record_count > 0:

    columns = silver_df.columns

    update_clause = ", ".join(
        f"target.`{c}` = source.`{c}`"
        for c in columns
        if c != "icao24"
    )

    insert_columns = ", ".join(
        f"`{c}`" for c in columns
    )

    insert_values = ", ".join(
        f"source.`{c}`" for c in columns
    )

    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING silver_source AS source

        ON target.icao24 = source.icao24

        WHEN MATCHED
             AND source.last_contact > target.last_contact
        THEN UPDATE SET
            {update_clause}

        WHEN NOT MATCHED
        THEN INSERT (
            {insert_columns}
        )
        VALUES (
            {insert_values}
        )
    """

    spark.sql(merge_sql)

    print("Iceberg MERGE completed successfully.")

else:
    print("No valid records to merge.")

# ----------------------------------------------------
# Validation
# ----------------------------------------------------

print("========================================")
print("Silver table record count:")

spark.sql(f"""
    SELECT COUNT(*) AS total_records
    FROM {target_table}
""").show()

print("========================================")

# ----------------------------------------------------
# Commit
# ----------------------------------------------------

job.commit()