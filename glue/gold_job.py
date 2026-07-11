import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext

from pyspark.sql.functions import *

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "silver_database",
        "silver_table",
        "gold_bucket"
    ]
)

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args["JOB_NAME"], args)

silver_df = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=args["silver_database"],
        table_name=args["silver_table"]
    )
    .toDF()
)

country_df = (
    silver_df
    .groupBy("origin_country")
    .agg(
        count("*").alias("total_flights"),

        avg("velocity").alias("avg_speed"),

        avg("geo_altitude").alias("avg_altitude"),

        sum(
            when(col("is_moving"),1).otherwise(0)
        ).alias("moving_flights"),

        sum(
            when(col("flight_status")=="On Ground",1).otherwise(0)
        ).alias("grounded_flights")
    )
)

country_df.write \
    .mode("overwrite") \
    .parquet(
        args["gold_bucket"] + "/flights_by_country/"
    )

speed_df = (
    silver_df
    .groupBy("speed_category")
    .agg(
        count("*").alias("flight_count")
    )
)

speed_df.write \
    .mode("overwrite") \
    .parquet(
        args["gold_bucket"] + "/speed_summary/"
    )

altitude_df = (
    silver_df
    .groupBy("altitude_category")
    .agg(
        count("*").alias("flight_count")
    )
)

altitude_df.write \
    .mode("overwrite") \
    .parquet(
        args["gold_bucket"] + "/altitude_summary/"
    )

status_df = (
    silver_df
    .groupBy("flight_status")
    .agg(
        count("*").alias("flight_count")
    )
)

status_df.write \
    .mode("overwrite") \
    .parquet(
        args["gold_bucket"] + "/flight_status/"
    )

day_df = (
    silver_df
    .groupBy("flight_day")
    .agg(
        count("*").alias("flight_count")
    )
)

day_df.write \
    .mode("overwrite") \
    .parquet(
        args["gold_bucket"] + "/daily_summary/"
    )

job.commit()