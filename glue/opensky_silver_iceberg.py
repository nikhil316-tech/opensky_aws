import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext

from pyspark.sql.functions import *

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    IntegerType,
    TimestampType
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
# Spark
# ----------------------------------------------------

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

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
        from_json(
            col("$json$data_infer_schema$_temporary$"),
            flight_schema
        ).alias("flight"),
        col("processed_timestamp")
    )
    .select("flight.*", "processed_timestamp")
)

silver_df=parsed_df.filter(
    col("icao24").isNotNull() & 
    col("latitude").isNotNull() & 
    col("longitude").isNotNull()
)

silver_df=silver_df.dropDuplicates(
    ["icao24","last_contact"]
)

silver_df=silver_df.withColumn(
    "callsign",
    trim(col("callsign"))
)

silver_df=silver_df.withColumn(
    "flight_date",
    to_date(col("ingestion_timestamp"))
)
silver_df=silver_df.drop(
    "sensors",
    "position_source"
    )

silver_df = silver_df.withColumn(
    "speed_category",
    when(col("velocity") < 100, "Low Speed")
    .when(col("velocity") < 300, "Cruise")
    .otherwise("High Speed")
)

silver_df = silver_df.withColumn(
    "altitude_category",
    when(col("geo_altitude") < 3000, "Low")
    .when(col("geo_altitude") < 9000, "Medium")
    .otherwise("High")
)

silver_df = silver_df.withColumn(
    "flight_day",
    date_format(col("ingestion_timestamp"), "EEEE")
)

silver_df = silver_df.withColumn(
    "is_moving",
    col("velocity") > 0
)

silver_df = silver_df.withColumn(
    "flight_status",
    when(col("on_ground"), "On Ground")
    .otherwise("In Air")
)

silver_df = silver_df.withColumn(
    "speed_kmh",
    round(col("velocity") * 3.6, 2)
)

print("========================================")
print(f"Records after transformations: {silver_df.count()}")
print("========================================")

silver_df.printSchema()

silver_df.writeTo(
    "glue_catalog.opensky_iceberg.flight_silver"
).append()

job.commit()