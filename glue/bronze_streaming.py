import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *



#jobarguments
args=getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "stream_name",
        "region",
        "output_path",
        "checkpoint_path"
    ]
)

#spark and glue

sc=SparkContext()
glueContext=GlueContext(sc)

spark=glueContext.spark_session

job=Job(glueContext)

job.init(args["JOB_NAME"],args)

#kinesis source

kinesis_options = {
    "streamARN": f"arn:aws:kinesis:{args['region']}:xxxxxxxxxxx:stream/{args['stream_name']}",
    "startingPosition": "LATEST",
    "inferSchema": "true",
    "classification": "json"
}

stream_df = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options=kinesis_options
)

# Batch Processing
##########################################################

def processBatch(data_frame, batchId):

    if data_frame.count() == 0:
        print(f"Batch {batchId} Empty")
        return

    print(f"Processing Batch : {batchId}")

    df = (
        data_frame
        .withColumn("processed_timestamp", current_timestamp())
    )

    (
        df.write
        .mode("append")
        .parquet(args["output_path"])
    )

    print(f"Batch {batchId} Written Successfully")

##########################################################
# Streaming
##########################################################

glueContext.forEachBatch(
    frame=stream_df,
    batch_function=processBatch,
    options={
        "windowSize": "60 seconds",
        "checkpointLocation": args["checkpoint_path"]
    }
)

job.commit()