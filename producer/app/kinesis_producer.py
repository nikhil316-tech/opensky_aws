import json
import boto3

from app.config import settings
from app.logger import get_logger

logger = get_logger(__name__)


class KinesisProducer:

    def __init__(self):

        self.client = boto3.client(
            "kinesis",
            region_name=settings.aws_region
        )

        self.stream_name = settings.stream_name

    def publish_batch(self, records):

        batch_size = 500

        total_success = 0
        total_failed = 0

        for i in range(0, len(records), batch_size):

            batch = records[i:i + batch_size]

            entries = []

            for record in batch:

                entries.append(
                    {
                        "Data": json.dumps(record),
                        "PartitionKey": record["icao24"]
                    }
                )

            response = self.client.put_records(
                StreamName=self.stream_name,
                Records=entries
            )

            success = len(entries) - response["FailedRecordCount"]

            total_success += success
            total_failed += response["FailedRecordCount"]

            logger.info(
                "Batch %d-%d | Success=%d | Failed=%d",
                i + 1,
                i + len(batch),
                success,
                response["FailedRecordCount"]
            )

        logger.info(
            "Publishing completed | Total Success=%d | Total Failed=%d",
            total_success,
            total_failed
        )

        return {
            "Success": total_success,
            "Failed": total_failed
        }