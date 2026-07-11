import time
import httpx

from app.config import settings
from app.enricher import FlightEnricher
from app.kinesis_producer import KinesisProducer
from app.logger import get_logger
from app.opensky_client import OpenSkyClient
from app.transformer import FlightTransformer

logger = get_logger(__name__)


class ProducerService:

    def __init__(self):
        self.client = OpenSkyClient()
        self.producer = KinesisProducer()

    def run(self):

        logger.info("Starting OpenSky Streaming Producer...")

        while True:

            try:

                logger.info("Fetching latest flight data...")

                flights = self.client.get_flights()[:settings.max_records]

                logger.info("Fetched %d flights", len(flights))

                records = []

                for flight in flights:

                    try:

                        transformed = FlightTransformer.transform(flight)

                        enriched = FlightEnricher.enrich(transformed)

                        records.append(enriched)

                    except Exception as e:

                        logger.exception(
                            "Failed to process aircraft %s : %s",
                            flight.get("icao24"),
                            e
                        )

                if records:

                    response = self.producer.publish_batch(records)

                    print(response)

                    logger.info(
                        "Successfully published %d records | Failed: %d",
                        response['Success'],
                        response["Failed"]
                    )

                else:

                    logger.warning("No valid records to publish.")

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    logger.warning(
                        f"OpenSky rate limit exceeded. Sleeping for {settings.rate_limit_backoff} seconds."
                    )
                    time.sleep(settings.rate_limit_backoff)
                else:
                    logger.exception("Producer iteration failed.")
                    time.sleep(settings.poll_interval)

            except Exception:
                logger.exception("Producer iteration failed.")
                time.sleep(settings.poll_interval)


if __name__ == "__main__":

    try:

        ProducerService().run()

    except KeyboardInterrupt:

        logger.info("Producer stopped by user.")