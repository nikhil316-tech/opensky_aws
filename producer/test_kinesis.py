from app.opensky_client import OpenSkyClient
from app.transformer import FlightTransformer
from app.enricher import FlightEnricher
from app.kinesis_producer import KinesisProducer

client = OpenSkyClient()
producer = KinesisProducer()

flights = client.get_flights()

records = []

for flight in flights[:10]:
    transformed = FlightTransformer.transform(flight)
    enriched = FlightEnricher.enrich(transformed)
    records.append(enriched)

response = producer.publish_batch(records)

print(response) 