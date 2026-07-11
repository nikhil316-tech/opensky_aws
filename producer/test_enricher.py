from app.opensky_client import OpenSkyClient
from app.transformer import FlightTransformer
from app.enricher import FlightEnricher

client = OpenSkyClient()

flight = client.get_flights()[0]

record = FlightTransformer.transform(flight)

enriched = FlightEnricher.enrich(record)

print(enriched)