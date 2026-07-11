from app.opensky_client import OpenSkyClient
from app.transformer import FlightTransformer

client = OpenSkyClient()

flights = client.get_flights()

record = FlightTransformer.transform(flights[0])

print(type(record))

print()

print(record)

print()

print(record.model_dump())