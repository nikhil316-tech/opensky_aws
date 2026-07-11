from app.opensky_client import OpenSkyClient

client = OpenSkyClient()

flights = client.get_flights()

print(f"Flights: {len(flights)}")

print(flights[0])