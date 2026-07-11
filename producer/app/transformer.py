from app.models import FlightRecord


class FlightTransformer:

    @staticmethod
    def transform(flight: dict) -> FlightRecord:
        return FlightRecord(
            icao24=flight["icao24"],

            callsign=flight.get("callsign").strip()
            if flight.get("callsign")
            else None,

            origin_country=flight["origin_country"],

            time_position=flight.get("time_position"),

            last_contact=flight.get("last_contact"),

            longitude=flight.get("longitude"),

            latitude=flight.get("latitude"),

            baro_altitude=flight.get("baro_altitude"),

            on_ground=flight.get("on_ground", False),

            velocity=flight.get("velocity"),

            true_track=flight.get("true_track"),

            vertical_rate=flight.get("vertical_rate"),

            sensors=flight.get("sensors"),

            geo_altitude=flight.get("geo_altitude"),

            squawk=flight.get("squawk"),

            spi=flight.get("spi", False),

            position_source=flight.get("position_source", 0)
        )