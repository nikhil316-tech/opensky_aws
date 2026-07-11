from datetime import datetime, timezone
import uuid

from app.models import FlightRecord


class FlightEnricher:

    @staticmethod
    def enrich(record: FlightRecord) -> dict:

        enriched = record.model_dump()

        enriched["record_id"] = str(uuid.uuid4())

        enriched["source"] = "opensky"

        enriched["pipeline_version"] = "1.0"

        enriched["ingestion_timestamp"] = (
            datetime.now(timezone.utc).isoformat()
        )

        return enriched