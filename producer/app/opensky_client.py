from typing import Any

STATE_INDEX = {
    "icao24": 0,
    "callsign": 1,
    "origin_country": 2,
    "time_position": 3,
    "last_contact": 4,
    "longitude": 5,
    "latitude": 6,
    "baro_altitude": 7,
    "on_ground": 8,
    "velocity": 9,
    "true_track": 10,
    "vertical_rate": 11,
    "sensors": 12,
    "geo_altitude": 13,
    "squawk": 14,
    "spi": 15,
    "position_source": 16,
}

import httpx
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential

from app.config import settings
from app.logger import get_logger

logger = get_logger(__name__)


class OpenSkyClient:

    def __init__(self):
        self.client = httpx.Client(timeout=30)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2)
    )
    def fetch_states(self):

        logger.info("Fetching latest flight data")

        response = self.client.get(settings.opensky_url)

        response.raise_for_status()

        return response.json()
    
    def extract_states(self, response: dict) -> list:

        if "states" not in response:
            logger.error("Missing states field")
            return []

        if response["states"] is None:
            return []

        return response["states"]

    def state_to_dict(self, state: list) -> dict:

        return {
            key: state[index] if index < len(state) else None
            for key, index in STATE_INDEX.items()
        }
        
    def get_flights(self):

        response = self.fetch_states()

        states = self.extract_states(response)

        flights = [
            self.state_to_dict(state)
            for state in states
        ]

        logger.info("Retrieved %d flights", len(flights))

        return flights


