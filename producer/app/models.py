from typing import Optional

from pydantic import BaseModel


class FlightRecord(BaseModel):
    icao24: str
    callsign: Optional[str] = None
    origin_country: str
    time_position: Optional[int] = None
    last_contact: Optional[int] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    baro_altitude: Optional[float] = None
    on_ground: bool
    velocity: Optional[float] = None
    true_track: Optional[float] = None
    vertical_rate: Optional[float] = None
    sensors: Optional[list[int]] = None
    geo_altitude: Optional[float] = None
    squawk: Optional[str] = None
    spi: bool
    position_source: int