from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    aws_region: str = Field(default="ap-south-1")
    stream_name: str = Field(default="opensky-dev-stream")

    opensky_url: str = "https://opensky-network.org/api/states/all"
    
    max_records: int = 500

    poll_interval: int = Field(default=60)

    rate_limit_backoff: int = Field(default=900)

    class Config:
        env_file = ".env"


settings = Settings()