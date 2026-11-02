"""Pipeline configuration. Override the BigQuery settings with environment variables."""
import os

# Cities to ingest: name -> (latitude, longitude)
CITIES = {
    "Chennai": (13.0827, 80.2707),
    "Bengaluru": (12.9716, 77.5946),
    "Mumbai": (19.0760, 72.8777),
    "Delhi": (28.6139, 77.2090),
}

API_URL = "https://api.open-meteo.com/v1/forecast"
DAILY_FIELDS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]
PAST_DAYS = 7  # re-pull the last week every run so late corrections are picked up

BQ_PROJECT = os.environ.get("GCP_PROJECT", "your-project-id")
BQ_DATASET = os.environ.get("BQ_DATASET", "weather_dw")
BQ_TABLE = os.environ.get("BQ_TABLE", "daily_weather")
BQ_STAGING_TABLE = f"{BQ_TABLE}_staging"
