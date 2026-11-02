"""Extract: pull daily weather for one city from the free Open-Meteo REST API (no API key)."""
import time

import requests

from etl.config import API_URL, DAILY_FIELDS, PAST_DAYS


def fetch_weather(lat: float, lon: float, retries: int = 3, backoff: float = 2.0) -> dict:
    """Return the raw JSON response. Retries on network errors and 5xx responses."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ",".join(DAILY_FIELDS),
        "past_days": PAST_DAYS,
        "forecast_days": 1,
        "timezone": "Asia/Kolkata",
    }
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            if resp.status_code >= 500:
                raise requests.HTTPError(f"server error {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"Open-Meteo request failed after {retries} attempts: {last_error}")
