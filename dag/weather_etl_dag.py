"""Daily ETL: Open-Meteo API -> pandas -> data quality checks -> BigQuery.

Tasks:  extract (one per city)  ->  transform  ->  quality_checks  ->  load
Retries and a failure callback are configured so transient API errors recover on their own and
real failures are logged (add email or Slack alerts in `notify_failure`).
"""
import logging
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

import pandas as pd

from etl.config import CITIES
from etl.extract import fetch_weather
from etl.load import load_to_bigquery
from etl.quality import run_checks
from etl.transform import combine, to_dataframe

log = logging.getLogger(__name__)


def notify_failure(context):
    """Failure callback. Replace the log line with an email or Slack notification if desired."""
    ti = context.get("task_instance")
    log.error("Task failed: dag=%s task=%s run=%s", ti.dag_id, ti.task_id, context.get("run_id"))


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="weather_api_to_bigquery",
    description="Daily weather ETL from Open-Meteo into BigQuery",
    schedule="0 2 * * *",  # 02:00 every day
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "bigquery", "weather"],
)
def weather_api_to_bigquery():
    @task
    def extract(city: str, lat: float, lon: float) -> dict:
        return {"city": city, "raw": fetch_weather(lat, lon)}

    @task
    def transform(payloads: list) -> list:
        frames = [to_dataframe(p["raw"], p["city"]) for p in payloads]
        df = combine(frames)
        # Convert to JSON-friendly records for XCom (dates and timestamps as strings)
        return df.astype({"obs_date": str, "ingested_at": str}).to_dict("records")

    @task
    def quality_checks(records: list) -> list:
        df = pd.DataFrame(records)
        results = run_checks(df, expected_cities=len(CITIES))
        log.info("Quality checks passed: %s", results)
        return records

    @task
    def load(records: list) -> int:
        df = pd.DataFrame(records)
        df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
        rows = load_to_bigquery(df)
        log.info("Loaded %d rows into BigQuery", rows)
        return rows

    extracted = [extract.override(task_id=f"extract_{c.lower()}")(c, lat, lon) for c, (lat, lon) in CITIES.items()]
    load(quality_checks(transform(extracted)))


weather_api_to_bigquery()
