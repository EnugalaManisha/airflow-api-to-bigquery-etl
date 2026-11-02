"""Transform: turn the raw API JSON into a clean, typed DataFrame."""
from datetime import datetime, timezone

import pandas as pd

COLUMNS = ["city", "obs_date", "temp_max_c", "temp_min_c", "temp_avg_c", "precipitation_mm", "ingested_at"]


def to_dataframe(raw: dict, city: str) -> pd.DataFrame:
    """Flatten the 'daily' block, enforce types, derive temp_avg_c and drop duplicate days."""
    daily = raw.get("daily")
    if not daily or "time" not in daily:
        raise ValueError(f"No 'daily' data in API response for {city}")

    df = pd.DataFrame(
        {
            "city": city,
            "obs_date": pd.to_datetime(daily["time"]).date,
            "temp_max_c": pd.to_numeric(daily.get("temperature_2m_max"), errors="coerce"),
            "temp_min_c": pd.to_numeric(daily.get("temperature_2m_min"), errors="coerce"),
            "precipitation_mm": pd.to_numeric(daily.get("precipitation_sum"), errors="coerce"),
        }
    )
    df["temp_avg_c"] = ((df["temp_max_c"] + df["temp_min_c"]) / 2).round(2)
    df["ingested_at"] = datetime.now(timezone.utc)
    df = df.drop_duplicates(subset=["city", "obs_date"], keep="last").reset_index(drop=True)
    return df[COLUMNS]


def combine(frames: list) -> pd.DataFrame:
    """Stack the per-city frames into one."""
    if not frames:
        raise ValueError("No data frames to combine")
    return pd.concat(frames, ignore_index=True)
