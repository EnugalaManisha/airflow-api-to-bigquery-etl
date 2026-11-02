"""Data quality checks that run before anything is loaded into BigQuery."""
import pandas as pd


class DataQualityError(Exception):
    """Raised when one or more checks fail, so the Airflow task fails and nothing is loaded."""


def run_checks(df: pd.DataFrame, expected_cities: int | None = None) -> dict:
    """Run all checks. Returns a dict of check name -> bool; raises DataQualityError if any fail."""
    results = {
        "not_empty": len(df) > 0,
        "no_null_keys": bool(df[["city", "obs_date"]].notna().all().all()) if len(df) else False,
        "no_duplicate_keys": not df.duplicated(subset=["city", "obs_date"]).any(),
        "temp_in_plausible_range": bool(df["temp_max_c"].dropna().between(-60, 60).all()
                                        and df["temp_min_c"].dropna().between(-60, 60).all()),
        "max_gte_min": bool((df["temp_max_c"] >= df["temp_min_c"]).dropna().all()),
        "precip_non_negative": bool((df["precipitation_mm"].dropna() >= 0).all()),
        "null_rate_under_10pct": bool(
            df[["temp_max_c", "temp_min_c", "precipitation_mm"]].isna().mean().max() < 0.10
        ) if len(df) else False,
    }
    if expected_cities is not None:
        results["all_cities_present"] = df["city"].nunique() == expected_cities

    failed = [name for name, ok in results.items() if not ok]
    if failed:
        raise DataQualityError(f"Data quality checks failed: {', '.join(failed)}")
    return results
