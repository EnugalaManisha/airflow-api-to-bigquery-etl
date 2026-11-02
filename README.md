# Automated ETL Pipeline: Open-Meteo API to BigQuery with Apache Airflow

A daily batch ETL pipeline orchestrated with **Apache Airflow**. It extracts weather data from a public REST API, transforms it with **Python and Pandas**, runs **data quality checks**, and loads it into **BigQuery** using an idempotent `MERGE`.

## Pipeline

```
Open-Meteo API  ->  extract (per city)  ->  transform (Pandas)  ->  quality_checks  ->  load (BigQuery MERGE)
```

| Airflow task | What it does |
|--------------|--------------|
| `extract_<city>` | Calls the Open-Meteo API for each city in `etl/config.py` (retries with backoff on network and 5xx errors) |
| `transform` | Flattens the JSON, enforces types, derives `temp_avg_c`, removes duplicate days, stacks all cities |
| `quality_checks` | Fails the run if any check fails, so bad data never reaches BigQuery |
| `load` | Loads to a staging table, then `MERGE`s into the partitioned and clustered target table |

The DAG runs daily at 02:00, re-pulls the last 7 days each run, and is safe to re-run because the `MERGE` on `(city, obs_date)` updates existing rows instead of duplicating them. Tasks retry twice (5 minutes apart), and a failure callback logs the failed task (extend `notify_failure` to send email or Slack alerts).

## Data quality checks (`etl/quality.py`)

- Frame is not empty
- No null keys (`city`, `obs_date`)
- No duplicate `(city, obs_date)` rows
- Temperatures within a plausible range (-60 to 60 C)
- `temp_max_c >= temp_min_c`
- Precipitation is not negative
- Null rate under 10% for each measure
- All configured cities are present

## BigQuery table

`weather_dw.daily_weather`, partitioned by `obs_date` and clustered by `city`:
`city, obs_date, temp_max_c, temp_min_c, temp_avg_c, precipitation_mm, ingested_at`

## Project structure

```
dags/weather_etl_dag.py    Airflow DAG
etl/config.py              cities, API settings, BigQuery names
etl/extract.py             API client with retries
etl/transform.py           Pandas transformation
etl/quality.py             data quality checks
etl/load.py                BigQuery staging load and MERGE
tests/                     pytest unit tests and a sample API response
docker-compose.yml         local Airflow for development
```

## Run the tests (no GCP or Airflow needed)

```bash
pip install requests pandas pyarrow pytest
python -m pytest -q
```

## Run the pipeline

1. Create a GCP project, enable BigQuery, and create a service account with the BigQuery Data Editor and Job User roles. Download its key and keep it out of Git.
2. Set your project and credentials:
   ```bash
   export GCP_PROJECT=your-project-id
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
   ```
3. Start Airflow locally with Docker (uncomment the credential mount lines in `docker-compose.yml` first):
   ```bash
   docker compose up
   ```
4. Open http://localhost:8080, log in with the credentials printed in the logs, enable the `weather_api_to_bigquery` DAG, and trigger a run.
5. Query the result in BigQuery:
   ```sql
   SELECT city, obs_date, temp_max_c, precipitation_mm
   FROM `your-project-id.weather_dw.daily_weather`
   ORDER BY obs_date DESC, city;
   ```

## Tech stack

Python, Pandas, Apache Airflow, BigQuery, Docker, REST APIs, pytest

## Testing status

- The extract (fixture-based), transform, and data quality logic are covered by 12 passing unit tests.
- The DAG file, the live API call, the BigQuery load and MERGE, and `docker-compose.yml` were written to standard Airflow 2.x/3 and BigQuery patterns but were **not run end to end** by the author of this template. Run them in your own environment, fix any environment-specific issues, and add a screenshot of a successful DAG run and the BigQuery result to this README.
