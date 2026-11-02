"""Load: write the DataFrame to a BigQuery staging table, then MERGE into the target table.

Using MERGE on (city, obs_date) makes the load idempotent: re-running a day updates existing
rows instead of creating duplicates.
"""
import pandas as pd

from etl.config import BQ_DATASET, BQ_PROJECT, BQ_STAGING_TABLE, BQ_TABLE


def load_to_bigquery(df: pd.DataFrame) -> int:
    from google.cloud import bigquery  # imported here so tests do not need the package

    client = bigquery.Client(project=BQ_PROJECT)
    dataset = f"{BQ_PROJECT}.{BQ_DATASET}"
    target = f"{dataset}.{BQ_TABLE}"
    staging = f"{dataset}.{BQ_STAGING_TABLE}"

    client.create_dataset(bigquery.Dataset(dataset), exists_ok=True)

    schema = [
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("obs_date", "DATE"),
        bigquery.SchemaField("temp_max_c", "FLOAT"),
        bigquery.SchemaField("temp_min_c", "FLOAT"),
        bigquery.SchemaField("temp_avg_c", "FLOAT"),
        bigquery.SchemaField("precipitation_mm", "FLOAT"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, staging, job_config=job_config).result()

    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{target}`
        PARTITION BY obs_date
        CLUSTER BY city
        AS SELECT * FROM `{staging}` WHERE FALSE
        """
    ).result()

    client.query(
        f"""
        MERGE `{target}` T
        USING `{staging}` S
        ON T.city = S.city AND T.obs_date = S.obs_date
        WHEN MATCHED THEN UPDATE SET
          temp_max_c = S.temp_max_c,
          temp_min_c = S.temp_min_c,
          temp_avg_c = S.temp_avg_c,
          precipitation_mm = S.precipitation_mm,
          ingested_at = S.ingested_at
        WHEN NOT MATCHED THEN INSERT ROW
        """
    ).result()
    return len(df)
