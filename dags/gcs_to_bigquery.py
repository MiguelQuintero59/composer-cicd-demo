from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="gcs_to_bigquery_pipeline",
    default_args=default_args,
    description="Load CSV files from GCS to BigQuery and run transformations",
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["gcp", "bigquery", "composer"],
) as dag:

    load_raw_data = GCSToBigQueryOperator(
        task_id="load_raw_data",
        bucket="raw-loan-transactions",
        source_objects=["raw-loan-transactions/*.csv"],
        destination_project_dataset_table="globant-technical-01.data_ingestion",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_APPEND",
    )

    transform_data = BigQueryInsertJobOperator(
        task_id="transform_data",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `globant-technical-01.data_ingestion` AS
                    SELECT
                        *
                    FROM `globant-technical-01.data_ingestion`
                """,
                "useLegacySql": False,
            }
        },
    )

    load_raw_data >> transform_data
