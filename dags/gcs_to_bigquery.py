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
        bucket="your-gcs-bucket-name",
        source_objects=["raw/customers/*.csv"],
        destination_project_dataset_table="your_project.raw_dataset.customers_raw",
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
                    CREATE OR REPLACE TABLE `your_project.analytics.customers_clean` AS
                    SELECT
                        customer_id,
                        customer_name,
                        email,
                        CURRENT_TIMESTAMP() AS processed_at
                    FROM `your_project.raw_dataset.customers_raw`
                    WHERE customer_id IS NOT NULL
                """,
                "useLegacySql": False,
            }
        },
    )

    load_raw_data >> transform_data
