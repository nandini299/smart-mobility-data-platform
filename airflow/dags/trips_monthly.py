import os
from datetime import UTC, datetime, timedelta

import boto3
import requests
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

from airflow import DAG

BUCKET = os.environ["S3_BUCKET"]
REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym}.parquet"
DBT_ENV = {"DBT": "/home/airflow/dbt_venv/bin/dbt", "DBT_DIR": "/opt/airflow/dbt"}


def notify_failure(context):
    ti = context["task_instance"]
    # Replace with a Slack webhook / email later
    print(f"ALERT: {ti.dag_id}.{ti.task_id} failed for {context['ds']}")


def tlc_to_bronze(logical_date, **_):
    """Stream the month's file from TLC straight into S3 (idempotent: fixed key)."""
    ym = logical_date.strftime("%Y-%m")
    key = (f"bronze/trips/year={logical_date:%Y}/month={logical_date:%m}/"
           f"yellow_tripdata_{ym}.parquet")
    with requests.get(TLC_URL.format(ym=ym), stream=True, timeout=300) as resp:
        resp.raise_for_status()
        boto3.client("s3", region_name=REGION).upload_fileobj(resp.raw, BUCKET, key)
    print(f"uploaded s3://{BUCKET}/{key}")


default_args = {
    "owner": "nandini",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="trips_monthly",
    start_date=datetime(2023, 1, 1, tzinfo=UTC),
    end_date=datetime(2023, 12, 1, tzinfo=UTC),
    schedule="@monthly",
    catchup=True,           # backfills all 12 months of 2023
    max_active_runs=1,      # dbt runs must not overlap
    default_args=default_args,
    tags=["batch", "trips"],
) as dag:

    ingest = PythonOperator(task_id="tlc_to_bronze", python_callable=tlc_to_bronze)

 
    to_silver = BashOperator(
		task_id="trips_bronze_to_silver",

		bash_command="""
		set -e

		echo "Starting local Glue Docker job..."
		echo "Year: {{ logical_date.strftime('%Y') }}"
		echo "Month: {{ logical_date.strftime('%m') }}"

		docker run --rm \
		  -v "$HOST_PROJECT_ROOT:/home/hadoop/workspace" \
		  -e AWS_ACCESS_KEY_ID \
		  -e AWS_SECRET_ACCESS_KEY \
		  -e AWS_DEFAULT_REGION \
		  public.ecr.aws/glue/aws-glue-libs:glue_libs_4.0.0_image_01 \
		  spark-submit \
		  /home/hadoop/workspace/glue/scripts/trips_bronze_to_silver.py \
		  --bucket "$S3_BUCKET" \
		  --year "{{ logical_date.strftime('%Y') }}" \
		  --month "{{ logical_date.strftime('%m') }}"

		echo "Local Glue job completed successfully."
		""",

		env={
		    "HOST_PROJECT_ROOT": "/home/vaaan/projects/smart-mobility-data-platform",
		    "S3_BUCKET": BUCKET,
		    "AWS_DEFAULT_REGION": REGION,
		},

		append_env=True,
    )

    repair = AthenaOperator(
        task_id="register_silver_partitions",
        query="MSCK REPAIR TABLE mobility_silver.trips",
        database="mobility_silver",
        output_location=f"s3://{BUCKET}/athena-results/",
        region_name=REGION,
    )

 
    
    
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="""
        set -e

        RUN_MONTH="{{ logical_date.strftime('%Y-%m') }}"

        echo "===================================="
        echo "Running dbt staging for: $RUN_MONTH"
        echo "===================================="

        cd /opt/airflow/dbt

        /home/airflow/dbt_venv/bin/dbt run \
          --profiles-dir . \
          --select stg_trips \
          --vars "{run_month: '$RUN_MONTH'}"
        """,
        append_env=True,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="""
        set -e

        echo "===================================="
        echo "Loading dbt seeds"
        echo "===================================="

        cd /opt/airflow/dbt

        /home/airflow/dbt_venv/bin/dbt seed \
          --profiles-dir .
        """,
        append_env=True,
    )
    
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="""
        set -e

        echo "===================================="
        echo "Running dbt snapshot"
        echo "===================================="

        cd /opt/airflow/dbt

        /home/airflow/dbt_venv/bin/dbt snapshot \
          --profiles-dir .
        """,
        append_env=True,
    )


    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command="""
        set -e

        RUN_MONTH="{{ logical_date.strftime('%Y-%m') }}"

        echo "===================================="
        echo "Running dbt Gold models for: $RUN_MONTH"
        echo "===================================="

        cd /opt/airflow/dbt

        /home/airflow/dbt_venv/bin/dbt run \
          --profiles-dir . \
          --select fact_trips \
          --vars "{run_month: '$RUN_MONTH'}"
        """,
        append_env=True,
    )


    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        set -e

        cd /opt/airflow/dbt

        /home/airflow/dbt_venv/bin/dbt test \
          --profiles-dir . \
          --select fact_trips
        """,
        append_env=True,
    )


    ingest >> to_silver >> repair >> dbt_seed >> dbt_staging >> dbt_snapshot >> dbt_gold >> dbt_test
    
    
    
    
    
 
