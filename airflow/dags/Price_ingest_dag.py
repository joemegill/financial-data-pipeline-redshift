from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.get_prices import get_prices
from etl.load_to_redshift import load_to_redshift

default_args = {
    'owner': 'joe_megill',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def pipeline() -> None:
    s3_key = get_prices()
    # assumes table raw_etrade_positions exists in Redshift
    load_to_redshift(s3_key, "raw_etrade_positions")

with DAG(
    dag_id='prices_ingest_dag_redshift',
    default_args=default_args,
    description='Pull get_prices, upload to S3, and load into Redshift',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    run_pipeline = PythonOperator(
        task_id='run_pipeline',
        python_callable=pipeline,
    )

    run_pipeline
