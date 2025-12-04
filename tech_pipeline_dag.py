from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import your ETL functions
from src.scraper import scrape_task
from src.cleaneer import run_cleaning
from src.loader import load_data

# Default task settings
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="tech_stocks_pipeline",
    description="Daily ETL pipeline for Investing.com Technology Stocks",
    default_args=default_args,
    schedule_interval="@daily",      # once every 24 hours
    start_date=datetime(2025, 1, 1),
    catchup=False,                   # do not run old dates
    tags=["etl", "stocks", "project"]
) as dag:

    # 1 — SCRAPE
    scrape_data = PythonOperator(
        task_id="scrape_data",
        python_callable=scrape_task
    )

    # 2 — CLEAN
    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=run_cleaning
    )

    # 3 — LOAD
    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    # Pipeline order: scrape → clean → load
    scrape_data >> clean_data >> load_data_task
