from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 7),
    'retries': 0,
}

with DAG(
    'Healthcare_ETL_Pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # 1. Extract (Bronze Layer)
    run_extract = BashOperator(
        task_id='extract_bronze',
        bash_command='spark-submit /opt/airflow/scripts/extract.py'
    )

    # 2. Transform Silver
    run_silver = BashOperator(
        task_id='transform_silver',
        bash_command='spark-submit /opt/airflow/scripts/transform_silver.py'
    )


    # 3. Transform Gold (Consumes Silver + Dim Date)
    run_gold = BashOperator(
        task_id='transform_gold',
        bash_command='spark-submit --jars /opt/airflow/drivers/postgresql-42.7.10.jar /opt/airflow/scripts/transform_gold.py'
    )

    run_extract >> run_silver  >> run_gold