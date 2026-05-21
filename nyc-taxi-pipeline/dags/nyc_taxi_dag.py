from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

default_args = {
    'owner': 'LMe',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nyc_taxi_incremental_pipeline',
    default_args=default_args,
    description='Orchestrates NYC Taxi Medallion pipeline on Databricks monthly',
    schedule_interval='@monthly',
    catchup=False,
    max_active_runs=1
)

incremental_params = {
    "run_date": "{{ ds }}",
    "environment": "production"
}

# For pure serverless workspace - use serverless compute with tasks array
bronze_ingest = DatabricksSubmitRunOperator(
    task_id='bronze_ingest',
    json={
        'run_name': f'bronze_ingest_{{{{ ds }}}}',
        'tasks': [
            {
                'task_key': 'bronze_ingest_task',
                'notebook_task': {
                    'notebook_path': '/Workspace/Users/lilit.levonyan@edu.ysu.am/NYC-Taxi-Data-Engineering-Project/nyc-taxi-pipeline/notebooks/01_bronze_ingest.py',
                    'base_parameters': incremental_params
                },
                'compute': {
                    'compute_key': 'serverless'  # This tells it to use serverless
                }
            }
        ]
        # No job_clusters section for pure serverless
    },
    dag=dag
)

silver_clean = DatabricksSubmitRunOperator(
    task_id='silver_clean',
    json={
        'run_name': f'silver_clean_{{{{ ds }}}}',
        'tasks': [
            {
                'task_key': 'silver_clean_task',
                'notebook_task': {
                    'notebook_path': '/Workspace/Users/lilit.levonyan@edu.ysu.am/NYC-Taxi-Data-Engineering-Project/nyc-taxi-pipeline/notebooks/02_silver_clean.py',
                    'base_parameters': incremental_params
                },
                'compute': {
                    'compute_key': 'serverless'
                }
            }
        ]
    },
    dag=dag
)

gold_ml = DatabricksSubmitRunOperator(
    task_id='gold_ml',
    json={
        'run_name': f'gold_ml_{{{{ ds }}}}',
        'tasks': [
            {
                'task_key': 'gold_ml_task',
                'notebook_task': {
                    'notebook_path': '/Workspace/Users/lilit.levonyan@edu.ysu.am/NYC-Taxi-Data-Engineering-Project/nyc-taxi-pipeline/notebooks/03_gold_ml.py',
                    'base_parameters': incremental_params
                },
                'compute': {
                    'compute_key': 'serverless'
                }
            }
        ]
    },
    dag=dag
)

bronze_ingest >> silver_clean >> gold_ml