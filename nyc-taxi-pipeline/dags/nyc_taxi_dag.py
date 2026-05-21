# dags/nyc_taxi_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': True,      
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nyc_taxi_incremental_pipeline',
    default_args=default_args,
    description='Orchestrates NYC Taxi Medallion pipeline on Databricks monthly',
    schedule_interval='@monthly', 
    start_date=datetime(2026, 1, 1),
    catchup=False,                
    max_active_runs=1,
) as dag:

    cluster_spec = {
        'spark_version': '14.3.x-scala2.12',
        'node_type_id': 'Standard_D4s_v5',  
        'num_workers': 2
    }

    incremental_params = {
        "load_type": "incremental",
        "year": "{{ data_interval_start.strftime('%Y') }}",
        "month": "{{ data_interval_start.strftime('%m') }}"
    }

    bronze_task = DatabricksSubmitRunOperator(
        task_id='bronze_ingest',
        databricks_conn_id='databricks_default',
        new_cluster=cluster_spec,
        notebook_task={
            'notebook_path': '/Repos/production/nyc-taxi-pipeline/notebooks/01_bronze_ingest',
            'base_parameters': incremental_params
        }
    )

    silver_task = DatabricksSubmitRunOperator(
        task_id='silver_clean',
        databricks_conn_id='databricks_default',
        new_cluster=cluster_spec,
        notebook_task={
            'notebook_path': '/Repos/production/nyc-taxi-pipeline/notebooks/02_silver_clean',
            'base_parameters': incremental_params
        }
    )

    gold_ml_task = DatabricksSubmitRunOperator(
        task_id='gold_ml',
        databricks_conn_id='databricks_default',
        new_cluster=cluster_spec,
        notebook_task={
            'notebook_path': '/Repos/production/nyc-taxi-pipeline/notebooks/03_gold_ml',
            'base_parameters': incremental_params
        }
    )

    bronze_task >> silver_task >> gold_ml_task