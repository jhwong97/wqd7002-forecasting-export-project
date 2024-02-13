from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Dag configurations part
default_args = {
    'owner': 'albert',
    'email': ['albertwong345@gmail.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'start_date': datetime(2024, 2, 7)
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'   
}

# Define the parent DAG
with DAG(
    dag_id = "parent_dag",
    default_args=default_args,
    schedule="@monthly",
    tags=["wqd7002"]
) as dag:
    
    trigger_mets_export_dag = TriggerDagRunOperator(
        task_id="trigger_mets_export",
        trigger_dag_id="mets_export_etl",
        dag=dag,
    )
    
    trigger_mets_import_dag = TriggerDagRunOperator(
        task_id="trigger_mets_import",
        trigger_dag_id="mets_import_etl",
        dag=dag,
    )
    
    trigger_fred_etl_dag = TriggerDagRunOperator(
        task_id="trigger_fred_etl",
        trigger_dag_id="fred_etl",
        dag=dag,
    )
    
    trigger_imf_etl_dag = TriggerDagRunOperator(
        task_id="trigger_imf_etl",
        trigger_dag_id="imf_etl",
        dag=dag,
    )
    
[trigger_mets_export_dag, trigger_mets_import_dag, trigger_imf_etl_dag, trigger_fred_etl_dag]