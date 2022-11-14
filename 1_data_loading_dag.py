# type: ignore
import sys
import os


from airflow import DAG
from airflow.utils.edgemodifier import Label
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils import dates
from datetime import timedelta
from functools import partial
from utils.get_settings import filter_execution_date
from config.task_config import task_config, dates_app_config
from utils.bq_helper import (
    run_query_job_from_file,
    create_empty_table,
    check_partition_task,
    check_table_interval,
    run_table_tests
)
from config.conf import settings as pipeline_settings



FROM_EMAIL = 'ataneja@jaguarlandrover.com'
task_settings = task_config
default_args = {
    
    'ownwer': 'ataneja',
    'start_date': dates.days_ago(1),
    'retries': 1, #number of retries that should be performed before failing the task
    'retry_delay': timedelta(minutes=1),
    'depend_on_past': True,
    'wait_for_downstream': True,
    'catch_up': False,
    'email': [FROM_EMAIL],
    'email_on_retry': False,
    'email_on_failure': True, 
    
}

with DAG(
    
    dag_id = "data_loading_dag",
    default_args=default_args,
    schedule_interval="30 06 * * *", #at 06:30 daily
    max_active_runs=1, #max_active_runs defines how many running concurrent instances of a DAG there are allowed to be.
    render_template_as_native_obj=True,
)as dag:
    
    weekdays_only = ShortCircuitOperator(
        dag=dag,
        provide_context=True,
        task_id='weekdays_only',
        python_callable=filter_execution_date  
    )
    
    #Let us create a TaskGroup
    
    with TaskGroup(
            group_id="Raw_Data_Ingestion", default_args={"retries": 1}
    ) as Raw_Data_Ingestion:
        table_tasks = []
        for task in task_config:
            check_task = check_partition_task(
                task,
                pipeline_settings,
                task_settings
            )
            create_table_task = create_empty_table(
                task,
                pipeline_settings,
                task_settings
            )
            run_query_task = run_query_job_from_file(
                task,
                pipeline_settings,
                task_settings,
                gcp_conn_id='edw_conn' #
            )
            check_interval_task = check_table_interval(
                task,
                pipeline_settings,
                task_settings
            )
            run_tests_task = run_table_tests(
                task,
                pipeline_settings,
                task_settings
            )
            final_task = DummyOperator(
                task_id=f'final_task_{task}',
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            )

            table_tasks.append(final_task)

            check_task >> Label(
                "table not found") >> create_table_task >> run_query_task >> check_interval_task >> run_tests_task >> final_task
            check_task >> Label(
                "partition not found") >> run_query_task >> check_interval_task >> run_tests_task >> final_task
            check_task >> Label("table up to date") >> final_task

    dates_app_run_query = run_query_job_from_file(
        "dates_applicability",
        pipeline_settings,
        dates_app_config
    )

    get_settings_task = get_settings(
        "get_settings",
        pipeline_settings,
    )

    weekdays_only >> Raw_Data_Ingestion >> dates_app_run_query
    
    
    
