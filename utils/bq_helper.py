#type:ignore
#https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/bigquery/index.html
from airflow.providers.google.cloud.operators.bigquery import(
    
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryIntervalCheckOperator,
    BigQueryCheckOperator
)

from airflow.operators.python import BranchPythonOperator, PythonOperator

def check_partition_task(
    
    task,
    pipeline_settings,
    task_settings
):
    
    """
    This function invokes the BranchPythonOperator. The BranchPythonOperator is much like the PythonOperator 
    except that it expects a python_callable that returns a task_id (or list of task_ids). 
    The task_id returned is followed, and all of the other paths are skipped.
    
    """
    
    task_settings = task_settings[task]
    return BranchPythonOperator(
        
        task_id=f"check_daily_partition_{task}",
        python_callable=check_todays_partition_exists,
        op_kwargs={
            
            "dataset": task_settings['dataset'],
            "table": task,
            "pipeline_settings": pipeline_settings,
            "partition_field": task_settings.get(
                "partition_field",
                "_PARTITION_TIME"
            )
            
        }
    )
    
def create_empty_table(
    
    task,
    pipeline_settings,
    task_settings
):
    
    """
    
    This function invokes the BigQueryCreateEmptyTableOperator 
    which creates a new external table in the dataset with the data 
    from Google Cloud
    The function is called from the TaskGroup within the DAG
    
    """
    
    task_settings = task_settings[task]
    return BigQueryCreateEmptyTableOperator(
        
        task_id=f"create_table_{task}",
        table_id=task,
        dataset_id=task_settings['dataset'],
        time_partitioning={
            "type": "DAY",
            "field": task_settings.get("partition_field", None)  
        },
        project_id=pipeline_settings['project'],
        schema_fields=task_settings['schema']
    )
    
def run_query_job_from_file(
        task,
        pipeline_settings,
        task_settings,
        gcp_conn_id='bigquery_default'
):
    
    """
    
    This function invokes the BigQueryInsertJobOperator which
    executes a BigQuery job. Waits for the job to complete and returns job id.
    The function is called from the TaskGroup within the DAG
    
    """
    task_settings = task_settings[task]
    config = {
        "vsim_project": pipeline_settings["project"],
        "raw_dataset": pipeline_settings["bigquery"]["raw_dataset"],
        "prod_project": pipeline_settings["prod_project"],
        "vsim_dataset": pipeline_settings["bigquery"]["vsim_dataset"],
        "run_date": pipeline_settings["run_date"],
    }
    return BigQueryInsertJobOperator(
        task_id=f"run_query_{task}",
        configuration={
            "query": {
                "query": get_query_text(
                    f"{pipeline_settings['repo_path']}sql/{task}.sql",
                    config
                ),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": pipeline_settings['project'],
                    "datasetId": task_settings['dataset'],
                    "tableId": task,
                },
                "writeDisposition": task_settings['write_disposition']
            },
        },
        project_id=pipeline_settings['project'],
        location='EU',
        gcp_conn_id=gcp_conn_id,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    
def check_table_interval(
        table,
        pipeline_settings,
        task_settings
):
    
    
    """
    This function invokes the BigQueryIntervalCheckOperator
    and checks that the values of metrics given as SQL expressions 
    are within
    
    """
        
    task_settings = task_settings[table]
    return BigQueryIntervalCheckOperator(
    task_id=f"check_interval_{table}",
    table=f"`{pipeline_settings['project']}.{task_settings['dataset']}.{table}`",
    days_back=task_settings.get('days_back', 2),
    metrics_thresholds=task_settings['tests']['interval_thresholds'],
    date_filter_column=task_settings.get('partition_field', '_PARTITIONTIME'),
    use_legacy_sql=False
    
    )
    
    
def run_table_tests(
    table,
    pipeline_settings,
    task_settings
):
    sql = compile_test_query(
        pipeline_settings,
        task_settings,
        table
    )
    return BigQueryCheckOperator(
        task_id=f"run_tests_{table}",
        sql=sql,
        use_legacy_sql=False,
        location="EU"
    )

    