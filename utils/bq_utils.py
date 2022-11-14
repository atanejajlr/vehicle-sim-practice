#type:ignore
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def check_todays_partition_exists(
    
    dataset,
    table,
    pipeline_settings,
    partition_field
):
    """
    This function checks if the table exists - if not it creates. 
    This is a Python callable which is a part of the Task Group in
    the main dag.
    
    """
    client = bigquery.Client(project=pipeline_settings['project'])
    sql = "SELECT COUNT(*) AS count\n" \
        + f"FROM `{pipeline_settings['project']}.{dataset}.{table}`\n" \
        + f"WHERE DATE ({partition_field}) = CURRENT_DATE()\n"
        
    try:
        query = client.query(sql)
        count = [row.count for row in query.result()][0]
        if count > 0:
            return f"Raw_Data_Ingestion.final_task_{table}"
        else:
            return f"Raw_Data_Ingestion.run_query_{table}"
        
    except NotFound:
        return f"Raw_Data_Ingestion.create_table_{table}"
    
    
def compile_test_query(
        pipeline_settings,
        task_settings,
        table
):
    task_settings = task_settings[table]
    table_parameters = {
        "table": table,
        "partition_field": task_settings.get('partition_field', '_PARTITIONTIME'),
        "dataset": task_settings['dataset'],
        "project": pipeline_settings['project']
    }
    tests = task_settings['tests']
    tests.pop("interval_thresholds", None)

    test_query = "SELECT MIN(test_pass) AS tests_pass\nFROM (\n"
    i = 0
    for test_name in tests:
        test_parameters = tests[test_name]
        sql = get_query_text(
            f"{pipeline_settings['repo_path']}sql/tests/{test_parameters['test_type']}.sql",
            conf={**table_parameters, **test_parameters}
        )
        i += 1
        test_query += sql
        if i < len(tests):
            test_query += "\nUNION ALL\n"
        else:
            test_query += ")"

    return test_query
        
            