# type: ignore
from datetime import datetime
from croniter import croniter
from sharedlib.helpers.central_config import CentralConfig
def filter_execution_date(**kwargs):
    dag_execution_date = kwargs['execution_date']
    dag_cron = croniter('30 6 * * *', dag_execution_date)
    original_execution_date = dag_cron.get_next(datetime)
    weekdays_cron = croniter('30 6 * * 1-5', original_execution_date)
    print(f'Execution date:{dag_execution_date}')
    print(f'Original Execution date:{original_execution_date}')
    weekdays_cron.get_next(datetime)
    return original_execution_date == weekdays_cron.get_prev(datetime)

def get_environment_settings(run_date):
    cc = CentralConfig(project='jlr-dl-cat-training',
                       config_bucket='ataneja-vsim-understanding',
                       config_file='vdm-central-config.json',
                       date_query='sql/date_values.sql',
                       run_date=run_date)
    return cc._get_env_specific_config()
    