from utils.get_settings import get_environment_settings
from airflow.models import Variable
from datetime import datetime

run_date = datetime.today() #Variable.get("run_date")
settings = get_environment_settings(run_date)

