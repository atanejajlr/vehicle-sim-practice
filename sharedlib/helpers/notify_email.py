"""It contains the method that gets invoked on Dag Task Failure"""
import traceback
import datetime
from airflow.configuration import conf as airflow_conf
from airflow.models import Variable
from airflow.providers.sendgrid.utils.emailer import send_email


def alert_dag_failure(message, log_url, config):
    email_circulation_list = Variable.get("email_circulation")
    """This function calls SendGrid method."""
    print("---> notify dag failure -------------------")
    try:
        send_email(
            to=email_circulation_list,
            subject=f'DAG: {config.get("dag")} has failed at: '
                    + str(datetime.datetime.now()),
            html_content="Composer log_url is: "
                         + f'<a href="{log_url}">Log URL</a>'
                         + "<br><br>"
                         + message,
        )
    except Exception as catch_sendgrid_exc:
        print(catch_sendgrid_exc)
        print(traceback.format_exc())


def notify_mail(context, config):
    """This function parses context info and extracts valuable params."""
    base_url = airflow_conf.get("webserver", "base_url")
    t_instance = context["ti"]
    error = context["exception"]
    ti_log_url = f"{base_url}/log?dag_id={t_instance.dag_id}&task_id={t_instance.task_id}&execution_date={t_instance.execution_date}&format=json"
    message = f"""
**Error:**
```
{repr(error)}
```
**DAG:**   __{t_instance.dag_id}__
**Task:**  __{t_instance.task_id}__ ({t_instance.execution_date})
"""

    alert_dag_failure(message=message, log_url=ti_log_url, config=config)
