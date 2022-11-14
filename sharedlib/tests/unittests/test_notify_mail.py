import unittest
from unittest.mock import patch
from sharedlib.helpers.notify_email import (
    alert_dag_failure,
    notify_mail
)


class TestNotifyMail(unittest.TestCase):

    def setUp(self):
        self.message = "**Error:** ``` AirflowException('Task failure') ``` **DAG:** Test **Task:** __test_task__ (" \
                       "2022-06-07 08:00:00+00:00) "
        self.log_url = "https://test"
        self.config = {
            "from_email": "test.com",
            "dag": "test_dag"
        }

    @patch("sharedlib.helpers.notify_email.Variable")
    @patch("sharedlib.helpers.notify_email.send_email")
    @patch("sharedlib.helpers.notify_email.datetime")
    def test_alert_dag_failure(
            self,
            dt_mock,
            mail_mock,
            var_mock
    ):
        dt_mock.datetime.now.return_value = "2022-06-08 11:15:31.780040"
        var_mock.get.return_value = "test.com"
        alert_dag_failure(self.message, self.log_url, self.config)
        mail_mock.assert_called_with(
            to="test.com",
            subject=f'DAG: test_dag has failed at: '
                    + '2022-06-08 11:15:31.780040',
            html_content="Composer log_url is: "
                         + f'<a href="https://test">Log URL</a>'
                         + "<br><br>"
                         + self.message,
        )

    @patch("sharedlib.helpers.notify_email.alert_dag_failure")
    @patch("sharedlib.helpers.notify_email.airflow_conf.get")
    def test_notify_mail(self,
                         conf_mock,
                         alert_mock,
                         ):
        conf_mock.return_value = "http://localhost"
        with patch("airflow.models.taskinstance.TaskInstance") as mock_class:
            mock_obj = mock_class.return_value
            mock_obj.dag_id = "Test_Dag"
            mock_obj.task_id = "Test_task"
            mock_obj.execution_date = "2022-06-08 12:00:00"

            context = {
                "ti": mock_obj,
                "exception": "AirflowException(Dataform task Rules_And_Features has been FAILED)",
            }
            notify_mail(context, self.config)

            alert_mock.assert_called_with(
                message="\n**Error:**\n```\n'AirflowException(Dataform task Rules_And_Features has been "
                        "FAILED)'\n```\n**DAG:**   __Test_Dag__\n**Task:**  __Test_task__ (2022-06-08 12:00:00)\n",
                log_url="http://localhost/log?dag_id=Test_Dag&task_id=Test_task&execution_date=2022-06-08 "
                        "12:00:00&format=json",
                config=self.config
            )
