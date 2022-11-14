"""
Functions for making dataform API calls
"""
import json
import time
import requests
from airflow import AirflowException
from airflow.models import Variable
from google.cloud import secretmanager

def dataform_call(schedule_name):
    """
    Main Function to get dataform api key,formulate the request and get response from API call
    """
    base_url = Variable.get("dataform_base_url")
    api_key = access_secret_version()
    response, headers = dataform_request(api_key, schedule_name)
    print(response)
    run_url = base_url + "/" + response.json()["id"]
    return loop_response_check(run_url, headers, schedule_name)

def access_secret_version(version_id="latest"):
    """
    Function to get data from secret manager for particular secret id
    """
    project_id = Variable.get("key_project_id")
    secret_id = Variable.get("secret_id")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def dataform_request(api_key, schedule_name):
    """
    Function to frame request and post the same using dataform API call
    """
    base_url = Variable.get("dataform_base_url")
    env_name = Variable.get("dataform_env")
    headers = {"Authorization": "Bearer " + api_key}
    run_create_request = {
        "environmentName": env_name,
        "scheduleName": schedule_name,
    }
    response = requests.post(
        base_url, data=json.dumps(run_create_request), headers=headers
    )
    return response, headers


def response_check(run_url, headers, schedule_name):
    time.sleep(10)
    response = requests.get(run_url, headers=headers)
    if response.json()["status"] in ["FAILED", "CANCELLED", "TIMED_OUT"]:
        raise AirflowException(
            f'Dataform task {schedule_name} has been {response.json()["status"]} for reason {response.json()["runLogUrl"]}'
        )
    print(f"Dataform Job Running:", response.json())
    return response


def loop_response_check(run_url, headers, schedule_name):
    """
    Function to get response from dataform API and check the job status
    """
    response = requests.get(run_url, headers=headers)
    while response.json()["status"] == "RUNNING":
        response = response_check(run_url, headers, schedule_name)
    return "Dataform job finished"
