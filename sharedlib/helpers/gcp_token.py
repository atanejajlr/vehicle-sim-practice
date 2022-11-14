import os
import requests
from subprocess import PIPE, Popen


def get_token(receiving_service_url):

    APP_ENV = os.getenv('APP_ENV')

    if APP_ENV in ['development', 'test', 'production']:
        return get_service_to_service_token(receiving_service_url)
    return get_local_token()


def get_service_to_service_token(receiving_service_url):
    # Set up metadata server request
    # See https://cloud.google.com/compute/docs/instances/verifying-instance-identity#request_signature
    metadata_server_token_url = 'http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience='

    token_request_url = metadata_server_token_url + receiving_service_url
    token_request_headers = {'Metadata-Flavor': 'Google'}

    # Fetch the token
    token_response = requests.get(token_request_url, headers=token_request_headers)
    return token_response.content.decode("utf-8")


def get_local_token():
    pipe = Popen(args=['gcloud', 'auth', 'print-identity-token'],
                 shell=True,
                 stdout=PIPE,
                 stderr=PIPE)
    return pipe.stdout.read().decode("utf-8").replace("\n", "").replace('\r', '')