import json
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator


def create_pubsub_message(req):
    message = {"data": json.dumps(req).encode('utf-8')}
    return message


def send_all_requests(requests, settings, task):
    messages = []
    for req in requests:
        messages.append(create_pubsub_message(req))
    publish_task = PubSubPublishMessageOperator(
        task_id=f"{task}_publisher",
        project_id=settings['project'],
        topic=settings['topic_name'],
        messages=messages,
    )
    publish_task.execute(dict())
