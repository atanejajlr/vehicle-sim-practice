from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
import json
from datetime import datetime


class CentralConfig:
    def __init__(self, project, config_bucket, config_file, date_query, run_date):
        self.project = project
        self.config_bucket = config_bucket
        self.config_file = config_file
        self.date_query = date_query
        self.run_date = run_date
        self._bucket = storage.Client(self.project).get_bucket(self.config_bucket)

    def get_full_conf(self):
        env_config = self._get_env_specific_config()
        if self.run_date == 'Current':
            env_config['run_date'] = datetime.now().strftime("%Y-%m-%d")
        else:
            env_config['run_date'] = self.run_date
        row = self._get_date_values(env_config['run_date'])
        env_config['ace_date'] = row.get("ace_date").strftime("%Y-%m-%d")
        env_config['wers_date'] = row.get("wers_date").strftime("%Y-%m-%d")
        env_config['wips_date'] = row.get("wips_date").strftime("%Y-%m-%d")
        env_config['cmms3_date'] = row.get("cmms3_date").strftime("%Y-%m-%d")
        return env_config

    def _get_date_values(self, run_date):
        try:
            result = self._run_query(run_date)
            rows = [row for row in result]
            return rows[0]  # Result only has 1
        except IndexError:
            print("Index error in initial dag loading or don't have ace/wips/wers data for current date")

    def _run_query(self, run_date):
        try:
            raw_conf = self._get_env_specific_config()
            client = bigquery.Client(raw_conf['project'])
            query_bytes = self._download_central_file(self.date_query)
            query_text = query_bytes.decode("utf-8").format(partition_date=run_date,
                                                            project=raw_conf['project'],
                                                            loading_dataset=raw_conf['bigquery']['loading_dataset'])

            query_job = client.query(query=query_text)
            return query_job.result()
        except NotFound as Error:
            print(f"Table Not found{Error}")

    def _get_env_specific_config(self):
        env = os.getenv("APP_ENV", "development") or "development"
        env_config = self._parse_central_file(self.config_file)[env]
        if self.run_date == 'Current':
            env_config['run_date'] = datetime.now().strftime("%Y-%m-%d")
        else:
            env_config['run_date'] = self.run_date
        return env_config

    def _parse_central_file(self, file):
        parsed_file = json.loads(self._download_central_file(file))
        return parsed_file

    def _download_central_file(self, file):
        blob = self._bucket.get_blob(file)
        file = blob.download_as_bytes()
        return file
