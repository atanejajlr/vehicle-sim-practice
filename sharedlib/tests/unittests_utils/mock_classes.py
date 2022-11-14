from datetime import datetime


class FakeDatetime(datetime):

    def now(self):
        return self

class FakePipe:

    def read(self):
        return b'eyJhbGciOiJSUzI1NiIsImtpZCI6ImZlZDgwZmVjNTZkYjk5MjMzZDRiNG\n'


class FakePopen:

    def __init__(self, args, shell, stdout, stderr):
        self.stdout = stdout


class FakeRequests:

    def get(self, url, headers):
        return FakeResponse(url, headers)

    def post(self, url, json, headers, timeout):
        return FakeResponse(url, headers)


class FakeResponse:

    def __init__(self, url, headers):
        self._url = url
        self.content = None
        if ("metadata" in self._url) and (headers == {'Metadata-Flavor': 'Google'}):
            self.content = b"ATOKEN123TOSEND"
        elif self._url == "https://www.cloud_run_address_for.jlrint/":
            self.content = b""

class FakeOS:
    def getenv(environment, default):
        if environment == "APP_ENV":
            value = "development"
        elif environment == "CLOUD":
            value = True
        return value


class FakeStorageClient:
    def __init__(self, project=None):
        self.project = project

    def get_bucket(self, bucket_name):
        return FakeStorageBucket(bucket_name)


class FakeStorageBucket:

    def __init__(self, name):
        self.name = name

    def get_blob(self, blob_name):
        if blob_name == "vdm-central-config.json":
            return FakeBlob(name=blob_name)
        if blob_name == "sql/date_values.sql":
            return FakeBlob(name=blob_name)
        else:
            return None


class FakeBlob:
    def __init__(self, name):
        self.name = name
        self.size = 1

    def download_as_bytes(self):
        if self.name == "vdm-central-config.json":
            string = b'{"development": {"project": "jlr-cat-vehiclesim-dev", "debug_flag": 1, "bigtable": {"instance": "vdm-dev", "tables": {"raw": "staging-data", "refined_data": "refined_data", "solves": "solves-library"}}, "bigquery": {"ace_dataset": "2020_vs", "loading_dataset": "vdm-data-loading", "output_dataset": "", "daily_upload_dataset": "rules_and_features","feature_profit_dataset":"feature_profit_data_ingestion"}, "gcs": {"buckets": {"data_loading": "vdm-data-loading"}}, "urls": {"rule_refiner": "https://hello-world", "data_layer":""}, "pubsub": {"refine_rules": "vdm-rule-refiner", "refine_parts": "trigger-parts-refiner", "bigtable_load": "bigtable-write", "comp_bigtable_load": "compdata-bigtable-write"}, "dataflow": {"template_path": "gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt","temp_location": "gs://dataflow_storage_bq_bt/tmp","worker_zone": "europe-west2-c","job_name": "df-bq-to-bt","autoscaling": "THROUGHPUT_BASED"}}, "production": {"project": "jlr-cat-vehiclesim-dev", "bigtable": {"instance": "vdm-dev", "tables": {"raw": "staging-data", "refined_data": "refined_data", "solves": "solves-library"}}, "bigquery": {"loading_dataset": "vdm-data-loading", "output_dataset": ""}, "gcs": {"buckets": {"data_loading": "vdm-data-loading"}}, "urls": {"refiner": "", "data_layer": ""}, "pubsub": {"refine_rules": "vdm-rule-refiner", "refine_parts": "trigger-parts-refiner", "bigtable_load": "bigtable-write", "comp_bigtable_load": "compdata-bigtable-write"}, "dataflow": {"template_path": "gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt","temp_location": "gs://dataflow_storage_bq_bt/tmp","worker_zone": "europe-west2-c","job_name": "df-bq-to-bt","autoscaling": "THROUGHPUT_BASED"}}, "test": {"project": "jlr-cat-vehiclesim-dev", "bigtable": {"instance": "vdm-dev", "tables": {"raw": "staging-data", "refined_data": "refined_data", "solves": "solves-library"}}, "bigquery": {"loading_dataset": "vdm-data-loading", "output_dataset": ""}, "gcs": {"buckets": {"data_loading": "vdm-data-loading"}}, "urls": {"refiner": "", "data_layer": ""}, "pubsub": {"refine_rules": "vdm-rule-refiner", "refine_parts": "trigger-parts-refiner", "bigtable_load": "bigtable-write", "comp_bigtable_load": "compdata-bigtable-write"}, "dataflow": {"template_path": "gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt","temp_location": "gs://dataflow_storage_bq_bt/tmp","worker_zone": "europe-west2-c","job_name": "df-bq-to-bt","autoscaling": "THROUGHPUT_BASED"}}}'
        elif self.name == "sql/date_values.sql":
            string = b"""SELECT *
FROM (
      SELECT selected_date 
            ,max(ace_partition_date) OVER (PARTITION BY 1) AS ace_date
            ,max(wers_partition_date) OVER (PARTITION BY 1) AS wers_date
            ,max(wips_partition_date) OVER (PARTITION BY 1) AS wips_date
            ,max(cmms3_partition_date) OVER (PARTITION BY 1) AS cmms3_date
      FROM `jlr-vehiclesim-prod.2020_vs.dates_applicability`
      WHERE selected_date <= '{partition_date}'
)
WHERE selected_date = '{partition_date}"""
        return string


class FakeBQClient:
    def __init__(self, project):
        pass

    def query(self, query, job_config=None):
        return FakeQueryJob(query)

    def dataset(self, dataset_id, project=None):
        return FakeDatasetReference(dataset_id, project)


class FakeDatasetReference:
    def __init__(self, dataset_id, project):
        self.dataset = dataset_id

    def table(self, table_id):
        return f"project.dataset_id.table_name"


class FakeQueryJob:
    def __init__(self, query):
        self.query = query

    def result(self):
        return [FakeRow({'ace_date': datetime(2021, 7, 15, 0, 0, 0), 'wers_date': datetime(2021, 7, 18, 0, 0, 0),
                         'wips_date': datetime(2021, 7, 19, 0, 0, 0), 'cmms3_date': datetime(2021, 7, 19, 0, 0, 0)})]


class FakeRow:
    def __init__(self, data):
        self.data = data

    def get(self, col):
        if col == "ace_date":
            data = self.data['ace_date']
        elif col == "wers_date":
            data = self.data['wers_date']
        elif col == "wips_date":
            data = self.data['wips_date']
        elif col == "cmms3_date":
            data = self.data['cmms3_date']
        return data


class FakeRuleRow:

    def __init__(self, values, columns):
        self._values = values
        self._columns = columns

    def get(self, key):
        return self._values[self._columns[key]]


class FakeDatetimeDatetime:

    @staticmethod
    def now():
        return FakeDatetimeObject()


class FakeDatetimeObject:

    def __init__(self):
        self.day = "2021-07-27"

    def strftime(self, settings):
        if settings == "%Y-%m-%d":
            return self.day
