from unittest import TestCase, mock
from sharedlib.tests.unittests_utils.mock_classes import FakeStorageClient, FakeBQClient, FakeOS, FakeDatetimeDatetime
from sharedlib.helpers.central_config import CentralConfig


class TestCentralConfig(TestCase):

    def setUp(self):
        mock.patch('sharedlib.helpers.central_config.storage.Client', FakeStorageClient).start()
        mock.patch('sharedlib.helpers.central_config.bigquery.Client', FakeBQClient).start()
        mock.patch('sharedlib.helpers.central_config.os', FakeOS).start()
        mock.patch('sharedlib.helpers.central_config.datetime', FakeDatetimeDatetime).start()

        self.get_expected_json()
        self.cc = CentralConfig(project="jlr-vehiclesim-prod",
                                config_file="vdm-central-config.json",
                                config_bucket='vdm_central_config',
                                date_query='sql/date_values.sql',
                                run_date='Current')

    def tearDown(self):
        mock.patch.stopall()

    def test_get_full_conf_current(self):
        expected_result = {
            "project": "jlr-cat-vehiclesim-dev",
            "debug_flag": 1,
            "run_date": "2021-07-27",
            "ace_date": '2021-07-15',
            "wers_date": '2021-07-18',
            "wips_date": '2021-07-19',
            "cmms3_date": '2021-07-19',
            "bigtable": {
                "instance": "vdm-dev",
                "tables": {
                    "raw": "staging-data",
                    "refined_data": "refined_data",
                    "solves": "solves-library"
                }
            },
            "bigquery": {
                "ace_dataset": "2020_vs",
                "loading_dataset": "vdm-data-loading",
                "output_dataset": "",
                "daily_upload_dataset": "rules_and_features",
                "feature_profit_dataset":"feature_profit_data_ingestion",
            },
            "gcs": {
                "buckets": {
                    "data_loading": "vdm-data-loading"
                }
            },
            "urls": {
                "rule_refiner": "https://hello-world",
                "data_layer": ""
            },
            "pubsub": {
                "refine_rules": "vdm-rule-refiner",
                "refine_parts": "trigger-parts-refiner",
                "bigtable_load": "bigtable-write",
                "comp_bigtable_load": "compdata-bigtable-write"
            },
            "dataflow": {
                "template_path": "gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt",
                "temp_location": "gs://dataflow_storage_bq_bt/tmp",
                "worker_zone": "europe-west2-c",
                "job_name": "df-bq-to-bt",
                "autoscaling": "THROUGHPUT_BASED"}
        }
        self.assertEqual(expected_result, self.cc.get_full_conf())

    def test_get_full_conf_specific(self):
        self.cc.run_date = '2021-09-25'
        expected_result = {'project': 'jlr-cat-vehiclesim-dev', 'debug_flag': 1, 'bigtable': {'instance': 'vdm-dev', 'tables': {'raw': 'staging-data', 'refined_data': 'refined_data', 'solves': 'solves-library'}}, 'bigquery': {'ace_dataset': '2020_vs', 'loading_dataset': 'vdm-data-loading', 'output_dataset': '', "daily_upload_dataset": "rules_and_features","feature_profit_dataset":"feature_profit_data_ingestion"}, 'gcs': {'buckets': {'data_loading': 'vdm-data-loading'}}, 'urls': {'rule_refiner': 'https://hello-world', 'data_layer': ''}, 'pubsub': {'refine_rules': 'vdm-rule-refiner', 'refine_parts': 'trigger-parts-refiner', 'bigtable_load': 'bigtable-write', 'comp_bigtable_load': 'compdata-bigtable-write'}, 'dataflow': {'template_path': 'gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt', 'temp_location': 'gs://dataflow_storage_bq_bt/tmp', 'worker_zone': 'europe-west2-c', 'job_name': 'df-bq-to-bt', 'autoscaling': 'THROUGHPUT_BASED'}, 'run_date': '2021-09-25', 'ace_date': '2021-07-15', 'wers_date': '2021-07-18', 'wips_date': '2021-07-19', 'cmms3_date': '2021-07-19'}
        self.assertEqual(expected_result, self.cc.get_full_conf())

    def test_get_env_specific_config(self):
        env_config = {'project': 'jlr-cat-vehiclesim-dev', 'debug_flag': 1, 'run_date': '2021-07-27', 'bigtable': {'instance': 'vdm-dev', 'tables': {'raw': 'staging-data', 'refined_data': 'refined_data', 'solves': 'solves-library'}}, 'bigquery': {'ace_dataset': '2020_vs', 'loading_dataset': 'vdm-data-loading', 'output_dataset': '', "daily_upload_dataset": "rules_and_features","feature_profit_dataset":"feature_profit_data_ingestion"}, 'gcs': {'buckets': {'data_loading': 'vdm-data-loading'}}, 'urls': {'rule_refiner': 'https://hello-world', 'data_layer': ''}, 'pubsub': {'refine_rules': 'vdm-rule-refiner', 'refine_parts': 'trigger-parts-refiner', 'bigtable_load': 'bigtable-write', 'comp_bigtable_load': 'compdata-bigtable-write'}, 'dataflow': {'template_path': 'gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt', 'temp_location': 'gs://dataflow_storage_bq_bt/tmp', 'worker_zone': 'europe-west2-c', 'job_name': 'df-bq-to-bt', 'autoscaling': 'THROUGHPUT_BASED'}}
        self.assertEqual(env_config, self.cc._get_env_specific_config())

    def test_parse_central_file(self):
        expected_result = {'development': {'project': 'jlr-cat-vehiclesim-dev', 'debug_flag': 1, 'bigtable': {'instance': 'vdm-dev', 'tables': {'raw': 'staging-data', 'refined_data': 'refined_data', 'solves': 'solves-library'}}, 'bigquery': {'ace_dataset': '2020_vs', 'loading_dataset': 'vdm-data-loading', 'output_dataset': '', "daily_upload_dataset": "rules_and_features","feature_profit_dataset":"feature_profit_data_ingestion"}, 'gcs': {'buckets': {'data_loading': 'vdm-data-loading'}}, 'urls': {'rule_refiner': 'https://hello-world', 'data_layer': ''}, 'pubsub': {'refine_rules': 'vdm-rule-refiner', 'refine_parts': 'trigger-parts-refiner', 'bigtable_load': 'bigtable-write', 'comp_bigtable_load': 'compdata-bigtable-write'}, 'dataflow': {'template_path': 'gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt', 'temp_location': 'gs://dataflow_storage_bq_bt/tmp', 'worker_zone': 'europe-west2-c', 'job_name': 'df-bq-to-bt', 'autoscaling': 'THROUGHPUT_BASED'}}, 'production': {'project': 'jlr-cat-vehiclesim-dev', 'bigtable': {'instance': 'vdm-dev', 'tables': {'raw': 'staging-data', 'refined_data': 'refined_data', 'solves': 'solves-library'}}, 'bigquery': {'loading_dataset': 'vdm-data-loading', 'output_dataset': ''}, 'gcs': {'buckets': {'data_loading': 'vdm-data-loading'}}, 'urls': {'refiner': '', 'data_layer': ''}, 'pubsub': {'refine_rules': 'vdm-rule-refiner', 'refine_parts': 'trigger-parts-refiner', 'bigtable_load': 'bigtable-write', 'comp_bigtable_load': 'compdata-bigtable-write'}, 'dataflow': {'template_path': 'gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt', 'temp_location': 'gs://dataflow_storage_bq_bt/tmp', 'worker_zone': 'europe-west2-c', 'job_name': 'df-bq-to-bt', 'autoscaling': 'THROUGHPUT_BASED'}}, 'test': {'project': 'jlr-cat-vehiclesim-dev', 'bigtable': {'instance': 'vdm-dev', 'tables': {'raw': 'staging-data', 'refined_data': 'refined_data', 'solves': 'solves-library'}}, 'bigquery': {'loading_dataset': 'vdm-data-loading', 'output_dataset': ''}, 'gcs': {'buckets': {'data_loading': 'vdm-data-loading'}}, 'urls': {'refiner': '', 'data_layer': ''}, 'pubsub': {'refine_rules': 'vdm-rule-refiner', 'refine_parts': 'trigger-parts-refiner', 'bigtable_load': 'bigtable-write', 'comp_bigtable_load': 'compdata-bigtable-write'}, 'dataflow': {'template_path': 'gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt', 'temp_location': 'gs://dataflow_storage_bq_bt/tmp', 'worker_zone': 'europe-west2-c', 'job_name': 'df-bq-to-bt', 'autoscaling': 'THROUGHPUT_BASED'}}}
        self.assertEqual(expected_result, self.cc._parse_central_file("vdm-central-config.json"))

    def test_download_central_file(self):
        self.assertEqual(self.expected_json, self.cc._download_central_file("vdm-central-config.json"))

    def get_expected_json(self):
        self.expected_json = b'{"development": {"project": "jlr-cat-vehiclesim-dev", "debug_flag": 1, "bigtable": {"instance": "vdm-dev", "tables": {"raw": "staging-data", "refined_data": "refined_data", "solves": "solves-library"}}, "bigquery": {"ace_dataset": "2020_vs", "loading_dataset": "vdm-data-loading", "output_dataset": "", "daily_upload_dataset": "rules_and_features","feature_profit_dataset":"feature_profit_data_ingestion"}, "gcs": {"buckets": {"data_loading": "vdm-data-loading"}}, "urls": {"rule_refiner": "https://hello-world", "data_layer":""}, "pubsub": {"refine_rules": "vdm-rule-refiner", "refine_parts": "trigger-parts-refiner", "bigtable_load": "bigtable-write", "comp_bigtable_load": "compdata-bigtable-write"}, "dataflow": {"template_path": "gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt","temp_location": "gs://dataflow_storage_bq_bt/tmp","worker_zone": "europe-west2-c","job_name": "df-bq-to-bt","autoscaling": "THROUGHPUT_BASED"}}, "production": {"project": "jlr-cat-vehiclesim-dev", "bigtable": {"instance": "vdm-dev", "tables": {"raw": "staging-data", "refined_data": "refined_data", "solves": "solves-library"}}, "bigquery": {"loading_dataset": "vdm-data-loading", "output_dataset": ""}, "gcs": {"buckets": {"data_loading": "vdm-data-loading"}}, "urls": {"refiner": "", "data_layer": ""}, "pubsub": {"refine_rules": "vdm-rule-refiner", "refine_parts": "trigger-parts-refiner", "bigtable_load": "bigtable-write", "comp_bigtable_load": "compdata-bigtable-write"}, "dataflow": {"template_path": "gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt","temp_location": "gs://dataflow_storage_bq_bt/tmp","worker_zone": "europe-west2-c","job_name": "df-bq-to-bt","autoscaling": "THROUGHPUT_BASED"}}, "test": {"project": "jlr-cat-vehiclesim-dev", "bigtable": {"instance": "vdm-dev", "tables": {"raw": "staging-data", "refined_data": "refined_data", "solves": "solves-library"}}, "bigquery": {"loading_dataset": "vdm-data-loading", "output_dataset": ""}, "gcs": {"buckets": {"data_loading": "vdm-data-loading"}}, "urls": {"refiner": "", "data_layer": ""}, "pubsub": {"refine_rules": "vdm-rule-refiner", "refine_parts": "trigger-parts-refiner", "bigtable_load": "bigtable-write", "comp_bigtable_load": "compdata-bigtable-write"}, "dataflow": {"template_path": "gs://dataflow_storage_bq_bt/dataflow_daily_upload/df-bq-to-bt","temp_location": "gs://dataflow_storage_bq_bt/tmp","worker_zone": "europe-west2-c","job_name": "df-bq-to-bt","autoscaling": "THROUGHPUT_BASED"}}}'
