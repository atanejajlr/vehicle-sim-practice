from unittest import TestCase, mock
from sharedlib.helpers.pubsub_helper import (
    create_pubsub_message,
    send_all_requests,
)


class TestPubsubHelper(TestCase):

    def setUp(self):
        self.input_request = {'metadata': {
            'base_id': '2ac54299589614e199e1b189cd7592ca5abb4c78aae496309ea3e0b79e89bf4d37207af7f3005f4f139716ab996ce0f77124fdc834aeb8b5fe27a1615316eb9e',
            'run_id': 'daily_20210623'},
            'request': {'ace_date': '2021-05-21',
                        'build_phase': 'J1',
                        'event_date': '2020-08-24 00:00:07 UTC',
                        'full_commercial_code': 'BMBH_HO405',
                        'full_d_pack_code': 'RALB_357EE',
                        'full_market_code': 'ZZUD_800PH',
                        'model_year': '2021',
                        'sales_feature_codes': [],
                        'vehicle_line': 'L405'}}
        self.raw_req = {'ace_date': '2021-05-21',
                        'vehicle_line': 'L405',
                        'full_market_code': 'ZZUD_800PH',
                        'full_commercial_code': 'BMBH_HO405',
                        'full_d_pack_code': 'RALB_357EE',
                        'model_year': '2021',
                        'build_phase': 'J1',
                        'event_date': '2020-08-24T00:00:07Z',
                        'sales_feature_codes': ['CCFA_029TB']}
        self.settings = {
            'project': 'jlr-vehiclesim-dev',
            'topic_name': 'test'
        }
        self.maxDiff = None
        
    def test_create_pubsub_messaage(self):
        expected_message = {
            "data": b'{"metadata": {"base_id": "2ac54299589614e199e1b189cd7592ca5abb4c78aae496309ea3e0b79e89bf4d37207af7f3005f4f139716ab996ce0f77124fdc834aeb8b5fe27a1615316eb9e", "run_id": "daily_20210623"}, "request": {"ace_date": "2021-05-21", "build_phase": "J1", "event_date": "2020-08-24 00:00:07 UTC", "full_commercial_code": "BMBH_HO405", "full_d_pack_code": "RALB_357EE", "full_market_code": "ZZUD_800PH", "model_year": "2021", "sales_feature_codes": [], "vehicle_line": "L405"}}'}
        actual_message = create_pubsub_message(self.input_request)
        self.assertEqual(expected_message, actual_message)

    def test_send_all_requests(self):
        with mock.patch(
                "sharedlib.helpers.pubsub_helper.PubSubPublishMessageOperator"
        ) as mock_op:
            send_all_requests(
                [self.raw_req],
                self.settings,
                "rule_refiner"
            )
            mock_op.assert_called_with(
                task_id="rule_refiner_publisher",
                project_id="jlr-vehiclesim-dev",
                topic="test",
                messages=[{'data': b'{"ace_date": "2021-05-21", "vehicle_line": "L405", "full_market_code": "ZZUD_800PH", "full_commercial_code": "BMBH_HO405", "full_d_pack_code": "RALB_357EE", "model_year": "2021", "build_phase": "J1", "event_date": "2020-08-24T00:00:07Z", "sales_feature_codes": ["CCFA_029TB"]}'}]
            )
