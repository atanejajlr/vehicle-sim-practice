from unittest import TestCase, mock
from sharedlib.helpers.dataform_helper import *
from airflow import AirflowException
import responses


class TestDataformHelpers(TestCase):
    def setUp(self):
        self.api_key = "PKAS*&^%$£"
        self.env_name = "development"
        self.schedule_name = "Parts"
        self.headers = {"Authorization": "Bearer " + "PKAS*&^%$£"}

    @mock.patch("sharedlib.helpers.dataform_helper.secretmanager.SecretManagerServiceClient")
    @mock.patch("sharedlib.helpers.dataform_helper.Variable.get")
    def test_access_secret_version(self,
                                   var_mock,
                                   sm_mock):
        var_mock.return_value = 'foo'
        sm_mock.return_value.access_secret_version.return_value.payload.data = b'{"secret_value": "dataform"}'
        actual_result = access_secret_version()
        expected_result = '{"secret_value": "dataform"}'
        self.assertEqual(expected_result, actual_result)

    @mock.patch("sharedlib.helpers.dataform_helper.Variable.get")
    @mock.patch("sharedlib.helpers.dataform_helper.requests.post")
    def test_dataform_request(self,
                              post_mock,
                              var_mock):
        var_mock.return_value = 'foo'
        post_mock.return_value = '{"id":"837832", "status":"Running"}'
        actual_response, actual_header = dataform_request(self.api_key,
                                                          self.schedule_name)
        expected_header = {"Authorization": "Bearer " + "PKAS*&^%$£"}
        expected_response = '{"id":"837832", "status":"Running"}'

        self.assertEqual(actual_header, expected_header)
        self.assertEqual(actual_response, expected_response)
        post_mock.assert_called_with("foo",
                                     data=json.dumps({
                                         "environmentName": "foo",
                                         "scheduleName": self.schedule_name,
                                     }),
                                     headers=expected_header)

    @responses.activate
    def test_response_check_success(self,
                                    ):
        responses.get(
            url="https://helloworld/8095292770821161",
            json={'id': '8095292770821161', 'status': 'SUCCESS',
                  'runLogUrl': 'https://helloworld/8095292770821161'}
        )
        actual_result = loop_response_check("https://helloworld/8095292770821161",
                                       self.headers,
                                       self.schedule_name,
                                       )
        expected_result = "Dataform job finished"
        self.assertEqual(actual_result, expected_result)

    @responses.activate
    def test_response_check_failed(self,
                                   ):
        responses.get(
            url="https://helloworld/8095292770821161",
            json={'id': '8095292770821161', 'status': 'FAILED',
                  'runLogUrl': 'https://helloworld/8095292770821161'}
        )
        with self.assertRaises(AirflowException):
            response_check("https://helloworld/8095292770821161",
                                self.headers,
                                self.schedule_name,
                                )
