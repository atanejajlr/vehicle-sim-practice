import os
from unittest import TestCase, mock
from sharedlib.tests.unittests_utils.mock_classes import FakeRequests, FakePipe, FakePopen
from sharedlib.helpers.gcp_token import (
    get_token,
    get_local_token,
    get_service_to_service_token
)


class TestGCPToken(TestCase):

    def setUp(self):
        os.environ['APP_ENV'] = ""
        mock.patch("sharedlib.helpers.gcp_token.requests", FakeRequests()).start()
        mock.patch("sharedlib.helpers.gcp_token.PIPE", FakePipe()).start()
        mock.patch("sharedlib.helpers.gcp_token.Popen", FakePopen).start()

    def test_get_service_to_service_token(self):
        self.assertEqual("ATOKEN123TOSEND", get_service_to_service_token("https://www.cloud_run_address_for.jlrint/"))

    def test_get_token_in_cloud(self):
        os.environ['APP_ENV'] = "development"
        self.assertEqual("ATOKEN123TOSEND", get_token("https://www.cloud_run_address_for.jlrint/"))

    def test_get_local_token(self):
        self.assertEqual("eyJhbGciOiJSUzI1NiIsImtpZCI6ImZlZDgwZmVjNTZkYjk5MjMzZDRiNG", get_local_token())

    def test_get_token_when_local(self):
        self.assertEqual("eyJhbGciOiJSUzI1NiIsImtpZCI6ImZlZDgwZmVjNTZkYjk5MjMzZDRiNG",
                         get_token("https://www.cloud_run_address_for.jlrint/"))

    def tearDown(self):
        del os.environ['APP_ENV']
