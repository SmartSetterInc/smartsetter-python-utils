from pathlib import Path
from unittest.mock import MagicMock

from django.test import override_settings
from test_plus.test import APITestCase as PlusAPITestCase
from test_plus.test import TestCase as PlusTestCase

from smartsetter_utils.elasticsearch import create_elasticsearch_connection


class TestMixin:
    def get_test_file(self, app_name, filename):
        return Path(__file__).parent.parent.joinpath(
            app_name, "tests", "files", filename
        )

    def read_test_file(self, app_name, filename, mode="r"):
        return self.get_test_file(app_name, filename).open(mode=mode).read()

    def mock_with_attributes(self, **kwargs):
        mock = MagicMock()
        for k, v in kwargs.items():
            setattr(mock, k, v)
        return mock


class ElasticsearchTestMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        with override_settings(DEBUG=True):
            self.es_client = create_elasticsearch_connection()

    def tearDown(self):
        self.es_client.indices.delete("_all")


class TestCase(TestMixin, PlusTestCase):
    pass


class APITestCase(TestMixin, PlusAPITestCase):
    pass
