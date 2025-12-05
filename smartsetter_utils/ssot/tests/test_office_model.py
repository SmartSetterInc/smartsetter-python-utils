import json
from unittest.mock import patch

from django.contrib.gis.geos import Point

from smartsetter_utils.ssot.models import Office
from smartsetter_utils.ssot.tests.base import TestCase


class TestOfficeModel(TestCase):
    @patch("smartsetter_utils.ssot.tasks.handle_before_office_created")
    def test_import_from_reality_data(self, _1):
        office_data = self.get_office_data()
        mls = self.make_mls(id=office_data["MLSID"])

        office = Office.from_reality_dict(office_data)
        office.save()

        self.assertTrue(Office.objects.get(office_id=office_data["OfficeID"], mls=mls))

    @patch("smartsetter_utils.ssot.tasks.handle_before_office_created")
    def test_updates_brand_name_in_office_name(self, _1):
        self.make_brand()
        office_data = self.get_office_data()
        office_data["Office"] = "re-max Reality Stuff re-max"

        office = Office.from_reality_dict(office_data)
        office.save()

        office.refresh_from_db()
        # works only when tested independently like with --lf flag
        self.assertEqual(office.name, "RE/MAX Reality Stuff RE/MAX")

    @patch("smartsetter_utils.ssot.tasks.get_location_from_zipcode_or_address")
    def test_before_create_signal_handler(self, mock_get_location):
        location = Point(0, 0)
        mock_get_location.return_value = location

        office = Office.objects.create(id="whatever")

        office.refresh_from_db()
        self.assertEqual(office.location, location)

    def get_office_data(self):
        return json.loads(self.read_test_file("ssot", "reality_office.json"))
