import json
from unittest.mock import patch

from smartsetter_utils.ssot.models import Office
from smartsetter_utils.ssot.tests.base import TestCase


@patch("smartsetter_utils.ssot.models.get_reality_db_hubspot_client")
class TestOfficeModel(TestCase):
    def test_import_from_reality_data(self, mock_hubspot_client):
        self.mock_hubspot_company_create(mock_hubspot_client)
        office_data = self.get_office_data()
        mls = self.make_mls(id=office_data["MLSID"])

        office = Office.from_reality_dict(office_data)
        office.save()

        self.assertTrue(Office.objects.get(office_id=office_data["OfficeID"], mls=mls))

    def test_updates_hubspot_when_name_changes(self, mock_hubspot_client):
        office = self.make_office()

        office.name = "Home Office"
        office.save()

        mock_hubspot_client.return_value.crm.companies.basic_api.update.assert_called_once()

    def test_updates_brand_name_in_office_name(self, mock_hubspot_client):
        self.mock_hubspot_company_create(mock_hubspot_client)
        self.make_brand()
        office_data = self.get_office_data()
        office_data["Office"] = "re-max Reality Stuff re-max"

        office = Office.from_reality_dict(office_data)
        office.save()

        office.refresh_from_db()
        # works only when tested independently like with --lf flag
        self.assertEqual(office.name, "RE/MAX Reality Stuff RE/MAX")

    def get_office_data(self):
        return json.loads(self.read_test_file("ssot", "reality_office.json"))

    def mock_hubspot_company_create(self, mock_hubspot_client):
        create_return_value = (
            mock_hubspot_client.return_value.crm.companies.basic_api.create.return_value
        )
        create_return_value.to_dict.return_value = {"id": "some-id"}
