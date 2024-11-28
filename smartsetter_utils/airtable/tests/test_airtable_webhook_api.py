from unittest.mock import patch

from smartsetter_utils.airtable.models import AirtableWebhook
from smartsetter_utils.airtable.utils import AirtableWebhookAPI
from smartsetter_utils.testing import TestCase


@patch("smartsetter_utils.airtable.utils.requests")
class TestAirtableWebhookAPI(TestCase):
    def test_create_webhook(self, mock_requests):
        airtable_webhook_api = AirtableWebhookAPI("test-base")
        mock_requests.post.return_value.json.return_value = {
            "id": "ach00000000000000",
            "macSecretBase64": "someBase64MacSecret",
        }

        airtable_webhook_api.create_webhook(
            {
                "specification": {
                    "options": {
                        "filters": {
                            "dataTypes": ["tableData"],
                            "recordChangeScope": "tbl00000000000000",
                        }
                    }
                },
                "notificationUrl": "https://foo.com/receive-ping",
            }
        )

        mock_requests.post.assert_called_once()
        self.assertTrue(
            AirtableWebhook.objects.get(
                airtable_id="ach00000000000000",
                base_id="test-base",
                mac_secret="someBase64MacSecret",
            )
        )

    def test_list_webhook_payloads(self, mock_requests):
        airtable_webhook_api = AirtableWebhookAPI("test-base")
        mock_requests.get.return_value.json.return_value = {
            "payloads": [],
            "cursor": 5,
            "mightHaveMore": False,
        }

        self.assertEqual(airtable_webhook_api.list_webhook_payloads("test-id"), [])

    def test_delete_webhook(self, mock_requests):
        airtable_webhook = AirtableWebhook.objects.create(airtable_id="test-webhook-id")
        airtable_webhook_api = AirtableWebhookAPI("test-base")

        airtable_webhook_api.delete_webhook(airtable_webhook.airtable_id)

        mock_requests.delete.assert_called_once()
        self.assertRaises(
            AirtableWebhook.DoesNotExist, airtable_webhook.refresh_from_db
        )
