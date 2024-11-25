import requests
from django.conf import settings
from pyairtable import Api as AirtableAPI

from smartsetter_utils.airtable.models import AirtableWebhook
from smartsetter_utils.core import absolute_link


class AirtableWebhookAPI:
    def __init__(self, base_id=settings.AIRTABLE_BASE_KEY):
        self.base_id = base_id
        self.base_url = f"https://api.airtable.com/v0/bases/{self.base_id}"

    def create_webhook(self, webhook_settings):
        response = requests.post(
            f"{self.base_url}/webhooks",
            json=webhook_settings,
            headers=self.get_auth_headers(),
        ).json()
        AirtableWebhook.objects.create(
            airtable_id=response["id"],
            base_id=self.base_id,
            mac_secret=response["macSecretBase64"],
        )
        return response

    def create_webhook_from_data(self, table_id, change_types, webhook_url_name):
        webhook_settings = {
            "specification": {
                "options": {
                    "filters": {
                        "dataTypes": ["tableData"],
                        "recordChangeScope": table_id,
                        "changeTypes": change_types,
                    }
                }
            },
            "notificationUrl": absolute_link(webhook_url_name),
        }
        return self.create_webhook(webhook_settings)

    def list_webhooks(self):
        return requests.get(
            f"{self.base_url}/webhooks", headers=self.get_auth_headers()
        ).json()

    def list_webhook_payloads(self, webhook_id, cursor=1, limit=50):
        payloads = []

        def get_payloads_response():
            return requests.get(
                f"{self.base_url}/webhooks/{webhook_id}/payloads?cursor={cursor}&limit={limit}",
                headers=self.get_auth_headers(),
            ).json()

        payloads_response = get_payloads_response()
        if new_payloads := payloads_response.get("payloads"):
            payloads.extend(new_payloads)
            while payloads_response.get("mightHaveMore"):
                cursor = payloads_response["cursor"]
                payloads_response = get_payloads_response()
                payloads.extend(payloads_response.get("payloads", []))
        return payloads

    def delete_webhook(self, webhook_id):
        response = requests.delete(
            f"{self.base_url}/webhooks/{webhook_id}", headers=self.get_auth_headers()
        )
        AirtableWebhook.objects.get(airtable_id=webhook_id).delete()
        return response

    def enable_webhook(self, webhook_id, enable=True):
        return requests.post(
            f"{self.base_url}/webhooks/{webhook_id}/enableNotifications",
            headers=self.get_auth_headers(),
            json={"enable": enable},
        )

    def get_auth_headers(self):
        return {"authorization": f"Bearer {settings.AIRTABLE_API_KEY}"}


def iterate_airtable_records(airtable_webhook: AirtableWebhook):
    airtable_webhook_api = AirtableWebhookAPI(airtable_webhook.base_id)
    payloads = airtable_webhook_api.list_webhook_payloads(airtable_webhook.airtable_id)
    records = []

    for payload in payloads:
        base_transaction_number = payload.get("baseTransactionNumber")
        if (
            not airtable_webhook.last_transaction_number
            or airtable_webhook.last_transaction_number < base_transaction_number
        ):
            airtable_webhook.last_transaction_number = base_transaction_number
            airtable_webhook.save()
            changed_table_id = list(payload["changedTablesById"].keys())[0]
            changed_table_data = payload["changedTablesById"][changed_table_id]
            created_record_id = None
            deleted_record_ids = []
            updated_record_id = None
            if "createdRecordsById" in changed_table_data:
                # record created
                created_record_id = list(
                    changed_table_data["createdRecordsById"].keys()
                )[0]
            elif "destroyedRecordIds" in changed_table_data:
                # record deleted
                deleted_record_ids.extend(changed_table_data["destroyedRecordIds"])
            elif "changedRecordsById" in changed_table_data:
                # record updated
                updated_record_id = list(
                    changed_table_data["changedRecordsById"].keys()
                )[0]
            if created_record_id:
                records.append((created_record_id, changed_table_id, "created"))
            elif deleted_record_ids:
                for deleted_record_id in deleted_record_ids:
                    records.append((deleted_record_id, changed_table_id, "deleted"))
            elif updated_record_id:
                records.append((updated_record_id, changed_table_id, "updated"))
    return records


def initiate_process_airtable_webhook(webhook_id):
    airtable_webhook = AirtableWebhook.objects.get(airtable_id=webhook_id)
    records = iterate_airtable_records(airtable_webhook)

    return airtable_webhook, records


def get_airtable_table(table_name, base_id=settings.AIRTABLE_BASE_KEY):
    return AirtableAPI(settings.AIRTABLE_API_KEY).table(base_id, table_name)
