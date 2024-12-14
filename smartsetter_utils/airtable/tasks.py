from celery import shared_task

from smartsetter_utils.airtable.utils import AirtableWebhookAPI


@shared_task
def re_enable_webhooks():
    airtable_webhook_api = AirtableWebhookAPI()
    webhooks = airtable_webhook_api.list_webhooks()["webhooks"]
    for webhook in webhooks:
        if not webhook["areNotificationsEnabled"]:
            airtable_webhook_api.enable_webhook(webhook["id"])
