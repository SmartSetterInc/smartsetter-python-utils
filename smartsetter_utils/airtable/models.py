from django.db import models
from model_utils.models import TimeStampedModel


class AirtableWebhook(TimeStampedModel):
    airtable_id = models.CharField(max_length=32, unique=True)
    base_id = models.CharField(max_length=32)
    mac_secret = models.CharField(max_length=256)
    last_transaction_number = models.PositiveIntegerField(null=True, blank=True)
