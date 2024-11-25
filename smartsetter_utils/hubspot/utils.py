from django.conf import settings
from hubspot import HubSpot


def get_hubspot_client(access_token=None):
    return HubSpot(access_token=access_token or settings.HUBSPOT_ACCESS_TOKEN)
