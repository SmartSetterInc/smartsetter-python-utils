from django.conf import settings
from hubspot import HubSpot


def get_hubspot_client(access_token=settings.HUBSPOT_ACCESS_TOKEN):
    return HubSpot(access_token=access_token)
