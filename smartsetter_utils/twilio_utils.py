from django.conf import settings
from twilio.rest import Client as TwilioClient


def get_twilio_client(sub_account_sid=None):
    return TwilioClient(
        settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN, sub_account_sid
    )
