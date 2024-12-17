from django.conf import settings
from twilio.rest import Client as TwilioClient


def get_twilio_client(account_sid=settings.TWILIO_ACCOUNT_SID):
    return TwilioClient(account_sid, settings.TWILIO_AUTH_TOKEN)
