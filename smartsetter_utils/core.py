import enum
from urllib.parse import urlunparse

import phonenumbers
from django.conf import settings
from django.contrib.auth.password_validation import validate_password
from django.contrib.sites.models import Site
from django.core.exceptions import ValidationError
from django.db import transaction
from django.urls import reverse
from rest_framework import serializers


def run_task_in_transaction(task, *args, **kwargs):
    transaction.on_commit(lambda: task.delay(*args, **kwargs))


def format_phone(phone: str):
    return phonenumbers.format_number(
        phonenumbers.parse(phone, "US"),
        phonenumbers.PhoneNumberFormat.E164,
    )


def optional_parse_phone(phone: str):
    if not phone:
        return None
    try:
        return format_phone(phone)
    except phonenumbers.NumberParseException:
        return None


def validate_passwords(password_1, password_2):

    if password_1 != password_2:
        raise serializers.ValidationError({"password_1": ["Passwords are different"]})
    try:
        validate_password(password_1)
    except ValidationError as ve:
        raise serializers.ValidationError({"password_1": ve.messages})
    return True


def absolute_link(url_name, *args, **kwargs):
    site = Site.objects.get_current()
    protocol = "https"
    return urlunparse(
        (protocol, site.domain, reverse(url_name, args=args, kwargs=kwargs), "", "", "")
    )


class Environments(enum.StrEnum):
    TESTING = "testing"
    LIVE_DEV = "live-dev"
    LIVE_PROD = "live-prod"

    @classmethod
    def is_testing(cls):
        return settings.ENVIRONMENT == cls.TESTING

    @classmethod
    def is_dev(cls):
        return settings.ENVIRONMENT in [cls.LIVE_DEV, "development"]

    @classmethod
    def is_prod(cls):
        return settings.ENVIRONMENT == cls.LIVE_PROD
