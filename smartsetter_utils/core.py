import phonenumbers
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.db import transaction
from rest_framework import serializers


def run_task_in_transaction(task, *args, **kwargs):
    transaction.on_commit(lambda: task.delay(*args, **kwargs))


def format_phone(phone: str):
    try:
        return phonenumbers.format_number(
            phonenumbers.parse(phone, "US"),
            phonenumbers.PhoneNumberFormat.E164,
        )
    except phonenumbers.NumberParseException:
        return phone


def validate_passwords(password_1, password_2):

    if password_1 != password_2:
        raise serializers.ValidationError({"password_1": "Passwords are different"})
    try:
        validate_password(password_1)
    except ValidationError as ve:
        raise serializers.ValidationError({"password_1": ve.messages})
    return True
