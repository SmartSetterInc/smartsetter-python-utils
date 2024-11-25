import phonenumbers
from django.db import transaction


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
