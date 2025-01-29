import re

import phonenumbers
from django.conf import settings

from smartsetter_utils.core import format_phone as utils_format_phone
from smartsetter_utils.hubspot.utils import get_hubspot_client


def format_phone(phone):
    if not phone:
        return None
    try:
        return utils_format_phone(phone)
    except phonenumbers.NumberParseException:
        return None


def get_reality_db_hubspot_client():
    return get_hubspot_client(settings.REALITY_DB_HUBSPOT_ACCESS_TOKEN)


def get_brand_fixed_office_name(office_name):
    from smartsetter_utils.ssot.models import cached_brands

    for brand in cached_brands():
        for mark in brand.marks:
            if mark in office_name.lower():
                office_name = re.sub(mark, brand.name, office_name, flags=re.IGNORECASE)
                break
    return office_name
