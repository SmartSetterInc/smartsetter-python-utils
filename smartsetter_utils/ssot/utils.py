import re

import phonenumbers
from django.conf import settings
from django.db import models
from django.db.models import Q

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


def apply_filter_to_queryset(queryset, filter):
    field_name = filter["field"]
    filter_method = queryset.filter
    filter_type = filter["type"]
    filter_value = filter.get("value")
    table_field = queryset.model._meta.get_field(field_name)
    field_is_text = isinstance(table_field, (models.CharField, models.TextField))
    if isinstance(filter_value, str):
        filter_value = filter_value.strip()
    if filter_type in ("is_not", "is_none_of", "not_contains", "not_exists"):
        filter_method = queryset.exclude
    field_lookup = None
    match filter_type:
        case "is" | "is_not":
            field_lookup = "iexact" if field_is_text else "exact"
        case "is_one_of" | "is_none_of":
            field_lookup = "in"
        case "contains" | "not_contains":
            field_lookup = "icontains"
        case "exists" | "not_exists":
            query = Q(
                **{
                    f"{field_name}__isnull": False,
                }
            )
            if field_is_text:
                query &= ~Q(**{field_name: ""})
            return filter_method(query)
        case "gt" | "lt":
            field_lookup = filter_type
    return filter_method(**{f"{field_name}__{field_lookup}": filter_value})
