import logging
import re

import phonenumbers
from django.conf import settings
from django.core.exceptions import FieldDoesNotExist
from django.db import models
from django.db.models import Q

from smartsetter_utils.core import format_phone as utils_format_phone
from smartsetter_utils.hubspot.utils import get_hubspot_client

logger = logging.getLogger(__name__)


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
    from smartsetter_utils.ssot.models.brand import cached_brands

    for brand in cached_brands():
        for mark in brand.marks:
            if mark in office_name.lower():
                office_name = re.sub(mark, brand.name, office_name, flags=re.IGNORECASE)
                break
    return office_name


# A comma, semicolon or new line starts a new value (a pasted column too).
FILTER_VALUE_SEPARATORS = re.compile(r"[,;\r\n]+")

# Operator names the CRM filter dialog uses for the same lookups.
FILTER_TYPE_ALIASES = {
    "is_any_of": "is_one_of",
    "contains_any_of": "contains",
    "contains_none_of": "not_contains",
    "at_least": "gte",
    "at_most": "lte",
    "is_empty": "not_exists",
    "is_not_empty": "exists",
}

NEGATIVE_FILTER_TYPES = ("is_not", "is_none_of", "not_contains", "not_exists")


def split_filter_values(filter_value):
    """
    The values a list filter matches, from a string or a list.

    A string splits on commas, semicolons and new lines, so "Remax, Coldwell
    Banker" means either brokerage. A list keeps its items whole (an item
    may contain a comma). Text is trimmed and empty values dropped.
    """
    if filter_value is None:
        return []
    if isinstance(filter_value, (list, tuple)):
        values = filter_value
    elif isinstance(filter_value, str):
        values = FILTER_VALUE_SEPARATORS.split(filter_value)
    else:
        return [filter_value]
    return [
        value.strip() if isinstance(value, str) else value
        for value in values
        if value is not None and str(value).strip()
    ]


def _resolve_filter_field(model, field_name):
    field = None
    for part in field_name.split("__"):
        if model is None:
            return None
        try:
            field = model._meta.get_field(part)
        except FieldDoesNotExist:
            return None
        model = field.related_model
    return field


def _any_of(field_name, lookup, values):
    query = Q()
    for value in values:
        query |= Q(**{f"{field_name}__{lookup}": value})
    return query


def apply_filter_to_queryset(queryset, filter):
    """
    One portal filter on a column of `queryset`.

    Any operator takes a string or a list; "is" with a list means "is one
    of". Text matching ignores case, lists included. An empty filter, an
    unknown operator or an unknown field is ignored (and logged) instead of
    failing the request. "gt" / "lt" stay strict; "gte" / "lte" / "between"
    are inclusive.
    """
    field_name = filter.get("field")
    filter_type = FILTER_TYPE_ALIASES.get(filter.get("type"), filter.get("type"))
    filter_value = filter.get("value")
    table_field = _resolve_filter_field(queryset.model, field_name or "")
    if table_field is None:
        logger.info("Ignoring filter on unknown field %r", field_name)
        return queryset
    field_is_text = isinstance(table_field, (models.CharField, models.TextField))
    if isinstance(filter_value, str):
        filter_value = filter_value.strip()
    if filter_type in ("is", "is_not") and isinstance(filter_value, (list, tuple)):
        filter_type = "is_one_of" if filter_type == "is" else "is_none_of"
    filter_method = (
        queryset.exclude if filter_type in NEGATIVE_FILTER_TYPES else queryset.filter
    )
    match filter_type:
        case "exists" | "not_exists":
            query = Q(**{f"{field_name}__isnull": False})
            if field_is_text:
                query &= ~Q(**{field_name: ""})
            return filter_method(query)
        case "is" | "is_not":
            if filter_value is None or filter_value == "":
                return queryset
            lookup = "iexact" if field_is_text else "exact"
            return filter_method(**{f"{field_name}__{lookup}": filter_value})
        case "is_one_of" | "is_none_of":
            values = split_filter_values(filter_value)
            if not values:
                return queryset
            if field_is_text:
                return filter_method(_any_of(field_name, "iexact", values))
            return filter_method(**{f"{field_name}__in": values})
        case "contains" | "not_contains" | "starts_with":
            values = split_filter_values(filter_value)
            if not values:
                return queryset
            lookup = "istartswith" if filter_type == "starts_with" else "icontains"
            return filter_method(_any_of(field_name, lookup, values))
        case "gt" | "lt" | "gte" | "lte":
            if filter_value is None or filter_value == "":
                return queryset
            return filter_method(**{f"{field_name}__{filter_type}": filter_value})
        case "between":
            if not isinstance(filter_value, (list, tuple)) or len(filter_value) != 2:
                return queryset
            low, high = filter_value
            query = Q()
            if low not in (None, ""):
                query &= Q(**{f"{field_name}__gte": low})
            if high not in (None, ""):
                query &= Q(**{f"{field_name}__lte": high})
            return filter_method(query) if query else queryset
    logger.info("Ignoring filter with unknown type %r", filter_type)
    return queryset
