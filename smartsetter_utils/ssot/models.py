import csv
import functools
import mimetypes
import re
import tempfile
import urllib.request
from decimal import Decimal
from typing import Any, List, Literal, Optional, TypedDict

import more_itertools
from django.conf import settings
from django.contrib.gis.db import models
from django.core.files import File
from django.db.models import F, Sum
from django.db.models.functions import Cast, Greatest
from django_lifecycle import AFTER_CREATE, AFTER_UPDATE, hook
from django_lifecycle.models import LifecycleModelMixin
from model_utils import Choices
from model_utils.models import TimeStampedModel

from smartsetter_utils.airtable.utils import get_airtable_table
from smartsetter_utils.aws_utils import read_brand_code_mapping_sheet
from smartsetter_utils.core import run_task_in_transaction
from smartsetter_utils.geo_utils import create_geometry_from_geojson
from smartsetter_utils.ssot.utils import format_phone, get_reality_db_hubspot_client


class CommonQuerySet(models.QuerySet):
    def get_by_id_or_none(self, id):
        try:
            return self.get(id=id)
        except Exception:
            return None


class MLS(TimeStampedModel):
    id = models.PositiveBigIntegerField(primary_key=True)
    name = models.CharField(max_length=256)
    table_name = models.CharField(max_length=64, null=True, blank=True)

    objects = CommonQuerySet.as_manager()

    def __str__(self):
        return self.name

    @classmethod
    def import_from_s3(cls):
        from smartsetter_utils.aws_utils import download_s3_file

        csv_sheet = download_s3_file("MLSID.csv")
        csv_reader = csv.DictReader(open(csv_sheet.name))
        cls.objects.bulk_create(
            [
                cls(
                    id=mls_row["MLS ID"],
                    name=mls_row["MLS Name"],
                    table_name=mls_row["Table Name"],
                )
                for mls_row in csv_reader
            ]
        )


def brand_icon_upload_to(instance, filename):
    return f"brand_icons/{instance.name}/{filename}"


class Brand(TimeStampedModel):
    name = models.CharField(max_length=64, unique=True)
    code = models.CharField(max_length=64, unique=True)
    marks = models.JSONField(default=list)
    icon = models.ImageField(null=True, blank=True, upload_to=brand_icon_upload_to)
    icon_circular = models.ImageField(
        null=True, blank=True, upload_to=brand_icon_upload_to
    )

    def __str__(self):
        return self.name

    @classmethod
    def create_from_mapping_sheet(cls):
        cls.objects.all().delete()
        brand_code_to_info_map = {}
        for brand_code, mark, brand_name in read_brand_code_mapping_sheet():
            info = brand_code_to_info_map.setdefault(
                brand_code, {"name": brand_name.strip(), "marks": []}
            )
            info["marks"].append(mark)
        unique_brand_codes = set(brand_code_to_info_map.keys())
        brand_icons_table = get_airtable_table("tblAcQIbuff6vIRgO", "appvmPuuLyPsk0PXg")
        brands_with_icons = brand_icons_table.all(formula="IF({Logo}, TRUE(), FALSE())")
        brand_code_to_downloaded_file_map = {}
        for brand_record in brands_with_icons:
            fields = brand_record["fields"]
            brand_code = fields["Name"]
            unique_brand_codes.add(brand_code)
            icon_url = fields["Logo"][0]["url"]
            filename = fields["Logo"][0]["filename"]
            if mimetypes.guess_type(filename)[0] is None:
                file_extension = mimetypes.guess_extension(fields["Logo"][0]["type"])
                filename = f"icon{file_extension}"
            temp_download_file = tempfile.NamedTemporaryFile()
            urllib.request.urlretrieve(icon_url, temp_download_file.name)
            temp_download_file.seek(0)
            brand_code_to_downloaded_file_map[brand_code] = File(
                temp_download_file, name=filename
            )
        return cls.objects.bulk_create(
            [
                Brand(
                    code=code,
                    name=(
                        brand_code_to_info_map[code]["name"]
                        if brand_code_to_info_map.get(code)
                        else code
                    ),
                    marks=(
                        brand_code_to_info_map[code]["marks"]
                        if brand_code_to_info_map.get(code)
                        else []
                    ),
                    icon=brand_code_to_downloaded_file_map.get(code),
                )
                for code in unique_brand_codes
            ]
        )

    class Meta:
        ordering = ("name",)


@functools.cache
def cached_brands():
    return Brand.objects.all()


class CommonEntity(TimeStampedModel):
    address = models.CharField(max_length=128, db_index=True)
    city = models.CharField(max_length=128, db_index=True)
    zipcode = models.CharField(max_length=32, db_index=True)
    phone = models.CharField(max_length=32, null=True, db_index=True)
    state = models.CharField(max_length=16, db_index=True)
    mls = models.ForeignKey(
        MLS, related_name="%(class)ss", null=True, on_delete=models.SET_NULL
    )

    objects = CommonQuerySet.as_manager()

    class Meta:
        abstract = True

    @staticmethod
    def get_common_properties_from_reality_dict(
        reality_dict, phone_field_name, zipcode_field_name="PostalCode"
    ):
        return {
            "address": reality_dict["Address"],
            "city": reality_dict["City"],
            "zipcode": reality_dict[zipcode_field_name],
            "phone": format_phone(reality_dict[phone_field_name]),
            "mls": MLS.objects.get_by_id_or_none(id=reality_dict["MLSID"]),
            "state": reality_dict["State"],
        }


class RealityDBBase:
    reality_table_name = None

    @classmethod
    def from_reality_dict(cls, reality_dict):
        raise NotImplementedError

    @staticmethod
    def get_id_from_reality_dict(reality_dict):
        raise NotImplementedError

    @staticmethod
    def get_property_dict_from_reality_dict(reality_dict):
        raise NotImplementedError


class BadDataException(Exception):
    pass


class Office(RealityDBBase, LifecycleModelMixin, CommonEntity):

    reality_table_name = "tblOffices"

    id = models.CharField(max_length=256, primary_key=True)
    name = models.CharField(max_length=128, db_index=True)
    office_id = models.CharField(max_length=128)
    hubspot_id = models.CharField(max_length=128, null=True, blank=True)

    def __str__(self):
        return self.name

    @hook(
        AFTER_UPDATE,
        when_any=["name", "address", "city", "zipcode", "phone", "state"],
        has_changed=True,
    )
    def handle_hubspot_properties_changed(self):
        from hubspot.crm.companies import SimplePublicObjectInput

        if settings.ENVIRONMENT == "dev":
            return

        hubspot_client = get_reality_db_hubspot_client()
        hubspot_client.crm.companies.basic_api.update(
            company_id=self.hubspot_id,
            simple_public_object_input=SimplePublicObjectInput(
                properties=self.get_hubspot_dict()
            ),
        )

    @classmethod
    def from_reality_dict(cls, reality_dict):
        return Office(
            id=cls.get_id_from_reality_dict(reality_dict),
            **cls.get_property_dict_from_reality_dict(reality_dict),
        )

    @staticmethod
    def get_id_from_reality_dict(reality_dict, office_id_field_name="OfficeID"):
        office_id = reality_dict[office_id_field_name]
        mls_id = reality_dict["MLSID"]
        return f"{office_id}__{mls_id}"

    @staticmethod
    def get_property_dict_from_reality_dict(reality_dict):
        office_name = reality_dict["Office"]
        for brand in cached_brands():
            for mark in brand.marks:
                if mark in office_name.lower():
                    office_name = re.sub(
                        mark, brand.name, office_name, flags=re.IGNORECASE
                    )
                    break
        data = {
            "name": office_name,
            "office_id": reality_dict["OfficeID"],
            **CommonEntity.get_common_properties_from_reality_dict(
                reality_dict, "Phone"
            ),
        }
        if data["name"] == data["address"]:
            raise BadDataException
        return data

    def get_hubspot_dict(self):
        return {
            "name": self.name,
            "address": self.address,
            "city": self.city,
            "zip": self.zipcode,
            "phone": self.phone,
            "state": self.state,
            "mls_board": self.mls.name if self.mls else None,
        }

    @property
    def hubspot_url(self):
        return (
            f"https://app.hubspot.com/contacts/{settings.REALITY_DB_HUBSPOT_PORTAL_ID}/company/{self.hubspot_id}"
            if self.hubspot_id
            else None
        )


class AgentQuerySet(CommonQuerySet):
    def annotate_extended_stats(self):
        return self.annotate(
            total_transactions_count=F("listing_transactions_count")
            + F("selling_transactions_count"),
            total_production=F("listing_production") + F("selling_production"),
            listing_ratio=Cast("listing_production", output_field=models.FloatField())
            / Greatest(F("total_production"), 1.0, output_field=models.FloatField()),
            average_transaction_price=F("total_production")
            * Decimal(1)
            / Greatest(F("total_transactions_count"), 1),
        )

    def update_cached_stats(self, continue_=True):
        # can't update using F expressions: Joined field references are not permitted in this query
        agents = self.all()
        if continue_:
            agent = agents.filter(
                listing_transactions_count=0, selling_transactions_count=0
            )
        for agent_group in more_itertools.chunked(agents, 1000):
            for agent in agent_group:
                agent.listing_transactions_count = agent.listing_transactions.count()
                agent.selling_transactions_count = agent.selling_transactions.count()
                agent.listing_production = (
                    agent.listing_transactions.aggregate(
                        listing_production=Sum("list_price")
                    )["listing_production"]
                    or 0
                )
                agent.selling_production = (
                    agent.selling_transactions.aggregate(
                        selling_production=Sum("sold_price")
                    )["selling_production"]
                    or 0
                )
            Agent.objects.bulk_update(
                agent_group,
                [
                    "listing_transactions_count",
                    "selling_transactions_count",
                    "listing_production",
                    "selling_production",
                ],
            )

    def filter_by_portal_filters(self, filters):
        type AllowedFilters = Literal[
            "city",
            "state",
            "zipcode",
            "phone",
            "mls_id",
            "sales_count",
            "total_dollar_ltm",
            "within_polygon",
        ]

        type AllowedTypes = Literal[
            "is",
            "is_not",
            "is_one_of",
            "is_none_of",
            "contains",
            "not_contains",
            "gt",
            "lt",
            "exists",
            "not_exists",
        ]

        class AgentFilter(TypedDict):
            field: AllowedFilters
            type: AllowedTypes
            value: Optional[Any]

        type AgentFilters = List[AgentFilter]

        typed_filters: AgentFilters = filters

        NUMBER_FIELDS = ("mls_id",)

        queryset = self.all()
        if not typed_filters:
            return queryset
        for filter in typed_filters:
            field_name = filter["field"]
            filter_value = filter.get("value")
            match field_name:
                case "sales_count":
                    queryset = queryset.annotate_extended_stats()
                    field_name = "total_transactions_count"
                case "total_dollar_ltm":
                    queryset = queryset.annotate_extended_stats()
                    field_name = "total_production"
                case "within_polygon":
                    queryset = queryset.filter(
                        location__intersects=create_geometry_from_geojson(filter_value)
                    )
                    continue
            filter_method = queryset.filter
            filter_type = filter["type"]
            if filter_type in ("is_not", "is_none_of", "not_contains", "not_exists"):
                filter_method = queryset.exclude
            field_lookup = None
            match filter_type:
                case "is" | "is_not":
                    field_lookup = "exact" if field_name in NUMBER_FIELDS else "iexact"
                case "is_one_of" | "is_none_of":
                    field_lookup = "in"
                case "contains" | "not_contains":
                    field_lookup = "icontains"
                case "exists" | "not_exists":
                    field_lookup = "isnull"
                    filter_value = False
                case "gt" | "lt":
                    field_lookup = filter_type
            queryset = filter_method(**{f"{field_name}__{field_lookup}": filter_value})
        return queryset

    def list_view_queryset(self):
        return self.select_related("mls", "brand").annotate_extended_stats()


class Agent(RealityDBBase, LifecycleModelMixin, CommonEntity):

    reality_table_name = "tblAgents"

    PHONE_VERIFIED_SOURCE_SHEET = "sheet"
    PHONE_VERIFIED_SOURCE_PHONE_VALIDATOR = "phone_validator"
    PHONE_VERIFIED_SOURCE_CLAY = "clay"

    PHONE_VERIFIED_SOURCE_CHOICES = Choices(
        (PHONE_VERIFIED_SOURCE_SHEET, "Sheet"),
        (PHONE_VERIFIED_SOURCE_PHONE_VALIDATOR, "Phone Validator API"),
        (PHONE_VERIFIED_SOURCE_CLAY, "Clay"),
    )

    id = models.CharField(max_length=32, primary_key=True)
    name = models.CharField(max_length=128, db_index=True)
    email = models.CharField(max_length=64, null=True, db_index=True)
    verified_phone = models.CharField(max_length=32, null=True, db_index=True)
    verified_phone_source = models.CharField(
        max_length=32, null=True, blank=True, choices=PHONE_VERIFIED_SOURCE_CHOICES
    )
    office = models.ForeignKey(
        Office, related_name="agents", null=True, blank=True, on_delete=models.SET_NULL
    )
    office_name = models.CharField(max_length=128, db_index=True)
    job_title = models.CharField(max_length=64, null=True, blank=True)
    brand = models.ForeignKey(
        Brand, related_name="agents", null=True, on_delete=models.SET_NULL
    )
    years_in_business = models.PositiveSmallIntegerField(db_index=True)
    # cached fields that can be calculated at query time but too slow to do so
    listing_transactions_count = models.PositiveIntegerField(default=0)
    selling_transactions_count = models.PositiveIntegerField(default=0)
    listing_production = models.PositiveBigIntegerField(default=0)
    selling_production = models.PositiveBigIntegerField(default=0)
    location = models.PointField(null=True, blank=True, srid=4326)
    hubspot_id = models.CharField(max_length=128, null=True, blank=True)

    objects = AgentQuerySet.as_manager()

    def __str__(self):
        return self.name

    @hook(AFTER_CREATE)
    def handle_after_create(self):
        from smartsetter_utils.ssot.tasks import process_agent_fields

        if settings.ENVIRONMENT == "dev":
            return

        run_task_in_transaction(process_agent_fields, self.id)

    @classmethod
    def from_reality_dict(cls, reality_dict):
        return Agent(
            id=cls.get_id_from_reality_dict(reality_dict),
            **cls.get_property_dict_from_reality_dict(reality_dict),
        )

    @staticmethod
    def get_id_from_reality_dict(reality_dict, agent_id_field="AgentID"):
        return f"{reality_dict[agent_id_field]}__{reality_dict['MLSID']}"

    @staticmethod
    def get_property_dict_from_reality_dict(reality_dict):
        return {
            "name": reality_dict["AgentName"].title(),
            "email": reality_dict["Email"].lower(),
            "office": Office.objects.get_by_id_or_none(
                Office.get_id_from_reality_dict(reality_dict)
            ),
            "office_name": reality_dict["OfficeName"],
            "years_in_business": reality_dict["YIB"],
            **CommonEntity.get_common_properties_from_reality_dict(
                reality_dict, "AgentPhone", "Zipcode"
            ),
        }


class Transaction(RealityDBBase, LifecycleModelMixin, TimeStampedModel):

    reality_table_name = "tblTransactions"

    id = models.CharField(max_length=32, primary_key=True)
    mls_number = models.CharField(max_length=32, db_index=True)
    mls = models.ForeignKey(
        MLS, related_name="transactions", null=True, on_delete=models.SET_NULL
    )
    address = models.CharField(max_length=128, db_index=True)
    district = models.CharField(max_length=128, db_index=True)
    community = models.CharField(max_length=128, db_index=True)
    city = models.CharField(max_length=128, db_index=True)
    county = models.CharField(max_length=64, db_index=True)
    zipcode = models.CharField(max_length=32, db_index=True)
    state_code = models.CharField(max_length=16, db_index=True)
    list_price = models.PositiveBigIntegerField(db_index=True)
    sold_price = models.PositiveBigIntegerField(db_index=True)
    days_on_market = models.IntegerField(db_index=True)
    closed_date = models.DateField(db_index=True)
    listing_agent = models.ForeignKey(
        Agent,
        related_name="listing_transactions",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    listing_office = models.ForeignKey(
        Office,
        related_name="listing_transactions",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    selling_agent = models.ForeignKey(
        Agent,
        related_name="selling_transactions",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    selling_office = models.ForeignKey(
        Office,
        related_name="selling_transactions",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )

    def __str__(self):
        return self.mls_number

    @hook(AFTER_CREATE)
    def handle_created(self):
        if self.listing_agent:
            self.listing_agent.listing_transactions_count += 1
            self.listing_agent.listing_production += self.list_price
            self.listing_agent.save()
        if self.selling_agent:
            self.selling_agent.selling_transactions_count += 1
            self.selling_agent.selling_production += self.sold_price
            self.selling_agent.save()

    @classmethod
    def from_reality_dict(cls, reality_dict):
        return Transaction(
            id=cls.get_id_from_reality_dict(reality_dict),
            **cls.get_property_dict_from_reality_dict(reality_dict),
        )

    @staticmethod
    def get_property_dict_from_reality_dict(reality_dict):
        return {
            "mls_number": reality_dict["MLSNumber"],
            "mls": MLS.objects.get_by_id_or_none(id=reality_dict["MLSID"]),
            "address": reality_dict["HomeAddress"],
            "district": reality_dict["DIST"],
            "community": reality_dict["Community"],
            "city": reality_dict["CITY"],
            "county": reality_dict["COUNTY"],
            "zipcode": reality_dict["ZIPCODE"],
            "state_code": reality_dict["StateCode"],
            "list_price": reality_dict["ListPrice"],
            "sold_price": reality_dict["SoldPrice"],
            "days_on_market": reality_dict["DOM"],
            "closed_date": reality_dict["ClosedDate"],
            "listing_agent": Agent.objects.get_by_id_or_none(
                Agent.get_id_from_reality_dict(reality_dict, "LAID")
            ),
            "listing_office": Office.objects.get_by_id_or_none(
                Office.get_id_from_reality_dict(reality_dict, "LOID")
            ),
            "selling_agent": Agent.objects.get_by_id_or_none(
                Agent.get_id_from_reality_dict(reality_dict, "SAID")
            ),
            "selling_office": Office.objects.get_by_id_or_none(
                Office.get_id_from_reality_dict(reality_dict, "SOID")
            ),
        }

    @staticmethod
    def get_id_from_reality_dict(reality_dict):
        return f"{reality_dict['MLSNumber']}__{reality_dict['MLSID']}"
