import csv
import functools
import mimetypes
import re
import tempfile
import urllib.request
from decimal import Decimal
from typing import Any, List, Literal, Optional, TypedDict

import isodate
import more_itertools
import urllib3.exceptions
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.contrib.gis.db import models
from django.core import validators
from django.core.files import File
from django.db import connection
from django.db.models import Count, F, Max, Min, Q, Sum
from django.db.models.functions import Cast, Coalesce, Greatest
from django.utils import timezone
from django_lifecycle import (
    AFTER_CREATE,
    AFTER_DELETE,
    AFTER_UPDATE,
    BEFORE_CREATE,
    hook,
)
from django_lifecycle.models import LifecycleModelMixin
from hubspot.crm.associations.v4.exceptions import (
    ApiException as AssociationsApiException,
)
from hubspot.crm.companies import (
    SimplePublicObjectInputForCreate as HubSpotCompanyInputForCreate,
)
from hubspot.crm.companies.exceptions import ApiException as CompanyApiException
from hubspot.crm.contacts import SimplePublicObjectInput as HubSpotContactInput
from hubspot.crm.contacts import (
    SimplePublicObjectInputForCreate as HubSpotContactInputForCreate,
)
from hubspot.crm.contacts.exceptions import ApiException as ContactApiException
from hubspot.crm.contacts.exceptions import ServiceException as ContactServiceException
from model_utils.choices import Choices
from model_utils.models import TimeStampedModel

from smartsetter_utils.airtable.utils import get_airtable_table
from smartsetter_utils.aws_utils import download_s3_file, read_brand_code_mapping_sheet
from smartsetter_utils.core import Environments, run_task_in_transaction
from smartsetter_utils.geo_utils import create_geometry_from_geojson
from smartsetter_utils.hubspot.utils import get_hubspot_client
from smartsetter_utils.ssot.utils import (
    apply_filter_to_queryset,
    format_phone,
    get_brand_fixed_office_name,
)


class CommonFieldsQuerySet(models.QuerySet):
    def reality(self):
        return self.filter(source=CommonFields.SOURCE_CHOICES.reality)

    def constellation(self):
        return self.filter(source=CommonFields.SOURCE_CHOICES.constellation)


class CommonFields(models.Model):
    SOURCE_CHOICES = Choices(
        ("reality", "Reality"), ("constellation", "Constellation1")
    )

    source = models.CharField(
        max_length=32,
        choices=SOURCE_CHOICES,
        default=SOURCE_CHOICES.constellation,
        db_index=True,
    )
    objects = CommonFieldsQuerySet.as_manager()

    class Meta:
        abstract = True


class CommonQuerySet(CommonFieldsQuerySet):
    def get_by_id_or_none(self, id):
        if id:
            try:
                return self.get(id=id)
            except Exception:
                return None

    def active(self):
        return self.filter(status="Active")


class MLS(LifecycleModelMixin, CommonFields, TimeStampedModel):
    MLS_NAME_LENGTH = 256

    id = models.CharField(max_length=32, primary_key=True)
    name = models.CharField(max_length=MLS_NAME_LENGTH)
    table_name = models.CharField(max_length=64, null=True, blank=True)
    company_hubspot_internal_value = models.CharField(
        max_length=MLS_NAME_LENGTH, null=True, blank=True
    )
    contact_hubspot_internal_value = models.CharField(
        max_length=MLS_NAME_LENGTH, null=True, blank=True
    )
    data_available_until = models.DateTimeField(null=True, blank=True)

    def get_company_hubspot_internal_value(self):
        return self.company_hubspot_internal_value or self.name

    def get_contact_hubspot_internal_value(self):
        return self.contact_hubspot_internal_value or self.name

    objects = CommonQuerySet.as_manager()

    def __str__(self):
        return self.name

    @hook(AFTER_CREATE)
    def handle_created(self):
        self.create_agent_materialized_view()

    @hook(AFTER_DELETE)
    def handle_deleted(self):
        with connection.cursor() as cursor:
            cursor.execute(
                f"DROP MATERIALIZED VIEW {self.agent_materialized_view_table_name}"
            )

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

    @property
    def agent_materialized_view_table_name(self):
        return f"{Agent._meta.db_table}_{self.table_name.lower()}"

    def create_agent_materialized_view(self):
        # mls-specific agent materialized view for MyMLS page
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE MATERIALIZED VIEW {self.agent_materialized_view_table_name} as
                SELECT * FROM {Agent._meta.db_table} WHERE mls_id = '{self.id}'
            """
            )

    def refresh_agent_materialized_view(self):
        with connection.cursor() as cursor:
            cursor.execute(
                f"REFRESH MATERIALIZED VIEW {self.agent_materialized_view_table_name}"
            )


def brand_icon_upload_to(instance, filename):
    return f"brand_icons/{instance.name}/{filename}"


class Brand(TimeStampedModel):
    name = models.CharField(max_length=64, unique=True)
    code = models.CharField(max_length=64, unique=True, db_index=True)
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


class AgentOfficeCommonFields(TimeStampedModel):
    CITY_FIELD_LENGTH = 128
    address = models.CharField(max_length=128, null=True, blank=True)
    city = models.CharField(
        max_length=CITY_FIELD_LENGTH, null=True, blank=True, db_index=True
    )
    zipcode = models.CharField(max_length=32, null=True, blank=True, db_index=True)
    location = models.PointField(null=True, blank=True, srid=4326)
    phone = models.CharField(max_length=32, null=True, db_index=True)
    state = models.CharField(max_length=16, null=True, blank=True, db_index=True)
    status = models.CharField(max_length=32, null=True, blank=True, db_index=True)
    mls = models.ForeignKey(
        MLS, related_name="%(class)ss", null=True, on_delete=models.SET_NULL
    )
    hubspot_id = models.CharField(max_length=128, null=True, blank=True)
    raw_data = models.JSONField(null=True, blank=True)

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

    @property
    def is_active(self):
        return self.status == "Active"


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


class OfficeQuerySet(CommonQuerySet):
    def filter_hubspot_material(self):
        return self.active()


class Office(RealityDBBase, LifecycleModelMixin, CommonFields, AgentOfficeCommonFields):

    reality_table_name = "tblOffices"

    id = models.CharField(max_length=256, primary_key=True)
    name = models.CharField(max_length=128, null=True, blank=True)
    office_id = models.CharField(max_length=128, null=True, blank=True)
    churn_score = models.FloatField(
        null=True,
        blank=True,
        db_index=True,
        validators=[validators.MaxValueValidator(30)],
    )

    objects = OfficeQuerySet.as_manager()

    def __str__(self):
        return self.name

    @hook(BEFORE_CREATE)
    def handle_before_create(self):
        from smartsetter_utils.ssot.tasks import handle_before_office_created

        if Environments.is_dev():
            return

        handle_before_office_created(self)

    @hook(
        AFTER_UPDATE,
        when_any=["name", "address", "city", "zipcode", "phone", "state", "status"],
        has_changed=True,
    )
    def handle_hubspot_properties_changed(self):
        if Environments.is_dev():
            return

        if self.hubspot_id:
            self.update_hubspot_properties(self.get_hubspot_dict())
        else:
            self.create_hubspot_company()

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
        data = {
            "name": get_brand_fixed_office_name(reality_dict["Office"]),
            "office_id": reality_dict["OfficeID"],
            **AgentOfficeCommonFields.get_common_properties_from_reality_dict(
                reality_dict, "Phone"
            ),
        }
        if data["name"] == data["address"]:
            raise BadDataException
        return data

    def get_hubspot_dict(self):
        hubspot_dict = {
            "name": self.name,
            "address": self.address,
            "city": self.city,
            "zip": self.zipcode,
            "phone": self.phone,
            "state": self.state,
            "mls_board": (
                self.mls.get_company_hubspot_internal_value() if self.mls else None
            ),
        }
        if self.source == self.SOURCE_CHOICES.constellation:
            hubspot_dict["resoofficekey"] = self.id
            hubspot_dict["resoofficestatus"] = self.status
            hubspot_dict["resomainofficekey"] = self.raw_data["MainOfficeKey"]
            hubspot_dict["resomainofficename"] = self.raw_data["MainOfficeName"]
            hubspot_dict["resoofficemlsid"] = self.raw_data["OfficeMlsId"]
            hubspot_dict["resoofficename"] = self.name
            hubspot_dict["originatingsystemname"] = self.raw_data[
                "OriginatingSystemName"
            ]
            hubspot_dict["rawmlsmodificationtimestamp"] = (
                get_hubspot_timestamp_from_iso_date(
                    self.raw_data["RawMlsModificationTimestamp"]
                )
            )
            hubspot_dict["sourcesystemid"] = self.raw_data["SourceSystemID"]
            hubspot_dict["sourcesystemname"] = self.raw_data["SourceSystemName"]
            hubspot_dict["reso_data_"] = "true"
        return hubspot_dict

    def get_full_hubspot_dict(self):
        return {
            **self.get_hubspot_dict(),
            **self.get_hubspot_stats_dict(),
            **self.get_hubspot_employee_count_dict(),
        }

    def create_hubspot_company(self):
        if not self.should_be_in_hubspot:
            return

        try:
            hubspot_company = get_hubspot_client().crm.companies.basic_api.create(
                simple_public_object_input_for_create=HubSpotCompanyInputForCreate(
                    properties=self.get_full_hubspot_dict()
                )
            )
        except (CompanyApiException, urllib3.exceptions.ProtocolError):
            return
        else:
            self.hubspot_id = hubspot_company.to_dict()["id"]
            self.save(update_fields=["hubspot_id"])

    def update_or_create_hubspot_company(self):
        if not self.should_be_in_hubspot:
            return

        if self.hubspot_id:
            self.update_hubspot_properties(self.get_full_hubspot_dict())
        else:
            self.create_hubspot_company()

    def update_hubspot_employee_count(self):
        if not self.hubspot_id:
            return

        self.update_hubspot_properties(self.get_hubspot_employee_count_dict())

    def update_hubspot_stats(self):
        if not self.hubspot_id:
            return

        self.update_hubspot_properties(self.get_hubspot_stats_dict())

    def update_hubspot_properties(self, properties: dict):
        from hubspot.crm.companies import SimplePublicObjectInput

        if not self.hubspot_id:
            return

        hubspot_client = get_hubspot_client()
        try:
            hubspot_client.crm.companies.basic_api.update(
                company_id=self.hubspot_id,
                simple_public_object_input=SimplePublicObjectInput(
                    properties=properties
                ),
            )
        except (CompanyApiException, urllib3.exceptions.ProtocolError):
            pass

    def get_hubspot_stats_dict(self):
        listing_transactions = self.listing_transactions.all()
        listing_transactions_12m = listing_transactions.filter_12m()
        selling_transactions = self.selling_transactions.all()
        selling_transactions_12m = selling_transactions.filter_12m()
        listing_production_12m = listing_transactions_12m.production()
        selling_production_12m = selling_transactions_12m.production()

        return {
            "sales_volume__12m_": listing_production_12m + selling_production_12m,
            "sales_listing_volume__12m_": listing_production_12m,
            "sales_buying_volume__12m_": selling_production_12m,
            "sales_listing_count__12m_": listing_transactions_12m.count(),
            "sales_buying_count__12m_": selling_transactions_12m.count(),
            "sales_volume__all_time_": listing_transactions.production()
            + selling_transactions.production(),
            "sales_count__all_time_": listing_transactions.count()
            + selling_transactions.count(),
        }

    def get_hubspot_employee_count_dict(self):
        return {"numberofemployees": self.agents.count()}

    @property
    def should_be_in_hubspot(self):
        return self.is_active

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
            listing_ratio=Cast("listing_production", output_field=models.FloatField())
            / Greatest(F("total_production"), 1.0, output_field=models.FloatField()),
            average_transaction_price=F("total_production")
            * Decimal(1)
            / Greatest(F("total_transactions_count"), 1),
        )

    def update_cached_fields(self):
        for agent_group in more_itertools.chunked(self.iterator(), 1000):
            for agent in agent_group:
                # stats
                # can't update using F expressions: Joined field references are not permitted in this query
                agent.listing_transactions_count = (
                    agent.listing_transactions.filter_12m().count()
                )
                agent.selling_transactions_count = (
                    agent.selling_transactions.filter_12m().count()
                )
                agent.total_transactions_count = (
                    agent.listing_transactions_count + agent.selling_transactions_count
                )
                agent.listing_production = (
                    agent.listing_transactions.filter_12m().production()
                )
                agent.selling_production = (
                    agent.selling_transactions.filter_12m().production()
                )
                agent.total_production = (
                    agent.listing_production + agent.selling_production
                )
                # tenure
                sold_transactions = Transaction.objects.filter_listing_or_selling(
                    agent
                ).sold()
                agent.tenure_start_date = sold_transactions.aggregate(
                    tenure_start_date=Min(
                        Coalesce("listing_contract_date", "closed_date")
                    )
                )["tenure_start_date"]
                agent.tenure_end_date = sold_transactions.aggregate(
                    tenure_end_date=Max(
                        Coalesce("listing_contract_date", "closed_date")
                    )
                )["tenure_end_date"]
                if agent.tenure_start_date:
                    agent.tenure = agent.tenure_end_date - agent.tenure_start_date
                # most transacted city
                most_transacted_city_tx = (
                    Transaction.objects.filter_listing_or_selling(agent)
                    .values("city")
                    .annotate(tx_count_per_city=Count("city"))
                    .order_by("-tx_count_per_city")
                    .values("city")
                    .first()
                )
                if most_transacted_city_tx:
                    agent.most_transacted_city = most_transacted_city_tx["city"]
                # last activity date
                agent.last_activity_date = (
                    Transaction.objects.filter_listing_or_selling(agent).aggregate(
                        max_listing_contract_date=Max("listing_contract_date")
                    )["max_listing_contract_date"]
                )

            Agent.objects.bulk_update(
                agent_group,
                [
                    "listing_transactions_count",
                    "selling_transactions_count",
                    "total_transactions_count",
                    "listing_production",
                    "selling_production",
                    "total_production",
                    "tenure_start_date",
                    "tenure_end_date",
                    "tenure",
                    "most_transacted_city",
                    "last_activity_date",
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
            filter["field"] = field_name
            queryset = apply_filter_to_queryset(queryset, filter)
        return queryset

    def list_view_queryset(self):
        return self.select_related("mls", "brand").annotate_extended_stats()

    def filter_hubspot_material(self):
        return self.active().exclude(
            Q(office__isnull=True) | Q(office__hubspot_id__isnull=True)
        )

    def filter_by_mls_materialized_view(self, mls: MLS):
        # must be applied as first query method
        return Agent.switch_to_mls_matview(mls).objects.all()


class Agent(RealityDBBase, LifecycleModelMixin, CommonFields, AgentOfficeCommonFields):

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
    name = models.CharField(max_length=128, null=True, blank=True, db_index=True)
    email = models.CharField(max_length=256, null=True, blank=True, db_index=True)
    verified_phone = models.CharField(max_length=32, null=True, blank=True)
    verified_phone_source = models.CharField(
        max_length=32, null=True, blank=True, choices=PHONE_VERIFIED_SOURCE_CHOICES
    )
    office = models.ForeignKey(
        Office, related_name="agents", null=True, blank=True, on_delete=models.SET_NULL
    )
    office_name = models.CharField(max_length=256, null=True, blank=True, db_index=True)
    job_title = models.CharField(max_length=256, null=True, blank=True)
    brand = models.ForeignKey(
        Brand, related_name="agents", null=True, on_delete=models.SET_NULL
    )
    years_in_business = models.PositiveSmallIntegerField(
        null=True, blank=True, db_index=True
    )
    # cached fields that can be calculated at query time but too slow to do so
    listing_transactions_count = models.PositiveIntegerField(default=0)
    selling_transactions_count = models.PositiveIntegerField(default=0)
    total_transactions_count = models.PositiveIntegerField(default=0, db_index=True)
    listing_production = models.PositiveBigIntegerField(default=0)
    selling_production = models.PositiveBigIntegerField(default=0)
    total_production = models.PositiveBigIntegerField(default=0, db_index=True)
    # used to skip fetching all agent transactions when we need their start/end dates
    tenure_start_date = models.DateField(null=True, blank=True)
    tenure_end_date = models.DateField(null=True, blank=True)
    # used to make tenure queries easier
    tenure = models.DurationField(null=True, blank=True, db_index=True)
    most_transacted_city = models.CharField(
        max_length=AgentOfficeCommonFields.CITY_FIELD_LENGTH,
        null=True,
        blank=True,
        db_index=True,
    )
    last_activity_date = models.DateField(null=True, blank=True, db_index=True)
    likelihood_to_move = models.FloatField(
        null=True,
        blank=True,
        db_index=True,
        validators=[validators.MaxValueValidator(100)],
    )

    objects = AgentQuerySet.as_manager()

    def __str__(self):
        return self.name

    @hook(AFTER_CREATE)
    def handle_after_create(self):
        from smartsetter_utils.ssot.tasks import handle_agent_created

        if Environments.is_dev():
            return

        run_task_in_transaction(handle_agent_created, self.id)

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
            "office_name": get_brand_fixed_office_name(reality_dict["OfficeName"]),
            "years_in_business": reality_dict["YIB"],
            **AgentOfficeCommonFields.get_common_properties_from_reality_dict(
                reality_dict, "AgentPhone", "Zipcode"
            ),
        }

    def get_hubspot_dict(self):
        mls_modification_timestamp = self.raw_data.get("RawMlsModificationTimestamp")
        return {
            "email": self.email,
            "firstname": self.raw_data["MemberFirstName"],
            "lastname": self.raw_data["MemberLastName"],
            "middle_name": self.raw_data["MemberMiddleName"],
            "full_name": self.raw_data["MemberFullName"],
            "company": self.office_name,
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "zip": self.zipcode,
            "phone": self.phone,
            "jobtitle": self.job_title,
            "mls_name__dropdown_": (
                self.mls.get_contact_hubspot_internal_value() if self.mls else None
            ),
            "memberdirectphone": self.raw_data["MemberDirectPhone"],
            "memberhomephone": self.raw_data["MemberHomePhone"],
            "resomemberkeyunique": self.raw_data["MemberKey"],
            "membermlsid": self.raw_data["MemberMlsId"],
            "membermlssecurityclass": self.raw_data["MemberMlsSecurityClass"],
            "resomembermobilephone": self.raw_data["MemberMobilePhone"],
            "memberpreferredphone": self.raw_data["MemberPreferredPhone"],
            "resomemberstatus": self.raw_data["MemberStatus"],
            "resomembertype": self.raw_data["MemberType"],
            "resomodificationtimestamp": self.raw_data["ModificationTimestamp"],
            "originatingsystemname": self.raw_data["OriginatingSystemName"],
            "rawmlsmodificationtimestamp": mls_modification_timestamp
            and get_hubspot_timestamp_from_iso_date(mls_modification_timestamp),
            "memberstatelicense": self.raw_data["MemberStateLicense"],
            "reso_data_": "true",
            **self.get_hubspot_stats_dict(),
        }

    def create_hubspot_contact(self, check_should_be_in_hubspot=True):
        if check_should_be_in_hubspot and not self.should_be_in_hubspot:
            return

        hubspot_client = get_hubspot_client()
        hubspot_contact_properties = self.get_hubspot_dict()
        hubspot_contact = None
        try:
            hubspot_contact = hubspot_client.crm.contacts.basic_api.create(
                simple_public_object_input_for_create=HubSpotContactInputForCreate(
                    properties=hubspot_contact_properties
                )
            )
        except ContactApiException as exc:
            if exc.reason == "Conflict":
                duplicate_contact_id = re.search(r"(?P<id>\d+)", exc.body).groupdict()[
                    "id"
                ]
                hubspot_contact_properties.pop("phone")
                try:
                    hubspot_contact = hubspot_client.crm.contacts.basic_api.update(
                        int(duplicate_contact_id),
                        simple_public_object_input=HubSpotContactInput(
                            properties=hubspot_contact_properties
                        ),
                    )
                except (
                    ContactApiException,
                    ContactServiceException,
                    urllib3.exceptions.ProtocolError,
                ):
                    pass

        except urllib3.exceptions.ProtocolError:
            pass

        if hubspot_contact:
            hubspot_contact_id = hubspot_contact.to_dict()["id"]
            self.hubspot_id = hubspot_contact_id
            self.save()

            try:
                hubspot_client.crm.associations.v4.basic_api.create(
                    object_type="contacts",
                    object_id=self.hubspot_id,
                    to_object_type="companies",
                    to_object_id=self.office.hubspot_id,
                    association_spec=[
                        {
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 279,
                        }
                    ],
                )
            except AssociationsApiException:
                pass

    def update_or_create_hubspot_contact(self, check_should_be_in_hubspot=True):
        if check_should_be_in_hubspot and not self.should_be_in_hubspot:
            return

        if self.hubspot_id:
            self.update_hubspot_properties(self.get_hubspot_dict())
        else:
            self.create_hubspot_contact(check_should_be_in_hubspot)

    def update_hubspot_stats(self):
        if not self.hubspot_id:
            return

        self.update_hubspot_properties(self.get_hubspot_stats_dict())

    def update_hubspot_properties(self, properties):
        from hubspot.crm.contacts import SimplePublicObjectInput

        try:
            get_hubspot_client().crm.contacts.basic_api.update(
                self.hubspot_id,
                simple_public_object_input=SimplePublicObjectInput(
                    properties=properties
                ),
            )
        except (urllib3.exceptions.ProtocolError, ContactApiException):
            pass

    def get_hubspot_stats_dict(self):
        listing_transactions_12m = self.listing_transactions.filter_12m()
        listing_production_12m = listing_transactions_12m.production()
        selling_transactions_12m = self.selling_transactions.filter_12m()
        selling_production_12m = selling_transactions_12m.production()

        return {
            "sales_volume__12m_": listing_production_12m + selling_production_12m,
            "sales_listing_volume__12m_": listing_production_12m,
            "sales_listing_count__12m_": listing_transactions_12m.count(),
            "sales_buying_volume__12m_": selling_production_12m,
            "sales_buying_count__12m_": selling_transactions_12m.count(),
            "sales_volume__all_time_": self.listing_production
            + self.selling_production,
            "sales_count__all_time_": self.listing_transactions_count
            + self.selling_transactions_count,
        }

    @property
    def should_be_in_hubspot(self):
        return self.is_active and self.office and self.office.hubspot_id

    @property
    def sales_volume_score(self):
        sales_volume_score = None
        if self.total_production == 0:
            sales_volume_score = 10
        elif self.total_production > 2e6:
            sales_volume_score = 0
        else:
            sales_volume_score = (2e6 - self.total_production) / 2e5
        return sales_volume_score

    @property
    def transaction_count_score(self):
        tx_count_score = 0
        if self.total_transactions_count == 0:
            tx_count_score = 10
        elif self.total_transactions_count > 10:
            tx_count_score = 10
        else:
            tx_count_score = 10 - self.total_transactions_count
        return tx_count_score

    @property
    def tenure_score(self):
        agent_tenure_score = 35
        if self.tenure:
            agent_tenure_years = (
                relativedelta(seconds=self.tenure.total_seconds()).days / 365
            )
            if agent_tenure_years > 7:
                agent_tenure_score = 0
            else:
                agent_tenure_score = (7 - agent_tenure_years) * 5
        return agent_tenure_score

    def get_office_size_score(self, office_size=None):
        office_size_score = 0
        if self.office:
            if office_size is None:
                office_size = self.office.agents.count()
            if office_size == 0:
                office_size_score = 0
            elif office_size > 75:
                office_size_score = 15
            else:
                office_size_score = office_size / 5
        return office_size_score, office_size

    @classmethod
    def switch_to_mls_matview(cls, mls: MLS):
        MLSAgentMeta = type(
            f"{mls.table_name}AgentMeta",
            (cls.Meta,),
            {"db_table": mls.agent_materialized_view_table_name},
        )
        MLSAgent = type(
            f"{mls.table_name}Agent",
            (cls,),
            {"Meta": MLSAgentMeta, "__module__": cls.__module__},
        )
        return MLSAgent


class AgentOfficeMovement(TimeStampedModel):
    agent = models.ForeignKey(
        Agent, related_name="office_movements", on_delete=models.CASCADE
    )
    from_office = models.ForeignKey(
        Office,
        related_name="out_movements",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
    )
    to_office = models.ForeignKey(
        Office, related_name="in_movements", on_delete=models.CASCADE
    )
    movement_date = models.DateField(db_index=True)


class TransactionQuerySet(CommonQuerySet):
    def filter_12m(self):
        year_ago = timezone.now() - relativedelta(years=1)
        return self.filter(closed_date__gte=year_ago)

    def filter_listing(self, agent):
        return self.filter(listing_agent=agent)

    def filter_selling(self, agent):
        return self.filter(selling_agent=agent)

    def filter_listing_or_selling(self, agent):
        return self.filter_listing(agent) | self.filter_selling(agent)

    def sold(self):
        return self.filter(closed_date__isnull=False)

    def production(self):
        return self.aggregate(production=Sum("sold_price"))["production"] or 0


class Transaction(RealityDBBase, LifecycleModelMixin, CommonFields, TimeStampedModel):

    reality_table_name = "tblTransactions"

    id = models.CharField(max_length=32, primary_key=True)
    mls_number = models.CharField(max_length=32, null=True, blank=True)
    mls = models.ForeignKey(
        MLS, related_name="transactions", null=True, on_delete=models.SET_NULL
    )
    address = models.CharField(max_length=128, null=True, blank=True)
    district = models.CharField(max_length=128, null=True, blank=True)
    community = models.CharField(max_length=128, null=True, blank=True)
    city = models.CharField(max_length=128, null=True, blank=True)
    county = models.CharField(max_length=64, null=True, blank=True)
    zipcode = models.CharField(max_length=32, null=True, blank=True)
    location = models.PointField(null=True, blank=True, srid=4326)
    property_type = models.CharField(max_length=32, null=True, blank=True)
    state_code = models.CharField(max_length=16, null=True, blank=True)
    list_price = models.PositiveBigIntegerField(null=True, blank=True)
    sold_price = models.PositiveBigIntegerField(null=True, blank=True)
    lease_price = models.PositiveIntegerField(null=True, blank=True)
    days_on_market = models.IntegerField(null=True, blank=True)
    closed_date = models.DateField(null=True, blank=True, db_index=True)
    listing_contract_date = models.DateField(null=True, blank=True, db_index=True)
    listing_agent = models.ForeignKey(
        Agent,
        related_name="listing_transactions",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    colisting_agent = models.ForeignKey(
        Agent,
        related_name="colisting_transactions",
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
    colisting_office = models.ForeignKey(
        Office,
        related_name="colisting_transactions",
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
    coselling_agent = models.ForeignKey(
        Agent,
        related_name="coselling_transactions",
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
    coselling_office = models.ForeignKey(
        Office,
        related_name="coselling_transactions",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    status = models.CharField(max_length=32, null=True, blank=True)
    raw_data = models.JSONField(null=True, blank=True)

    objects = TransactionQuerySet.as_manager()

    def __str__(self):
        return self.mls_number

    @hook(BEFORE_CREATE)
    def handle_before_create(self):
        from smartsetter_utils.ssot.tasks import handle_before_transaction_created

        if Environments.is_dev():
            return

        handle_before_transaction_created(self)

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


class Zipcode(TimeStampedModel):
    zipcode = models.CharField(max_length=16, db_index=True)
    city = models.CharField(max_length=64)
    state = models.CharField(max_length=16)

    @classmethod
    def import_from_s3(cls):
        zipcodes_file = download_s3_file("Zipcodes.csv")
        csv_reader = csv.DictReader(open(zipcodes_file.name, "r"))
        Zipcode.objects.bulk_create(
            [
                Zipcode(zipcode=row["zip"], city=row["city"], state=row["state_id"])
                for row in csv_reader
            ],
            batch_size=1000,
        )


def get_hubspot_timestamp_from_iso_date(date: str):
    if not date:
        return None
    return int(isodate.parse_datetime(date).timestamp()) * 1000
