import re
from decimal import Decimal
from typing import Any, List, Literal, Optional, TypedDict

import more_itertools
import urllib3.exceptions
from dateutil.relativedelta import relativedelta
from django.contrib.gis.db import models
from django.core import validators
from django.db.models import Count, F, Max, Min, Q
from django.db.models.functions import Cast, Coalesce, Greatest
from django_lifecycle import AFTER_CREATE, hook
from django_lifecycle.models import LifecycleModelMixin
from hubspot.crm.associations.v4.exceptions import (
    ApiException as AssociationsApiException,
)
from hubspot.crm.contacts import SimplePublicObjectInput as HubSpotContactInput
from hubspot.crm.contacts import (
    SimplePublicObjectInputForCreate as HubSpotContactInputForCreate,
)
from hubspot.crm.contacts.exceptions import ApiException as ContactApiException
from hubspot.crm.contacts.exceptions import ServiceException as ContactServiceException
from model_utils.choices import Choices

from smartsetter_utils.core import Environments, run_task_in_transaction
from smartsetter_utils.geo_utils import create_geometry_from_geojson
from smartsetter_utils.hubspot.utils import get_hubspot_client
from smartsetter_utils.ssot.data import member_type_patterns, security_class_patterns
from smartsetter_utils.ssot.models.base_models import (
    AgentOfficeCommonFields,
    CommonFields,
    RealityDBBase,
)
from smartsetter_utils.ssot.models.brand import Brand
from smartsetter_utils.ssot.models.mls import MLS
from smartsetter_utils.ssot.models.office import Office
from smartsetter_utils.ssot.models.querysets import CommonQuerySet
from smartsetter_utils.ssot.models.utils import get_hubspot_timestamp_from_iso_date
from smartsetter_utils.ssot.utils import (
    apply_filter_to_queryset,
    get_brand_fixed_office_name,
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
        from smartsetter_utils.ssot.models.agent import Agent
        from smartsetter_utils.ssot.models.transaction import Transaction

        for agent_group in more_itertools.chunked(self.iterator(), 1000):
            for agent in agent_group:
                # stats
                # can't update using F expressions: Joined field references are not permitted in this query
                agent.listing_transactions_count = (
                    agent.listing_transactions.filter_12m().count()
                    + (agent.colisting_transactions.filter_12m().count() / 2)
                )
                agent.selling_transactions_count = (
                    agent.selling_transactions.filter_12m().count()
                    + (agent.coselling_transactions.filter_12m().count() / 2)
                )
                agent.total_transactions_count = (
                    agent.listing_transactions_count + agent.selling_transactions_count
                )
                agent.listing_production = (
                    agent.listing_transactions.filter_12m().production()
                    + (agent.colisting_transactions.filter_12m().production() / 2)
                )
                agent.selling_production = (
                    agent.selling_transactions.filter_12m().production()
                    + (agent.coselling_transactions.filter_12m().production() / 2)
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
                agent.assign_role()

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
                    "role",
                ],
            )

    def filter_by_portal_filters(self, filters):
        type AllowedFilters = Literal[
            "city",
            "state",
            "zipcode",
            "phone",
            "mls_id",
            "total_transactions_count",
            "total_production",
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
        # filter by mls_id first because that modifies the queryset
        sorted_filters = sorted(
            typed_filters, key=lambda filter: 0 if filter["field"] == "mls_id" else 1
        )
        for filter in sorted_filters:
            field_name = filter["field"]
            filter_value = filter.get("value")
            match field_name:
                case "mls_id":
                    mls = MLS.objects.get(id=filter_value)
                    queryset = queryset.filter_by_mls_materialized_view(mls)
                    continue
                case "within_polygon":
                    queryset = queryset.filter(
                        location__intersects=create_geometry_from_geojson(filter_value)
                    )
                    continue
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
        # also changes queryset type to that of dynamic subclass
        return mls.AgentMaterializedView.objects.all()


class AbstractAgent(
    RealityDBBase, LifecycleModelMixin, CommonFields, AgentOfficeCommonFields
):
    """
    This model is necessary so subclasses don't create a OneToOne relationship with Agent
    """

    class Meta:
        abstract = True

    reality_table_name = "tblAgents"

    ROLE_CHOICES = Choices(("agent", "Agent"), ("broker", "Broker"), ("other", "Other"))

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
        Office,
        related_name="%(class)ss",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    office_name = models.CharField(max_length=256, null=True, blank=True, db_index=True)
    job_title = models.CharField(max_length=256, null=True, blank=True)
    brand = models.ForeignKey(
        Brand, related_name="%(class)ss", null=True, on_delete=models.SET_NULL
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
    role = models.CharField(
        max_length=16, choices=ROLE_CHOICES, db_index=True, null=True, blank=True
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

    def assign_role(self):
        """
        Must be called after setting total_transactions_count
        """
        office_raw_data = self.office and self.office.raw_data
        raw_data = self.raw_data
        if office_raw_data and self.id in (
            office_raw_data.get("OfficeBrokerKey"),
            office_raw_data.get("OfficeManagerKey"),
            office_raw_data.get("OfficeBrokerMlsId"),
        ):
            self.role = self.ROLE_CHOICES.broker
        else:
            if self.total_transactions_count > 0:
                self.role = self.ROLE_CHOICES.agent
            elif raw_data:

                def is_role_other(value: str, patterns):
                    if value:
                        value_normalized = value.lower().strip()
                        for pattern in patterns:
                            if value_normalized in pattern:
                                return True
                    return False

                member_type = raw_data.get("MemberType")
                security_class = raw_data.get("MemberMlsSecurityClass")
                if is_role_other(member_type, member_type_patterns) or is_role_other(
                    security_class, security_class_patterns
                ):
                    self.role = self.ROLE_CHOICES.other
                else:
                    self.role = self.ROLE_CHOICES.agent

    @classmethod
    def from_reality_dict(cls, reality_dict):
        from smartsetter_utils.ssot.models.agent import Agent

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
