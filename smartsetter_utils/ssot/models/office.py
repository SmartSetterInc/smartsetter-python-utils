import urllib3.exceptions
from django.conf import settings
from django.contrib.gis.db import models
from django.core import validators
from django_lifecycle import AFTER_UPDATE, BEFORE_CREATE, hook
from django_lifecycle.models import LifecycleModelMixin
from hubspot.crm.companies import (
    SimplePublicObjectInputForCreate as HubSpotCompanyInputForCreate,
)
from hubspot.crm.companies.exceptions import ApiException as CompanyApiException

from smartsetter_utils.core import Environments
from smartsetter_utils.hubspot.utils import get_hubspot_client
from smartsetter_utils.ssot.models.base_models import (
    AgentOfficeCommonFields,
    CommonFields,
    RealityDBBase,
)
from smartsetter_utils.ssot.models.querysets import CommonQuerySet
from smartsetter_utils.ssot.models.utils import get_hubspot_timestamp_from_iso_date
from smartsetter_utils.ssot.utils import get_brand_fixed_office_name


class OfficeQuerySet(CommonQuerySet):
    def filter_hubspot_material(self):
        return self.active()


class BadDataException(Exception):
    pass


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
