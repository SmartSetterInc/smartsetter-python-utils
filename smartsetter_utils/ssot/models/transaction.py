from dateutil.relativedelta import relativedelta
from django.contrib.gis.db import models
from django.db.models import Sum
from django.utils import timezone
from django_lifecycle import BEFORE_CREATE, hook
from django_lifecycle.models import LifecycleModelMixin
from model_utils.models import TimeStampedModel

from smartsetter_utils.core import Environments
from smartsetter_utils.ssot.models.agent import Agent
from smartsetter_utils.ssot.models.base_models import CommonFields, RealityDBBase
from smartsetter_utils.ssot.models.mls import MLS
from smartsetter_utils.ssot.models.office import Office
from smartsetter_utils.ssot.models.querysets import CommonQuerySet


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
    address = models.CharField(max_length=256, null=True, blank=True)
    district = models.CharField(max_length=256, null=True, blank=True)
    community = models.CharField(max_length=256, null=True, blank=True)
    city = models.CharField(max_length=256, null=True, blank=True)
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
