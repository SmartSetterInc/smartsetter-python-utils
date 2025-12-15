from django.contrib.gis.db import models
from model_utils.choices import Choices
from model_utils.models import TimeStampedModel

from smartsetter_utils.ssot.models.querysets import CommonFieldsQuerySet, CommonQuerySet
from smartsetter_utils.ssot.utils import format_phone


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
        "MLS", related_name="%(class)ss", null=True, on_delete=models.SET_NULL
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
        from smartsetter_utils.ssot.models.mls import MLS

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
