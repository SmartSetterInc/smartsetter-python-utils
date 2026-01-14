import csv

from django.apps import apps
from django.contrib.gis.db import models
from django.db import connection
from django_lifecycle import AFTER_CREATE, AFTER_DELETE, hook
from django_lifecycle.models import LifecycleModelMixin
from model_utils.models import TimeStampedModel

from smartsetter_utils.ssot.models.base_models import CommonFields
from smartsetter_utils.ssot.models.querysets import CommonQuerySet


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
        from smartsetter_utils.ssot.models import Agent

        return f"{Agent._meta.db_table}_{self.source_alnum.lower()}_{self.table_name_alnum.lower()}"

    @property
    def agent_materialized_view_model_name(self):
        return (
            f"{self.source_alnum.capitalize()}{self.table_name_alnum.capitalize()}Agent"
        )

    @property
    def table_name_alnum(self):
        return self.get_alnum_str(self.table_name)

    @property
    def source_alnum(self):
        return self.get_alnum_str(self.source)

    def create_agent_materialized_view(self):
        from smartsetter_utils.ssot.models import Agent

        # mls-specific agent materialized view for MyMLS page
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE MATERIALIZED VIEW {self.agent_materialized_view_table_name} as
                SELECT * FROM {Agent._meta.db_table} WHERE status = 'Active' AND mls_id = '{self.id}'
            """
            )

    def refresh_agent_materialized_view(self):
        with connection.cursor() as cursor:
            cursor.execute(
                f"REFRESH MATERIALIZED VIEW {self.agent_materialized_view_table_name}"
            )

    @property
    def AgentMaterializedView(self):
        return apps.get_model("ssot", self.agent_materialized_view_model_name)

    def get_alnum_str(self, value: str):
        return "".join([char for char in value if char.isalnum()])
