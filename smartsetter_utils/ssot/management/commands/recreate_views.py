from django.core.management.base import BaseCommand
from smartsetter_utils.ssot.models.mls import MLS
from django.db import connection


class Command(BaseCommand):
    help = "Recreate all materialized views"

    def handle(self, *args, **kwargs):
        for mls in MLS.objects.all():
            self.stdout.write(self.style.SUCCESS("MLS views Process Starting"))
            with connection.cursor() as cursor:
                cursor.execute(
                    f"DROP MATERIALIZED VIEW IF EXISTS {mls.agent_materialized_view_table_name} CASCADE"
                )
                cursor.execute(
                    f"""
                    CREATE MATERIALIZED VIEW {mls.agent_materialized_view_table_name} AS
                    SELECT * FROM ssot_agent
                    WHERE mls_id = '{mls.id}'
                    """
                )

        self.stdout.write(self.style.SUCCESS("All views recreated"))