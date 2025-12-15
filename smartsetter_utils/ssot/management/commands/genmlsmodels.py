import io
import json
from pathlib import Path
from textwrap import dedent

from django.apps import apps
from django.core.management.base import BaseCommand

from smartsetter_utils.ssot.models.mls import MLS


class Command(BaseCommand):
    def handle(self, *args, **options):
        mlss_dump_path = Path(__file__).resolve().parent.joinpath("mlss.json")
        mlss_json = json.load(mlss_dump_path.open("r"))
        models_buffer = io.StringIO()
        models_buffer.write(
            "from smartsetter_utils.ssot.models.abstract_agent import AbstractAgent\n\n"
        )
        for mls_json in mlss_json:
            mls = MLS.objects.get(id=mls_json["pk"])
            models_buffer.write(
                dedent(
                    f"""
                    class {mls.agent_materialized_view_model_name}(AbstractAgent):
                        class Meta:
                            db_table = "{mls.agent_materialized_view_table_name}"
                            managed = False\n
                    """
                )
            )
        Path(
            apps.get_app_config("ssot").path, "models", "materialized_view_agent.py"
        ).write_text(models_buffer.getvalue())
