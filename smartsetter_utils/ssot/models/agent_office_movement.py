from django.contrib.gis.db import models
from model_utils.models import TimeStampedModel

from smartsetter_utils.ssot.models.agent import Agent
from smartsetter_utils.ssot.models.office import Office


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
