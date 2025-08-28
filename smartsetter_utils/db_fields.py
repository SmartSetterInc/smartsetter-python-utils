from django.db import models
from model_utils import Choices


class TourStatusField(models.CharField):
    TOUR_STATUS_CHOICES = Choices(
        ("new", "New"), ("finished", "Finished"), ("skipped", "Skipped")
    )

    def __init__(self, *args, **kwargs):
        kwargs["max_length"] = 16
        kwargs["choices"] = self.TOUR_STATUS_CHOICES
        kwargs["default"] = self.TOUR_STATUS_CHOICES.new
        super().__init__(*args, **kwargs)
