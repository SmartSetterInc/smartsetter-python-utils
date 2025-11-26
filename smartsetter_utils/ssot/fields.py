from django.core import validators
from django.db import models


class PercentageNumberField(models.PositiveSmallIntegerField):
    def __init__(self, *args, **kwargs):
        kwargs.update(
            {
                "null": True,
                "blank": True,
                "db_index": True,
                "validators": [
                    validators.MinValueValidator(0),
                    validators.MaxValueValidator(100),
                ],
            }
        )
        super().__init__(*args, **kwargs)
