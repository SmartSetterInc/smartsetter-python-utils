import csv

from django.contrib.gis.db import models
from model_utils.models import TimeStampedModel

from smartsetter_utils.aws_utils import download_s3_file


class Zipcode(TimeStampedModel):
    zipcode = models.CharField(max_length=16, db_index=True)
    city = models.CharField(max_length=64)
    state = models.CharField(max_length=16)

    @classmethod
    def import_from_s3(cls):
        zipcodes_file = download_s3_file("Zipcodes.csv")
        csv_reader = csv.DictReader(open(zipcodes_file.name, "r"))
        Zipcode.objects.bulk_create(
            [
                Zipcode(zipcode=row["zip"], city=row["city"], state=row["state_id"])
                for row in csv_reader
            ],
            batch_size=1000,
        )
