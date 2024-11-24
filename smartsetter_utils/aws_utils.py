import csv
import tempfile

import boto3
from django.conf import settings


def read_brand_code_mapping_sheet():
    brand_mappings_csv_tempfile = tempfile.NamedTemporaryFile(suffix=".csv")
    get_boto_client("s3").download_file(
        "smartsetter-media",
        "brand-code-mapping-v2.csv",
        brand_mappings_csv_tempfile.name,
    )
    brand_mappings_csv_tempfile.seek(0)
    brand_mapping_csv_reader = csv.reader(open(brand_mappings_csv_tempfile.name))
    # skip the first 2 rows
    next(brand_mapping_csv_reader)
    next(brand_mapping_csv_reader)
    for row in brand_mapping_csv_reader:
        target_brand_code, wrong_brand_name, replacement_brand_name = (
            row[1],
            row[2],
            row[3],
        )
        wrong_brand_name = wrong_brand_name.strip('"')
        replacement_brand_name = replacement_brand_name.strip('"')
        yield target_brand_code, wrong_brand_name, replacement_brand_name


def get_boto_client(service_name):
    return boto3.client(
        service_name,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )
