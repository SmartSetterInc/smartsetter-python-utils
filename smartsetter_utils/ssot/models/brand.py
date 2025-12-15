import functools
import mimetypes
import tempfile
import urllib.request

from django.contrib.gis.db import models
from django.core.files import File
from model_utils.models import TimeStampedModel

from smartsetter_utils.airtable.utils import get_airtable_table
from smartsetter_utils.aws_utils import read_brand_code_mapping_sheet


def brand_icon_upload_to(instance, filename):
    return f"brand_icons/{instance.name}/{filename}"


class Brand(TimeStampedModel):
    name = models.CharField(max_length=64, unique=True)
    code = models.CharField(max_length=64, unique=True, db_index=True)
    marks = models.JSONField(default=list)
    icon = models.ImageField(null=True, blank=True, upload_to=brand_icon_upload_to)
    icon_circular = models.ImageField(
        null=True, blank=True, upload_to=brand_icon_upload_to
    )

    def __str__(self):
        return self.name

    @classmethod
    def create_from_mapping_sheet(cls):
        cls.objects.all().delete()
        brand_code_to_info_map = {}
        for brand_code, mark, brand_name in read_brand_code_mapping_sheet():
            info = brand_code_to_info_map.setdefault(
                brand_code, {"name": brand_name.strip(), "marks": []}
            )
            info["marks"].append(mark)
        unique_brand_codes = set(brand_code_to_info_map.keys())
        brand_icons_table = get_airtable_table("tblAcQIbuff6vIRgO", "appvmPuuLyPsk0PXg")
        brands_with_icons = brand_icons_table.all(formula="IF({Logo}, TRUE(), FALSE())")
        brand_code_to_downloaded_file_map = {}
        for brand_record in brands_with_icons:
            fields = brand_record["fields"]
            brand_code = fields["Name"]
            unique_brand_codes.add(brand_code)
            icon_url = fields["Logo"][0]["url"]
            filename = fields["Logo"][0]["filename"]
            if mimetypes.guess_type(filename)[0] is None:
                file_extension = mimetypes.guess_extension(fields["Logo"][0]["type"])
                filename = f"icon{file_extension}"
            temp_download_file = tempfile.NamedTemporaryFile()
            urllib.request.urlretrieve(icon_url, temp_download_file.name)
            temp_download_file.seek(0)
            brand_code_to_downloaded_file_map[brand_code] = File(
                temp_download_file, name=filename
            )
        return cls.objects.bulk_create(
            [
                Brand(
                    code=code,
                    name=(
                        brand_code_to_info_map[code]["name"]
                        if brand_code_to_info_map.get(code)
                        else code
                    ),
                    marks=(
                        brand_code_to_info_map[code]["marks"]
                        if brand_code_to_info_map.get(code)
                        else []
                    ),
                    icon=brand_code_to_downloaded_file_map.get(code),
                )
                for code in unique_brand_codes
            ]
        )

    class Meta:
        ordering = ("name",)


@functools.cache
def cached_brands():
    return Brand.objects.all()
