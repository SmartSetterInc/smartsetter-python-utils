import json
import re

import googlemaps
from django.conf import settings
from django.contrib.gis.geos import GEOSGeometry, Point

from smartsetter_utils.elasticsearch import create_elasticsearch_connection

CANADA_ZIPCODE_RE = re.compile(r"[A-Z]\d[A-Z]\s?(\d[A-Z]\d)?$", re.IGNORECASE)
USA_ZIPCODE_RE = re.compile(r"\d{5}(-\d{4})?$")


def geocode_address(address, zip_code=None):
    components = {}
    if zip_code:
        if USA_ZIPCODE_RE.match(zip_code):
            components["country"] = "US"
        elif CANADA_ZIPCODE_RE.match(zip_code):
            components["country"] = "CA"
    geocode_res = get_googlemaps_client().geocode(
        address,
        components=components,
    )
    if geocode_res:
        location = geocode_res[0]["geometry"]["location"]
        return Point((location["lng"], location["lat"]))
    return None


def query_location_for_zipcode(zip_code):
    es = create_elasticsearch_connection()
    zipcode_location_response = es.search(
        index=settings.ES_ZIPCODE_POLYGONS_INDEX_NAME,
        body={"query": {"term": {"zip_code": zip_code}}, "size": 1},
    )
    if not zipcode_location_response["hits"]["hits"]:
        return None
    return create_geometry_from_geojson(
        zipcode_location_response["hits"]["hits"][0]["_source"]["location"]
    )


def create_geometry_from_geojson(geojson):
    return GEOSGeometry(
        json.dumps(geojson),
        srid=4326,
    )


def get_googlemaps_client():
    return googlemaps.Client(settings.GOOGLE_API_KEY)
