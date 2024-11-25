import json

from django.contrib.gis.geos import GEOSGeometry


def create_geometry_from_geojson(geojson):
    return GEOSGeometry(
        json.dumps(geojson),
        srid=4326,
    )
