import isodate


def get_hubspot_timestamp_from_iso_date(date: str):
    if not date:
        return None
    return int(isodate.parse_datetime(date).timestamp()) * 1000
