from datetime import datetime
import isodate


def get_hubspot_timestamp_from_iso_date(date: str):
    if not date:
        return None
    if isinstance(date, datetime):
        return int(date.timestamp()) * 1000
    return int(isodate.parse_datetime(date).timestamp()) * 1000
