import requests
from django.conf import settings


def get_access_token():
    return requests.post(
        "https://realtyfeed-sso.auth.us-east-1.amazoncognito.com/oauth2/token",
        data={"client_id": "", "client_secret": "", "grant_type": "client_credentials"},
    ).json()["access_token"]


def get_session():
    access_token = get_access_token()
    session = requests.Session()
    session.headers.update(
        {
            "x-api-key": settings.REALITY_FEED_API_KEY,
            "Authorization": f"Bearer {access_token}",
        }
    )
    return session
