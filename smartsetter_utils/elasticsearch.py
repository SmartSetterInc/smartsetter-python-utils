from django.conf import settings
from elasticsearch import Elasticsearch, RequestsHttpConnection


def create_elasticsearch_connection(url=settings.ELASTICSEARCH_RESEARCH_URL):
    if settings.DEBUG:
        conn_kwargs = {"hosts": [settings.ELASTICSEARCH_URL]}
    else:
        from requests_aws4auth import AWS4Auth

        url = url or settings.ELASTICSEARCH_URL
        conn_kwargs = {
            "hosts": [url],
            "http_auth": AWS4Auth(
                settings.AWS_ACCESS_KEY_ID,
                settings.AWS_SECRET_ACCESS_KEY,
                "us-west-2",
                "es",
            ),
            "use_ssl": True,
            "verify_certs": True,
            "connection_class": RequestsHttpConnection,
        }
    return Elasticsearch(**conn_kwargs)
