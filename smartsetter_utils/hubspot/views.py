import base64
import hashlib
import hmac
from urllib.parse import urlunparse

from django.conf import settings
from django.contrib.sites.models import Site
from rest_framework import response, views


class BaseHubspotWebhookView(views.APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request, *args, **kwargs):
        domain = Site.objects.get_current().domain
        uri = urlunparse(("https", domain, request.path, "", "", ""))
        message = (
            request.method.encode("utf-8")
            + uri.encode("utf-8")
            + request.body
            + str(request.headers["X-Hubspot-Request-Timestamp"]).encode("utf-8")
        )
        digest = hmac.new(
            key=settings.HUBSPOT_APP_CLIENT_SECRET.encode("utf-8"),
            msg=message,
            digestmod=hashlib.sha256,
        ).digest()
        b64 = base64.b64encode(digest).decode()
        hubspot_b64 = request.headers["X-Hubspot-Signature-V3"]
        if b64 != hubspot_b64:
            return response.Response(status=400)

        return self.handle_data()

    def handle_data(self):
        raise NotImplementedError
