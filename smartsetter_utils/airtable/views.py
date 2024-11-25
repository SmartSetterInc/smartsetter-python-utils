import base64
import hashlib
import hmac

from rest_framework import response, status, views

from smartsetter_utils.airtable.models import AirtableWebhook


class BaseAirtableWebhookView(views.APIView):
    authentication_classes = []
    permission_classes = []
    http_method_names = ["post"]
    webhook_url_name = None

    def post(self, request, *args, **kwargs):
        request_body = request.body
        self.request_data = request.data
        self.airtable_webhook = AirtableWebhook.objects.get(
            airtable_id=self.request_data["webhook"]["id"]
        )
        signature = hmac.new(
            base64.b64decode(self.airtable_webhook.mac_secret),
            request_body,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(
            "hmac-sha256=" + signature, request.META["HTTP_X_AIRTABLE_CONTENT_MAC"]
        ):
            return response.Response(status=status.HTTP_401_UNAUTHORIZED)

        return self.handle_records()

    def handle_records(self):
        raise NotImplementedError

    @classmethod
    def create_notification_webhook(cls):
        raise NotImplementedError
