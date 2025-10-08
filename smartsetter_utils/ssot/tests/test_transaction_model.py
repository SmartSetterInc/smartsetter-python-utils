import json
from unittest.mock import patch

from django.utils import timezone

from smartsetter_utils.ssot.models import Agent, Office, Transaction
from smartsetter_utils.ssot.tests.base import TestCase


@patch("smartsetter_utils.ssot.models.get_hubspot_client")
class TestTransactionModel(TestCase):
    def test_import_from_reality_data(self, _1):
        transaction_data = json.loads(
            self.read_test_file("ssot", "reality_transaction.json")
        )
        mls = self.make_mls(id=transaction_data["MLSID"])
        transaction_data["ClosedDate"] = timezone.now().date()
        agent = self.make_agent(
            id=Agent.get_id_from_reality_dict(transaction_data, "SAID")
        )
        listing_office = self.make_office(
            id=Office.get_id_from_reality_dict(transaction_data, "LOID")
        )
        selling_office = self.make_office(
            id=Office.get_id_from_reality_dict(transaction_data, "SOID")
        )

        transaction = Transaction.from_reality_dict(transaction_data)
        transaction.save()

        self.assertTrue(
            Transaction.objects.get(
                mls=mls,
                mls_number=transaction_data["MLSNumber"],
                listing_agent=agent,
                selling_agent=agent,
                listing_office=listing_office,
                selling_office=selling_office,
            )
        )
