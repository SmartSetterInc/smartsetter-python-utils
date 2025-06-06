import json
from unittest.mock import patch

from smartsetter_utils.ssot.models import Agent, Office
from smartsetter_utils.ssot.tests.base import TestCase


class TestAgentModel(TestCase):
    def test_import_from_reality_data(self):
        agent_data = json.loads(self.read_test_file("ssot", "reality_agent.json"))
        mls = self.make_mls(id=agent_data["MLSID"])
        office = self.make_office(id=Office.get_id_from_reality_dict(agent_data))

        agent = Agent.from_reality_dict(agent_data)
        agent.save()

        self.assertTrue(
            Agent.objects.get(
                id=Agent.get_id_from_reality_dict(agent_data), office=office, mls=mls
            )
        )

    def test_update_cached_stats(self):
        agent = self.make_agent()
        listing_transaction = self.make_transaction(listing_agent=agent)
        selling_transaction = self.make_transaction(selling_agent=agent)

        Agent.objects.update_cached_stats()

        agent.refresh_from_db()
        self.assertEqual(agent.listing_transactions_count, 1)
        self.assertEqual(agent.selling_transactions_count, 1)
        self.assertEqual(agent.listing_production, listing_transaction.list_price)
        self.assertEqual(agent.selling_production, selling_transaction.sold_price)

    @patch("smartsetter_utils.ssot.models.run_task_in_transaction")
    def test_runs_submit_to_clay_webhook_task(self, mock_run_task):
        self.make_agent()

        self.assertEqual(mock_run_task.call_count, 2)
