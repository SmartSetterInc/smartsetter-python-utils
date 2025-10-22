import json
from unittest.mock import patch

from dateutil.relativedelta import relativedelta
from django.utils import timezone

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
        self.assertEqual(agent.listing_production, listing_transaction.sold_price)
        self.assertEqual(agent.selling_production, selling_transaction.sold_price)

    def test_update_tenure(self):
        agent = self.make_agent()
        listing_transaction = self.make_transaction(
            listing_agent=agent,
            closed_date=(timezone.now() - relativedelta(years=5)).date(),
        )
        selling_transaction = self.make_transaction(
            selling_agent=agent, closed_date=timezone.now().date()
        )

        Agent.objects.update_tenure()

        agent.refresh_from_db()
        self.assertEqual(agent.tenure_start_date, listing_transaction.closed_date)
        self.assertEqual(agent.tenure_end_date, selling_transaction.closed_date)
        self.assertEqual(
            agent.tenure,
            selling_transaction.closed_date - listing_transaction.closed_date,
        )

    @patch("smartsetter_utils.ssot.models.run_task_in_transaction")
    def test_runs_submit_to_clay_webhook_task(self, mock_run_task):
        self.make_agent()

        self.assertEqual(mock_run_task.call_count, 2)
