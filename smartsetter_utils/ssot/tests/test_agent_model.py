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

    def test_update_cached_fields(self):
        agent = self.make_agent()
        city = "Mansoura"
        tx_1_date = (timezone.now() - relativedelta(years=5)).date()
        listing_transaction = self.make_transaction(
            listing_agent=agent,
            listing_contract_date=tx_1_date,
            closed_date=tx_1_date,
            city=city,
        )
        tx_2_date = timezone.now().date()
        selling_transaction = self.make_transaction(
            selling_agent=agent,
            listing_contract_date=tx_2_date,
            closed_date=tx_2_date,
            city=city,
        )
        listing_transaction_2 = self.make_transaction(
            listing_agent=agent,
            city="Not Mansoura",
            listing_contract_date=tx_2_date,
            closed_date=tx_2_date,
        )

        Agent.objects.update_cached_fields()

        agent.refresh_from_db()
        self.assertEqual(agent.listing_transactions_count, 1)
        self.assertEqual(agent.selling_transactions_count, 1)
        self.assertEqual(agent.total_transactions_count, 2)
        self.assertEqual(agent.listing_production, listing_transaction_2.sold_price)
        self.assertEqual(agent.selling_production, selling_transaction.sold_price)
        self.assertEqual(
            agent.total_production,
            agent.listing_production + agent.selling_production,
        )
        self.assertEqual(agent.tenure_start_date, listing_transaction.closed_date)
        self.assertEqual(agent.tenure_end_date, selling_transaction.closed_date)
        self.assertEqual(
            agent.tenure,
            selling_transaction.closed_date - listing_transaction.closed_date,
        )
        self.assertEqual(agent.most_transacted_city, city)
        self.assertEqual(
            agent.last_activity_date, listing_transaction_2.listing_contract_date
        )

    @patch("smartsetter_utils.ssot.models.run_task_in_transaction")
    def test_runs_submit_to_clay_webhook_task(self, mock_run_task):
        self.make_agent()

        self.assertEqual(mock_run_task.call_count, 1)

    def test_switch_to_mls_matview(self):
        mls = self.make_mls(table_name="TrebVOW")

        trebvow_mls_table = Agent.switch_to_mls_matview(mls)

        self.assertEqual(trebvow_mls_table._meta.db_table, "ssot_agent_trebvow")
