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
        past_date = (timezone.now() - relativedelta(years=5)).date()
        listing_transaction = self.make_transaction(
            listing_agent=agent,
            listing_contract_date=past_date,
            closed_date=past_date,
            city=city,
        )
        date_12m = timezone.now().date()
        selling_transaction = self.make_transaction(
            selling_agent=agent,
            listing_contract_date=date_12m,
            closed_date=date_12m,
            city=city,
        )
        listing_transaction_2 = self.make_transaction(
            listing_agent=agent,
            city="Not Mansoura",
            listing_contract_date=date_12m,
            closed_date=date_12m,
        )
        colisting_transaction = self.make_transaction(
            colisting_agent=agent,
            closed_date=date_12m,
        )
        coselling_transaction = self.make_transaction(
            coselling_agent=agent, closed_date=date_12m
        )

        Agent.objects.update_cached_fields()

        agent.refresh_from_db()
        self.assertEqual(agent.listing_transactions_count, 1.5)
        self.assertEqual(agent.selling_transactions_count, 1.5)
        self.assertEqual(agent.total_transactions_count, 3)
        self.assertEqual(
            agent.listing_production,
            int(
                listing_transaction_2.sold_price + colisting_transaction.sold_price / 2
            ),
        )
        self.assertEqual(
            agent.selling_production,
            int(selling_transaction.sold_price + coselling_transaction.sold_price / 2),
        )
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

    @patch("smartsetter_utils.ssot.models.abstract_agent.run_task_in_transaction")
    def test_runs_submit_to_clay_webhook_task(self, mock_run_task):
        self.make_agent()

        self.assertEqual(mock_run_task.call_count, 1)

    def test_query_materialized_view(self):
        mls = self.make_mls(table_name="SAVANNAH GA")
        self.make_agent(mls=mls)

        self.assertEqual(Agent.objects.count(), 1)
        self.assertEqual(Agent.objects.filter_by_mls_materialized_view(mls).count(), 0)
        mls.refresh_agent_materialized_view()
        self.assertEqual(Agent.objects.filter_by_mls_materialized_view(mls).count(), 1)

    def test_filter_by_mls_id_portal_filter(self):
        mls = self.make_mls(table_name="SAVANNAH GA")
        self.make_agent(name="Test Candidate", mls=mls)
        mls.refresh_agent_materialized_view()

        self.assertEqual(
            Agent.objects.filter_by_portal_filters(
                [
                    {"field": "name", "type": "contains", "value": "test"},
                    {"field": "mls_id", "type": "is", "value": mls.id},
                ]
            ).count(),
            1,
        )
