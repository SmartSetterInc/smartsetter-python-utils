from smartsetter_utils.ssot.models import Agent
from smartsetter_utils.ssot.tests.base import TestCase
from smartsetter_utils.ssot.utils import apply_filter_to_queryset


class TestApplyFilterToQuerySet(TestCase):
    def test_with_exists_query(self):
        agent_with_email = self.make_agents()

        filtered_agents = apply_filter_to_queryset(
            Agent.objects.all(), {"field": "email", "type": "exists"}
        )

        self.assertEqual(filtered_agents.count(), 1)
        self.assertEqual(filtered_agents.first(), agent_with_email)

    def test_with_not_exists_query(self):
        self.make_agents()

        filtered_agents = apply_filter_to_queryset(
            Agent.objects.all(), {"field": "email", "type": "not_exists"}
        )

        self.assertEqual(filtered_agents.count(), 2)

    def test_exists_with_number_field(self):
        self.make_agent(years_in_business=10)

        filtered_agents = apply_filter_to_queryset(
            Agent.objects.all(), {"field": "years_in_business", "type": "not_exists"}
        )

        self.assertEqual(filtered_agents.count(), 0)

    def make_agents(self):
        agent_with_email = self.make_agent(email="test@example.com")
        self.make_agent(email="")
        self.make_agent(email=None)
        return agent_with_email
