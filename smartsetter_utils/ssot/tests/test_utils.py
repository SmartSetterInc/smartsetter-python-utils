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


class TestCommaSeparatedContainsFilters(TestCase):
    """
    The filter dialog sends "contains" / "does not contain" as a raw string, so
    a comma-separated list used to be matched as one literal value and excluded
    nothing. See split_filter_values.
    """

    def setUp(self):
        super().setUp()
        self.remax = self.make_agent(office_name="RE/MAX Premier Properties")
        self.coldwell = self.make_agent(office_name="Coldwell Banker Realty")
        self.other = self.make_agent(office_name="Keller Williams Realty")

    def test_contains_matches_any_comma_separated_term(self):
        filtered = apply_filter_to_queryset(
            Agent.objects.all(),
            {"field": "office_name", "type": "contains", "value": "RE/MAX, Coldwell"},
        )

        self.assertCountEqual(filtered, [self.remax, self.coldwell])

    def test_not_contains_excludes_every_comma_separated_term(self):
        filtered = apply_filter_to_queryset(
            Agent.objects.all(),
            {
                "field": "office_name",
                "type": "not_contains",
                "value": "RE/MAX, Coldwell",
            },
        )

        self.assertCountEqual(filtered, [self.other])

    def test_single_term_is_unchanged(self):
        filtered = apply_filter_to_queryset(
            Agent.objects.all(),
            {"field": "office_name", "type": "contains", "value": "Coldwell"},
        )

        self.assertCountEqual(filtered, [self.coldwell])

    def test_whitespace_and_empty_terms_are_ignored(self):
        filtered = apply_filter_to_queryset(
            Agent.objects.all(),
            {
                "field": "office_name",
                "type": "contains",
                "value": "  RE/MAX ,, Coldwell  ,",
            },
        )

        self.assertCountEqual(filtered, [self.remax, self.coldwell])

    def test_accepts_an_already_split_list(self):
        filtered = apply_filter_to_queryset(
            Agent.objects.all(),
            {
                "field": "office_name",
                "type": "contains",
                "value": ["RE/MAX", "Coldwell"],
            },
        )

        self.assertCountEqual(filtered, [self.remax, self.coldwell])
