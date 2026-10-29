from smartsetter_utils.ssot.models import Agent
from smartsetter_utils.ssot.tests.base import TestCase
from smartsetter_utils.ssot.utils import apply_filter_to_queryset, split_filter_values


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


class TestSharedFilterRules(TestCase):
    """The CRM filter rules: string or list, one split rule, case-insensitive."""

    def setUp(self):
        super().setUp()
        self.austin = self.make_agent(city="Austin", zipcode="78701", years_in_business=5)
        self.dallas = self.make_agent(city="DALLAS", zipcode="75201", years_in_business=6)
        self.houston = self.make_agent(city="Houston", zipcode="77001", years_in_business=None)

    def filtered(self, filter):
        return apply_filter_to_queryset(Agent.objects.all(), filter)

    def test_split_on_commas_semicolons_and_new_lines(self):
        self.assertEqual(
            split_filter_values("a, b;c\nd\r\n,"), ["a", "b", "c", "d"]
        )
        self.assertEqual(split_filter_values(["Realty, Inc. ", ""]), ["Realty, Inc."])

    def test_is_one_of_accepts_a_string_and_ignores_case(self):
        self.assertCountEqual(
            self.filtered({"field": "city", "type": "is_one_of", "value": "austin; dallas"}),
            [self.austin, self.dallas],
        )

    def test_is_with_a_list_means_any_of(self):
        self.assertCountEqual(
            self.filtered({"field": "city", "type": "is", "value": ["austin", "Dallas"]}),
            [self.austin, self.dallas],
        )
        self.assertCountEqual(
            self.filtered({"field": "city", "type": "is_not", "value": ["austin"]}),
            [self.dallas, self.houston],
        )

    def test_starts_with(self):
        self.assertCountEqual(
            self.filtered({"field": "zipcode", "type": "starts_with", "value": "787, 770"}),
            [self.austin, self.houston],
        )

    def test_inclusive_bounds_and_strict_legacy_bounds(self):
        field = "years_in_business"
        self.assertCountEqual(
            self.filtered({"field": field, "type": "gte", "value": 5}),
            [self.austin, self.dallas],
        )
        self.assertCountEqual(
            self.filtered({"field": field, "type": "lte", "value": 5}), [self.austin]
        )
        self.assertCountEqual(
            self.filtered({"field": field, "type": "between", "value": [6, None]}),
            [self.dallas],
        )
        self.assertCountEqual(
            self.filtered({"field": field, "type": "gt", "value": 5}), [self.dallas]
        )

    def test_empty_unknown_and_garbage_filters_are_ignored(self):
        everyone = [self.austin, self.dallas, self.houston]
        for filter in (
            {"field": "city", "type": "is_one_of", "value": []},
            {"field": "city", "type": "is_one_of", "value": " , "},
            {"field": "city", "type": "is", "value": ""},
            {"field": "city", "type": "sounds_like", "value": "x"},
            {"field": "no_such_field", "type": "is", "value": "x"},
            {"field": "years_in_business", "type": "gt", "value": ""},
        ):
            with self.subTest(filter=filter):
                self.assertCountEqual(self.filtered(filter), everyone)
