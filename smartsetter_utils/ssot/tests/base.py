from smartsetter_utils.ssot.tests.factories import (
    AgentFactory,
    BrandFactory,
    MLSFactory,
    OfficeFactory,
    TransactionFactory,
)
from smartsetter_utils.testing import TestCase as BaseTestCase


class TestCase(BaseTestCase):
    def make_mls(self, **kwargs):
        return MLSFactory(**kwargs)

    def make_brand(self, **kwargs):
        return BrandFactory(**kwargs)

    def make_office(self, **kwargs):
        return OfficeFactory(**kwargs)

    def make_agent(self, **kwargs):
        return AgentFactory(**kwargs)

    def make_transaction(self, **kwargs):
        return TransactionFactory(**kwargs)
