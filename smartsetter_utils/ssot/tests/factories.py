from unittest.mock import patch

import factory.django
from django.utils import timezone
from faker.providers import address, internet, misc, phone_number, python

from smartsetter_utils.ssot.models import (
    MLS,
    Agent,
    AgentOfficeMovement,
    Brand,
    Office,
    Transaction,
)

factory.Faker.add_provider(address)
factory.Faker.add_provider(phone_number)
factory.Faker.add_provider(internet)
factory.Faker.add_provider(misc)
factory.Faker.add_provider(python)


class MLSFactory(factory.django.DjangoModelFactory):
    id = factory.Faker("pystr")
    name = factory.Faker("name")
    table_name = factory.Faker("name")

    class Meta:
        model = MLS


class BrandFactory(factory.django.DjangoModelFactory):
    name = "RE/MAX"
    code = "REMAX"
    marks = ["re/max", "re-max", "remax", "re max"]

    class Meta:
        model = Brand


class OfficeFactory(factory.django.DjangoModelFactory):
    id = factory.Faker("password")
    name = factory.Faker("name")
    office_id = factory.Faker("random_letter")
    address = factory.Faker("address")
    city = factory.Faker("city")
    zipcode = factory.Faker("postcode")
    phone = factory.Faker("phone_number")
    state = "SHORT"

    class Meta:
        model = Office

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        with patch(
            "smartsetter_utils.ssot.models.get_hubspot_client"
        ) as mock_hubspot_client:
            create_return_value = (
                mock_hubspot_client.return_value.crm.companies.basic_api.create.return_value
            )
            create_return_value.to_dict.return_value = {"id": "some-id"}
            return super()._create(model_class, *args, **kwargs)


class AgentFactory(factory.django.DjangoModelFactory):
    id = factory.Faker("password")
    name = factory.Faker("name")
    email = factory.Faker("email")
    office = factory.SubFactory(OfficeFactory)
    office_name = factory.Faker("name")
    years_in_business = factory.Faker("random_digit")
    address = factory.Faker("address")
    city = factory.Faker("city")
    zipcode = factory.Faker("postcode")
    phone = factory.Faker("phone_number")
    state = "SHORT"

    class Meta:
        model = Agent


class TransactionFactory(factory.django.DjangoModelFactory):
    id = factory.Faker("password")
    mls_number = factory.Faker("password")
    address = factory.Faker("address")
    district = factory.Faker("city")
    community = factory.Faker("city")
    city = factory.Faker("city")
    county = factory.Faker("country")
    zipcode = factory.Faker("postcode")
    state_code = factory.Faker("country_code")
    list_price = factory.Faker("pyint")
    sold_price = factory.Faker("pyint")
    days_on_market = factory.Faker("pyint")
    closed_date = factory.LazyFunction(timezone.now)

    class Meta:
        model = Transaction


class AgentOfficeThroughFactory(factory.django.DjangoModelFactory):
    agent = factory.SubFactory(AgentFactory)
    office = factory.SubFactory(OfficeFactory)

    class Meta:
        model = AgentOfficeMovement
