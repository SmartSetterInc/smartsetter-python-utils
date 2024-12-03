import factory.django
from django.utils import timezone
from faker.providers import address, internet, misc, phone_number, python

from smartsetter_utils.ssot.models import MLS, Agent, Brand, Office, Transaction

factory.Faker.add_provider(address)
factory.Faker.add_provider(phone_number)
factory.Faker.add_provider(internet)
factory.Faker.add_provider(misc)
factory.Faker.add_provider(python)


class MLSFactory(factory.django.DjangoModelFactory):
    id = factory.Faker("pyint")
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
    office_id = factory.Faker("random_letter")
    address = factory.Faker("address")
    city = factory.Faker("city")
    zipcode = factory.Faker("postcode")
    phone = factory.Faker("phone_number")
    state = "SHORT"

    class Meta:
        model = Office


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
