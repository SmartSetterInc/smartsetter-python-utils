import csv
import datetime
import time

import isodate
import pymysql.cursors
import pymysql.err
from celery import shared_task
from django.conf import settings
from django.db.utils import IntegrityError
from hubspot.crm.companies import (
    SimplePublicObjectInputForCreate as HubSpotCompanyInputForCreate,
)
from hubspot.crm.contacts import (
    SimplePublicObjectInputForCreate as HubspotCreateContactInput,
)
from hubspot.crm.contacts.exceptions import ApiException as HubSpotContactsApiException

from smartsetter_utils.aws_utils import download_s3_file
from smartsetter_utils.core import Environments
from smartsetter_utils.geo_utils import geocode_address, query_location_for_zipcode
from smartsetter_utils.ssot.models import (
    MLS,
    Agent,
    BadDataException,
    Brand,
    Office,
    Transaction,
    Zipcode,
    cached_brands,
)
from smartsetter_utils.ssot.utils import format_phone, get_reality_db_hubspot_client


@shared_task
def import_from_reality_db():
    MLS.import_from_s3()
    Brand.create_from_mapping_sheet()

    iterate_all_create_in_batches(ModelClassMapper.office_id)
    # warning: doesn't assign brands to agents. Use pull_reality_db_updates instead
    iterate_all_create_in_batches(ModelClassMapper.agent_id)
    iterate_all_create_in_batches(ModelClassMapper.transaction_id)
    Agent.objects.update_cached_stats()


@shared_task(name="ssot.pull_reality_db_updates")
def pull_reality_db_updates(force=False):
    # force allows to run without hubspot updates
    if Environments.is_dev() and not force:
        return

    update_or_create_items(ModelClassMapper.office_id)
    update_or_create_items(ModelClassMapper.agent_id)
    update_or_create_items(ModelClassMapper.transaction_id)
    Agent.objects.update_cached_stats()


@shared_task
def process_agent_fields(agent_id, agent=None):
    if not agent:
        agent = Agent.objects.select_related("office").get(id=agent_id)

    for brand in cached_brands():
        for mark in brand.marks:
            if (
                agent.email
                and mark in agent.email.lower()
                or agent.office
                and agent.office.name
                and mark in agent.office.name.lower()
            ):
                agent.brand = brand
                break
        if agent.brand:
            break

    agent.location = query_location_for_zipcode(agent.zipcode)
    if not agent.location:
        agent.location = geocode_address(agent.address, agent.zipcode)

    if not agent.state:
        try:
            zipcode = Zipcode.objects.get(zipcode=agent.zipcode)
            agent.state = zipcode.state
        except Zipcode.DoesNotExist:
            pass

    agent.save()


@shared_task
def create_hubspot_offices():
    hubspot_client = get_reality_db_hubspot_client()
    # limit api calls to 100 per 10 seconds
    hubspot_created_offices_count = 0
    start_time = time.time()
    for office in Office.objects.filter(hubspot_id__isnull=True):
        hubspot_company = hubspot_client.crm.companies.basic_api.create(
            simple_public_object_input_for_create=HubSpotCompanyInputForCreate(
                properties=office.get_hubspot_dict()
            )
        )
        office.hubspot_id = hubspot_company.to_dict()["id"]
        office.save(update_fields=["hubspot_id"])
        hubspot_created_offices_count += 1
        if hubspot_created_offices_count == 99:
            seconds_passed = time.time() - start_time
            time_to_sleep = 10 - seconds_passed
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
            hubspot_created_offices_count = 0
            start_time = time.time()


@shared_task
def verify_agent_phones_from_validated_phones_sheet():
    validated_phones_file = download_s3_file("phone_validator.csv")
    validated_phones_csv_reader = csv.DictReader(
        open(validated_phones_file.name, newline="")
    )
    for row in validated_phones_csv_reader:
        if row["line type"] == "CELL PHONE":
            formatted_phone = format_phone(row["phone number"])
            if formatted_phone:
                Agent.objects.filter(phone=formatted_phone).update(
                    verified_phone=formatted_phone,
                    verified_phone_source=Agent.PHONE_VERIFIED_SOURCE_SHEET,
                )


@shared_task
def populate_hubspot_database(limit=None):
    def get_hubspot_date(date_str):
        return (
            isodate.datetime_isoformat(
                datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            )
            if date_str
            else None
        )

    hubspot_client = get_reality_db_hubspot_client()

    offices = Office.objects.filter(hubspot_id__isnull=True).select_related("mls")
    if limit:
        offices = offices[:limit]

    hubspot_contacts_csv = download_s3_file("hubspot_contacts_oct_28.csv")
    hubspot_contacts_csv_reader = csv.DictReader(
        open(hubspot_contacts_csv.name, newline="")
    )
    contact_email_to_data_map = {
        contact["Email"]: contact for contact in hubspot_contacts_csv_reader
    }
    contact_phone_to_data_map = {
        format_phone(contact["Phone Number"]): contact
        for contact in hubspot_contacts_csv_reader
    }

    for office in offices:
        company_create_response = hubspot_client.crm.companies.basic_api.create(
            simple_public_object_input_for_create=HubSpotCompanyInputForCreate(
                properties=office.get_hubspot_dict()
            )
        ).to_dict()
        office.hubspot_id = company_create_response["id"]

        agents = Agent.objects.filter(office=office)
        for agent in agents:
            email_match = contact_email_to_data_map.get(agent.email)
            phone_match = contact_phone_to_data_map.get(agent.phone)
            match = email_match or phone_match
            if match:
                try:
                    hubspot_contact = hubspot_client.crm.contacts.basic_api.create(
                        simple_public_object_input_for_create=HubspotCreateContactInput(
                            properties={
                                "firstname": match["First Name"],
                                "lastname": match["Last Name"],
                                "email": match["Email"],
                                "hs_lead_status": match["Lead Status"],
                                "phone": format_phone(match["Phone Number"]),
                                "state": match["State/Region"],
                                "city": match["City"],
                                "zip": match["Postal Code"],
                                "num_associated_deals": match[
                                    "Number of Associated Deals"
                                ],
                                "notes_next_activity_date": get_hubspot_date(
                                    match["Next Activity Date"]
                                ),
                                "jobtitle": match["Job Title"],
                            },
                            associations=[
                                {
                                    "types": [
                                        {
                                            "associationCategory": "HUBSPOT_DEFINED",
                                            "associationTypeId": 279,
                                        }
                                    ],
                                    "to": {"id": office.hubspot_id},
                                }
                            ],
                        )
                    ).to_dict()
                except HubSpotContactsApiException:
                    continue
                else:
                    agent.hubspot_id = hubspot_contact["id"]
        Agent.objects.bulk_update(agents, ["hubspot_id"])
    Office.objects.bulk_update(offices, ["hubspot_id"], batch_size=1000)


@shared_task
def iterate_all_create_in_batches(model_class_name: str):
    ModelClass = ModelClassMapper.get_model_class_from_id(model_class_name)
    connection = get_reality_db_connection()
    with connection.cursor() as cursor:
        guarded_cursor_execute(cursor, f"SELECT * FROM {ModelClass.reality_table_name}")
        # this fetchmany doesn't reduce memory usage because all data
        # has been fetched already
        while many_fetched := cursor.fetchmany(1000):
            instances = []
            for reality_dict in many_fetched:
                try:
                    instances.append(ModelClass.from_reality_dict(reality_dict))
                except BadDataException:
                    continue
            try:
                ModelClass.objects.bulk_create(instances)
            except IntegrityError:
                for instance in instances:
                    try:
                        instance.save()
                    except IntegrityError:
                        continue


@shared_task
def update_agent_cached_stats():
    Agent.objects.update_cached_stats()


@shared_task
def update_or_create_items(model_class_id):
    ModelClass = ModelClassMapper.get_model_class_from_id(model_class_id)
    connection = get_reality_db_connection()
    with connection.cursor() as cursor:
        guarded_cursor_execute(cursor, f"SELECT * FROM {ModelClass.reality_table_name}")
        for reality_dict in cursor.fetchall():
            item_id = ModelClass.get_id_from_reality_dict(reality_dict)
            try:
                ModelClass.objects.update_or_create(
                    id=item_id,
                    defaults=ModelClass.get_property_dict_from_reality_dict(
                        reality_dict
                    ),
                )
            except BadDataException:
                continue


def guarded_cursor_execute(cursor, statement):
    while True:
        try:
            cursor.execute(statement)
        except (pymysql.err.OperationalError, pymysql.err.InterfaceError):
            time.sleep(30)
        else:
            break


def get_reality_db_connection():
    return pymysql.connect(
        host=settings.REALITY_DB_HOST,
        user=settings.REALITY_DB_USER,
        password=settings.REALITY_DB_PASSWORD,
        database=settings.REALITY_DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )


class ModelClassMapper:
    agent_id = "a"
    office_id = "o"
    transaction_id = "t"

    @staticmethod
    def get_model_class_from_id(id):
        match id:
            case ModelClassMapper.agent_id:
                return Agent
            case ModelClassMapper.office_id:
                return Office
            case _:
                return Transaction
