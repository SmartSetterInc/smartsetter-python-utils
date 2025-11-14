from django.contrib import admin
from django.utils.html import format_html

from smartsetter_utils.ssot.models import MLS, Agent, Office, Transaction


class NoAddMixin:
    extra = 0
    show_change_link = True

    def has_add_permission(self, *args):
        return False


@admin.register(Office)
class OfficeAdmin(admin.ModelAdmin):
    class AgentsInline(NoAddMixin, admin.StackedInline):
        model = Agent
        fields = (
            "name",
            "email",
            "phone",
            "address",
            "city",
            "zipcode",
            "state",
            "years_in_business",
            "mls",
        )
        readonly_fields = fields

    class ListingTransactionsInline(NoAddMixin, admin.StackedInline):
        class ListingTransactionProxy(Transaction):
            class Meta:
                proxy = True
                verbose_name = "Listing Transaction"

        model = ListingTransactionProxy
        fk_name = "listing_office"
        fields = (
            "address",
            "district",
            "community",
            "city",
            "county",
            "zipcode",
            "state_code",
            "days_on_market",
            "list_price",
            "sold_price",
            "closed_date",
            "listing_agent",
            "selling_agent",
        )
        readonly_fields = (
            "address",
            "district",
            "community",
            "city",
            "county",
            "zipcode",
            "state_code",
            "days_on_market",
            "list_price",
            "sold_price",
            "closed_date",
        )
        raw_id_fields = ("listing_agent", "selling_agent")

    class SellingTransactionsInline(ListingTransactionsInline):
        class SellingTransactionProxy(Transaction):
            class Meta:
                proxy = True
                verbose_name = "Selling Transaction"

        model = SellingTransactionProxy
        fk_name = "selling_office"

    fields = (
        "name",
        "address",
        "city",
        "state",
        "zipcode",
        "phone",
        "mls",
        "hubspot_link",
    )
    readonly_fields = ("hubspot_link", "mls")
    search_fields = (
        "name",
        "address",
        "city",
        "state",
        "zipcode",
        "phone",
        "mls__name",
    )
    inlines = [AgentsInline, ListingTransactionsInline, SellingTransactionsInline]

    @admin.display(description="HubSpot Link")
    def hubspot_link(self, office):
        url = office.hubspot_url
        return (
            format_html(
                """<a href="{}" target="_blank">View HubSpot Company</a>""", url
            )
            if url
            else None
        )


common_transaction_fields = (
    "address",
    "district",
    "community",
    "city",
    "county",
    "zipcode",
    "state_code",
    "days_on_market",
    "list_price",
    "sold_price",
    "closed_date",
)


@admin.register(Agent)
class AgentAdmin(admin.ModelAdmin):
    class ListingTransactionsInline(NoAddMixin, admin.StackedInline):
        class ListingTransactionProxy(Transaction):
            class Meta:
                proxy = True
                verbose_name = "Listing Transaction"

        model = ListingTransactionProxy
        fk_name = "listing_agent"
        fields = common_transaction_fields + ("selling_agent",)
        readonly_fields = common_transaction_fields
        raw_id_fields = ("selling_agent",)

    class SellingTransactionsInline(NoAddMixin, admin.StackedInline):
        class SellingTransactionProxy(Transaction):
            class Meta:
                proxy = True
                verbose_name = "Selling Transaction"

        model = SellingTransactionProxy
        fk_name = "selling_agent"
        fields = common_transaction_fields + ("listing_agent",)
        readonly_fields = common_transaction_fields
        raw_id_fields = ("listing_agent",)

    fields = (
        "name",
        "email",
        "phone",
        "verified_phone",
        "address",
        "city",
        "zipcode",
        "state",
        "years_in_business",
        "mls",
        "office",
    )
    raw_id_fields = ("office",)
    search_fields = (
        "name",
        "email",
        "phone",
        "verified_phone",
        "address",
        "city",
        "zipcode",
        "state",
        "mls__name",
    )
    inlines = [ListingTransactionsInline, SellingTransactionsInline]


@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    foreign_fields = (
        "listing_agent",
        "listing_office",
        "selling_agent",
        "selling_office",
    )
    fields = common_transaction_fields + foreign_fields
    raw_id_fields = foreign_fields
    search_fields = (
        "address",
        "district",
        "community",
        "city",
        "county",
        "zipcode",
        "state_code",
    )


@admin.register(MLS)
class MLSAdmin(admin.ModelAdmin):
    list_display = ["name", "table_name", "source", "data_available_until"]
    readonly_fields = [
        "source",
        "data_available_until",
        "agents",
        "offices",
        "transactions",
    ]

    @admin.display(description="Agents", ordering="agent_count")
    def agents(self, mls):
        return mls.agents.count()

    @admin.display(description="Offices", ordering="office_count")
    def offices(self, mls):
        return mls.offices.count()

    @admin.display(description="Transactions", ordering="transaction_count")
    def transactions(self, mls):
        return mls.transactions.count()
