import django.utils.timezone
import model_utils.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="AirtableWebhook",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "created",
                    model_utils.fields.AutoCreatedField(
                        default=django.utils.timezone.now,
                        editable=False,
                        verbose_name="created",
                    ),
                ),
                (
                    "modified",
                    model_utils.fields.AutoLastModifiedField(
                        default=django.utils.timezone.now,
                        editable=False,
                        verbose_name="modified",
                    ),
                ),
                ("airtable_id", models.CharField(max_length=32, unique=True)),
                ("base_id", models.CharField(max_length=32)),
                ("mac_secret", models.CharField(max_length=256)),
                (
                    "last_transaction_number",
                    models.PositiveIntegerField(blank=True, null=True),
                ),
            ],
            options={
                "abstract": False,
            },
        ),
    ]
