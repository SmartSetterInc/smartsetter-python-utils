# Generated by Django 4.2.11 on 2025-05-31 10:42

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ssot", "0005_agent_status_office_location_office_status_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="agent",
            name="address",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="agent",
            name="city",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="agent",
            name="email",
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AlterField(
            model_name="agent",
            name="name",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="agent",
            name="office_name",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="agent",
            name="state",
            field=models.CharField(blank=True, max_length=16, null=True),
        ),
        migrations.AlterField(
            model_name="agent",
            name="verified_phone",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        migrations.AlterField(
            model_name="agent",
            name="years_in_business",
            field=models.PositiveSmallIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="agent",
            name="zipcode",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        migrations.AlterField(
            model_name="office",
            name="address",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="office",
            name="city",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="office",
            name="name",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="office",
            name="office_id",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="office",
            name="state",
            field=models.CharField(blank=True, max_length=16, null=True),
        ),
        migrations.AlterField(
            model_name="office",
            name="zipcode",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="address",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="city",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="closed_date",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="community",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="county",
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="days_on_market",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="district",
            field=models.CharField(blank=True, max_length=128, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="list_price",
            field=models.PositiveBigIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="mls_number",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="sold_price",
            field=models.PositiveBigIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="state_code",
            field=models.CharField(blank=True, max_length=16, null=True),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="zipcode",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
    ]
