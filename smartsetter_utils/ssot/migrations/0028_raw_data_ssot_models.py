from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("ssot", "0027_agent_rawmls_fields"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="office",
            name="raw_data",
        ),
        migrations.RemoveField(
            model_name="agent",
            name="raw_data",
        ),
        migrations.RemoveField(
            model_name="transaction",
            name="raw_data",
        ),
    ]