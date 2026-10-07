from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ssot", "0028_raw_data_ssot_models"),
    ]

    operations = [
        migrations.AlterField(
            model_name="mls",
            name="source",
            field=models.CharField(
                choices=[
                    ("reality", "Reality"),
                    ("constellation", "Constellation1"),
                    ("nureality", "Reality API"),
                    ("trestle", "Trestle"),
                    ("mlsgrid", "MLSGrid"),
                    ("restats", "Restats"),
                ],
                db_index=True,
                default="constellation",
                max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name="agent",
            name="source",
            field=models.CharField(
                choices=[
                    ("reality", "Reality"),
                    ("constellation", "Constellation1"),
                    ("nureality", "Reality API"),
                    ("trestle", "Trestle"),
                    ("mlsgrid", "MLSGrid"),
                    ("restats", "Restats"),
                ],
                db_index=True,
                default="constellation",
                max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name="office",
            name="source",
            field=models.CharField(
                choices=[
                    ("reality", "Reality"),
                    ("constellation", "Constellation1"),
                    ("nureality", "Reality API"),
                    ("trestle", "Trestle"),
                    ("mlsgrid", "MLSGrid"),
                    ("restats", "Restats"),
                ],
                db_index=True,
                default="constellation",
                max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name="transaction",
            name="source",
            field=models.CharField(
                choices=[
                    ("reality", "Reality"),
                    ("constellation", "Constellation1"),
                    ("nureality", "Reality API"),
                    ("trestle", "Trestle"),
                    ("mlsgrid", "MLSGrid"),
                    ("restats", "Restats"),
                ],
                db_index=True,
                default="constellation",
                max_length=32,
            ),
        ),
    ]
