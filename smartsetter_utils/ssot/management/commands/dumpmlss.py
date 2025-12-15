import json

from django.core.management.base import BaseCommand
from rest_framework import serializers

from smartsetter_utils.ssot.models import MLS


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-i", "--indent", action="store_true")

    def handle(self, *args, **options):
        class MLSSerializer(serializers.ModelSerializer):
            class Meta:
                model = MLS
                fields = ("id", "table_name")

        self.stdout.write(
            json.dumps(
                MLSSerializer(MLS.objects.all(), many=True).data,
                indent=2 if options["indent"] else None,
            )
        )
