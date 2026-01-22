from django.contrib.gis.db import models


class CommonFieldsQuerySet(models.QuerySet):
    def reality(self):
        from smartsetter_utils.ssot.models.base_models import CommonFields

        return self.filter_by_source(CommonFields.SOURCE_CHOICES.reality)

    def nureality(self):
        from smartsetter_utils.ssot.models.base_models import CommonFields

        return self.filter_by_source(CommonFields.SOURCE_CHOICES.nureality)

    def constellation(self):
        from smartsetter_utils.ssot.models.base_models import CommonFields

        return self.filter_by_source(CommonFields.SOURCE_CHOICES.constellation)

    def filter_by_source(self, source):
        return self.filter(source=source)


class CommonQuerySet(CommonFieldsQuerySet):
    def get_by_id_or_none(self, id):
        if id:
            try:
                return self.get(id=id)
            except Exception:
                return None

    def active(self):
        return self.filter(status="Active")
