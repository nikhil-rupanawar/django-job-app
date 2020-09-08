from django.db import models


class Severity(models.IntegerChoices):
    INFO = 1
    WARNING = 2
    CRITICAL = 3


class AbstractDiagnostic(models.Model):
    class Meta:
        abstract = True
    severity = models.IntegerField(default=Severity.INFO)
    created_at = models.DateTimeField(auto_now_add=True)
    message = models.CharField(null=True, blank=True, max_length=255)
    details = models.JSONField(null=True)


class AbstractStepDiagnostic(AbstractDiagnostic):
    class Meta:
        abstract = True
    stage = models.CharField(null=True, blank=True, max_length=50)
    step = models.CharField(null=True, blank=True, max_length=50)