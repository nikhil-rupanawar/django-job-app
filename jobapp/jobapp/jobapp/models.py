import logging
import time
import enum
import django.contrib.postgres.fields as postgres_fields
from django.db import transaction

from django.utils import timezone
from datetime import datetime, timedelta
from django.db import models
from typing import Type
from django.contrib.auth.models import AbstractUser
from django.contrib.auth.models import Group


logger = logging.getLogger(__name__)


def now():
    return timezone.now()


class JobStatus(enum.IntEnum):
    PENDING = 1
    REQUEST_ACK = 2
    RUNNING = 3
    FAILED = 5
    ERRORED = 6
    SUCCESS = 7
    SUCCESS_WITH_WARNING = 8


class UiStatus(models.TextChoices):
    PENDING = 'Pending'
    REQUEST_ACK = 'Request acknowledged'
    RUNNING = 'Running'
    FAILED = 'Failed'
    ERRORED = 'Errored'
    SUCCESS = 'Success'
    SUCCESS_WITH_WARNING = 'Success with warning'


class Severity(enum.IntEnum):
    INFO = 1
    WARNING = 2
    MINOR = 3
    MAJOR = 4
    CRITICAL = 5
    FATAL = 6


ALL_STATUSES = tuple(JobStatus)
FINAL_STATUSES = (JobStatus.FAILED, JobStatus.ERRORED, JobStatus.SUCCESS, JobStatus.SUCCESS_WITH_WARNING)
SUCCESS_STATUSES = (JobStatus.SUCCESS, JobStatus.SUCCESS_WITH_WARNING)
FAILED_STATUSES = (JobStatus.FAILED, JobStatus.ERRORED)


class Diagnostic(models.Model):
    
    class Meta:
        abstract = True

    severity = models.IntegerField(default=Severity.INFO.value)
    message = models.CharField(null=True, max_length=255)
    details = postgres_fields.JSONField(null=True)


class Job(models.Model):

    DEFAULT_TTL_THRESHOLD = (3 * 24 * 60 * 60)

    class Meta:
        abstract = True

    _status = models.IntegerField(null=True)
    _ui_status = models.CharField(choices=UiStatus.choices, max_length=255)
    type = models.IntegerField(null=True)
    created_by = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    data = postgres_fields.JSONField(null=True)
    _percentage_progress = models.IntegerField(null=True)
    created_at =  models.DateTimeField(auto_now_add=True)
    updated_at =  models.DateTimeField(auto_now_add=True)
    ttl = models.IntegerField(default=DEFAULT_TTL_THRESHOLD) # 3 days

    @property
    def status(self):
        return self._status

    def ui_status(self):
        return self._ui_status

    @property
    def percentage_progress(self):
        return self._percentage_progress

    @percentage_progress.setter
    def percentage_progress(self, value):
        self._percentage_progress = value

    def update_status(self, status: JobStatus=None, ui_status: UiStatus=None):
        assert status or ui_stauts
        self._status = status.value
        if ui_status is not None:
            self._ui_status = ui_status.value
        else:
            mapped_status = getattr(UiStatus, status.name, None)
            if mapped_status is not None:
                self._ui_status = mapped_status.value
        self.ping()

    def ping(self):
        self.updated_at = now()

    def publish_state(self):
        # By default save to the db
        self.save()

    def on_success(self):
        pass

    def on_failure(self):
        pass

    def finalize(self):
        pass

    def act(self):
        pass

    def resume(self):
        pass

    @property
    def has_expired(self):
        return not (now() <  self.created_at + timedelta(seconds=self.ttl))

    @property
    def is_stale(self):
        return self.has_expired

    def publish_job_request(self):
        pass


class DefaultJobRunner:

    @classmethod
    def run(cls, job: Type[Job]):
        job.update_status(status=JobStatus.REQUEST_ACK)
        job.publish_state()
        try:
            job.act()
        except Exception as e:
            logger.exception(e)
            job.update_status(status=JobStatus.ERRORED)
            job.publish_state()
        finally:
            cls.process_post_job_hooks(job)

    @classmethod
    def process_post_job_hooks(cls, job: Type[Job]):
        try:
            if job.status in SUCCESS_STATUSES:
                job.on_success()
            if job.status in FAILED_STATUSES:
                job.on_failure()
        except Exception as e:
            logger.exception(e)
            self.finalize()


############### DEMO ###########

class User(AbstractUser):

    @property
    def direct_roles(self):
        return [g.name for g in self.groups.all()]

    @property
    def groupset_roles(self):
        return [
            g.name for gs in self.groupset_set.all()
            for g in gs.groups.all()
        ]

    @property
    def effective_roles(self):
        return set(self.direct_roles + self.groupset_roles)


class Groupset(models.Model):
    name = models.CharField(max_length=255)
    users = models.ManyToManyField(User)
    groups = models.ManyToManyField(Group)


MOCK_DATA = {
    'to_add_users': [1, 2, 3, 5, 6, 8],
    'to_remove_users': [9, 4]
}

class GroupsetIdpSyncJob(Job):
    groupset = models.ForeignKey(Groupset, on_delete=models.CASCADE) # demo only null true

    @classmethod
    def sync_with_okta(cls, user):
        time.sleep(1)
        print(f'synced effective roles {user.effective_roles}'}

    def add_user(self, user):
        # Manage trasaction: TODO
        with transaction.atomic():
            try:
                self.groupset.users.add(user)
                self.groupset.save()
                self.sync_with_okta(user)
            except Exception as e:
                logger.exception(e)
                print(e)

    def act(self):
        self.update_status(status=JobStatus.RUNNING)
        self.publish_state()
        to_add_users = User.objects.filter(id__in=MOCK_DATA['to_add_users'])
        TOTAL_USERS = len(to_add_users)
        PROCESSED_USERS = 0
        self.percentage_progress = 0
        self.publish_state()
        for user in to_add_users:
            PROCESSED_USERS += 1
            self.percentage_progress = int((PROCESSED_USERS * 100) / TOTAL_USERS)
            self.publish_state()
            print(f'processed user {user.username}')
            self.groupsetidpsyncjobdiagnostic_set.create(
                userid=user_id,
                job=self,
                message=f'user processed successfully'
            )
        self.update_status(status=JobStatus.SUCCESS)
        self.percentage_progress = 100
        self.publish_state()
        print(f'job completed')

    def finalize(self):
        pass


class GroupsetIdpSyncJobDiagnostic(Diagnostic):
    job = models.ForeignKey(GroupsetIdpSyncJob,  on_delete=models.CASCADE)
    userid = models.IntegerField(null=True)
    operation = models.CharField(max_length=10, choices=['add', 'remove'])

class DeleteGroupsetJob(GroupsetIdpSyncJob):
    class Meta:
        proxy = True

    def act(self):
        super().act()

