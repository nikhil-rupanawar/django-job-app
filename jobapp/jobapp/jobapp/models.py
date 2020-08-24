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
    REQUEST_ACK = 'Acknowledged'
    RUNNING = 'Running'
    FAILED = 'Failed'
    ERRORED = 'Errored'
    SUCCESS = 'Success'
    SUCCESS_WITH_WARNING = 'Success with warning(s)'


class Severity(enum.IntEnum):
    INFO = 1
    WARNING = 2
    MINOR = 3
    MAJOR = 4
    CRITICAL = 5
    FATAL = 6


ALL_STATUSES = tuple(JobStatus)
FINAL_STATUSES = (
    JobStatus.FAILED,
    JobStatus.ERRORED,
    JobStatus.SUCCESS,
    JobStatus.SUCCESS_WITH_WARNING
)
GOOD_STATUSES = (JobStatus.SUCCESS, JobStatus.SUCCESS_WITH_WARNING)
BAD_STATUSES = (JobStatus.FAILED, JobStatus.ERRORED)


class JobFailedError(Exception):
    pass


class AbstractDiagnostic(models.Model):
    
    class Meta:
        abstract = True

    severity = models.IntegerField(default=Severity.INFO.value)
    details = postgres_fields.JSONField(null=True)


class AbstractJob(models.Model):

    DEFAULT_TTL_THRESHOLD = (3 * 24 * 60 * 60)

    class Meta:
        abstract = True

    _status = models.IntegerField(null=True)
    _ui_status = models.CharField(choices=UiStatus.choices, max_length=255)
    _data = postgres_fields.JSONField(null=True)
    type = models.IntegerField(null=True)
    created_by = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    created_at =  models.DateTimeField(auto_now_add=True)
    updated_at =  models.DateTimeField(auto_now_add=True)
    ttl = models.IntegerField(default=DEFAULT_TTL_THRESHOLD) # 3 days

    @property
    def status(self):
        return self._status

    def ui_status(self):
        return self._ui_status

    def acknowledge(self, save=True):
        self.update_status(status=JobStatus.REQUEST_ACK, save=save)

    def running(self, save=True):
        self.update_status(status=JobStatus.RUNNING, save=save)

    def fail(self, save=True, raise_error=True, reason=''):
        self.update_status(status=JobStatus.FAILED, save=save)
        if raise_error:
            raise JobFailedError(f'Job failed, reason={reason}')

    def success(self, save=True):
        self.update_status(status=JobStatus.SUCCESS, save=save)

    def error(self, save=True):
        self.update_status(status=JobStatus.ERRORED, save=save)

    def success_with_warning(self, save=True):
        self.update_status(status=JobStatus.SUCCESS_WITH_WARNING, save=save)

    def update_status(self, status: JobStatus, ui_status: UiStatus=None, save=True):
        assert status or ui_stauts
        self._status = status.value
        if ui_status is not None:
            self._ui_status = ui_status.value
        else:
            mapped_status = getattr(UiStatus, status.name, None)
            if mapped_status is not None:
                self.update_ui_status(mapped_status, save=False)
        self.ping()
        if save:
            sellf.save()

    def update_ui_status(self, status: UiStatus, save=True):
        self._ui_status = ui_status
        if save:
            self.save()

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

    def act_resume(self):
        pass

    @property
    def has_expired(self):
        return not (now() <  self.created_at + timedelta(seconds=self.ttl))

    @property
    def is_stale(self):
        return self.has_expired

    def publish_job_request(self):
        pass


class JobProgressMixin(models.Model):
    class Meta:
        abstract = True
    total_units = models.IntegerField(db_column='progress_total_units')
    done_units = models.IntegerField(default=0, db_column='progress_done_units')
    progress_unit = models.CharField(max_length=50)
    progress_unit_plural = models.CharField(max_length=50)
    _progress_percent = models.IntegerField(null=True)

    def report_progress(self, units:int=1, save=True):
        self.done_units += units
        self.remaining_units
        if save:
            self.save()

    @property
    def remaaining_units(self):
        return self.total_units - self.done_units

    @property
    def progress_percent(self):
        if self._progress_percent is not None:
            return _progress_percent
        return ((self.done_units * 100) / self.total_units)
 
    @progress_percent.setter
    def progress_percent(self, value: int):
        assert 0 <= value <= 100
        self._progress_percent = value


class JobRunnerMixin:
    @classmethod
    def run(cls, job: Type[AbstractJob]):
        job.acknowledge()
        try:
            job.running()
            job.act()
        except JobFailedError as e:
            pass
        except Exception as e:
            logger.exception(e)
            job.error()
        else:
            if job.status not in FINAL_STATUSES:
                job.success()
        finally:
            cls.process_post_job_hooks(job)

    @classmethod
    def process_post_job_hooks(cls, job: Type[AbstractJob]):
        try:
            if job.status in SUCCESS_STATUSES:
                job.on_success()
            if job.status in FAILED_STATUSES:
                job.on_failure()
        except Exception as e:
            logger.exception(e)
        finally:
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


class GroupsetSyncJob(AbstractJob, JobProgressMixin, JobRunnerMixin):
    groupset = models.ForeignKey(Groupset, on_delete=models.CASCADE)

    @classmethod
    def _add_user_to_groupset(self, user, groupset):
        with transaction.atomic():
            try:
                groupset.users.add(user)
                groupset.save()
                user.save()
                user.sync_with_okta()
            except Exception as e:
                logger.exception(e)
                print(e)
   
    @property
    def data(self):
        # TODO real data
        return {
           'groupset_id': 1,
           'to_add_users': [1, 2, 3, 5, 6, 8],
           'to_remove_users': [9, 4]
        }

    def act(self):
        groupset = Groupset.objects.get(id=self.data['groupset_id'])
        to_add_users = User.objects.filter(id__in=self.data['to_add_users'])
        for user in to_add_users:
            self._add_user_to_groupset(groupset, user)
            print(f'Processed user {user.username}')
            self.report_progress(units=1)
            # handle exception and add dignostics
            self.add_user_diagnostic(
                user=user,
                groupset=groupset,
                job=self,
                message=f'User {user.username} added successfully.',
                serverity=Severity.INFO
            )
        # TODO: remove user from groupset
        print(f'job completed')

    def add_user_diagnostic(
        self,
        user,
        groupset,
        messagge=None,
        operation=None,
        severity=Severity.INFO
    ):
        details = {
           'groupset_id': groupset.id,
           'user_id': user.id,
           'operation': operation,
           'message': message,
        }
        self.groupsetsyncjobdiagnostic_set.create(
            job_id=self.id,
            details=details,
            severity=severity
        )


class JobDiagnostic(AbstractDiagnostic):
    job_id = models.IntegerField() 


class DeleteGroupsetJob(GroupsetSyncJob):
    class Meta:
        proxy = True
