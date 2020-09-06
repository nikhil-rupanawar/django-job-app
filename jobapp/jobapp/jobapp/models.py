import logging
import abc
import time
import enum
import functools
import django.contrib.postgres.fields as postgres_fields
from django.db.models.query import QuerySet
from django.db import transaction
from django.utils import timezone
from datetime import datetime, timedelta
from django.db import models
from typing import Type
from django.contrib.auth.models import AbstractUser
from django.contrib.auth.models import Group
from contextlib import contextmanager

logger = logging.getLogger(__name__)

def now():
    return timezone.now()


class JobStatus(models.IntegerChoices):
    PENDING = 1
    REQUEST_ACK = 2
    RUNNING = 3
    FAILED = 5
    ERRORED = 6
    SUCCESS = 7
    SUCCESS_WITH_WARNING = 8
    CANCEL_REQUESTED = 9
    CANCELED = 10
    PAUSED = 11


class UiStatus(models.TextChoices):
    PENDING = 'Pending'
    REQUEST_ACK = 'Acknowledged'
    RUNNING = 'Running'
    FAILED = 'Failed'
    ERRORED = 'Errored'
    SUCCESS = 'Success'
    SUCCESS_WITH_WARNING = 'Success with warning(s)'
    CANCEL_REQUESTED = 'Cancel requested'
    CANCELED = 'Canceled'


class Severity(models.IntegerChoices):
    INFO = 1
    WARNING = 2
    CRITICAL = 3


ALL_STATUSES = tuple(JobStatus)
FINAL_STATUSES = (
    JobStatus.FAILED,
    JobStatus.ERRORED,
    JobStatus.SUCCESS,
    JobStatus.SUCCESS_WITH_WARNING,
    JobStatus.CANCELED
)
UNDETERMINISTIC_STATUSES = (JobStatus.RUNNING, JobStatus.PAUSED, JobStatus.CANCEL_REQUESTED, JobStatus.REQUEST_ACK)
SUCCESS_STATUSES = (JobStatus.SUCCESS, JobStatus.SUCCESS_WITH_WARNING)
FAILED_STATUSES = (JobStatus.FAILED, JobStatus.ERRORED)


class JobFailedError(Exception):
    pass


class JobStageFailedError(Exception):
    pass


class JobStepFailedError(Exception):
    pass


class JobStateError(Exception):
    pass


class JobCanceledError(JobStateError):
    pass


class AbstractJobNotifier(abc.ABCMeta):
    @abc.abstractmethod
    def notify(self, job):
        ...


class DbSaveNotifier(AbstractJobNotifier):
    """ Simply save the job state to db """
    def notify(self, job):
        job.save()


# This could be done through model signals however,
# If there are cases when job wants to
# 1. Not to update to db but notify
# 2. save() to db but do not notify
# 3. Controll when to notify and when not to. (by overriding method and removing decorator)
#  Hence keeping save and notify separate things.
def notify_update(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        try:
           return f(self, *args, **kwargs)
        finally:
           self.notify()
    return wrapper


class AbstractJob(models.Model):

    DEFAULT_TTL_THRESHOLD = (3 * 24 * 60 * 60) # 3 days

    class Meta:
        abstract = True

    _status = models.IntegerField(null=True)
    _ui_status = models.CharField(choices=UiStatus.choices, max_length=255)
    _data = models.JSONField(null=True)
    type = models.IntegerField(null=True)
    created_by = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    created_at =  models.DateTimeField(auto_now_add=True)
    updated_at =  models.DateTimeField(auto_now_add=True)
    ttl = models.IntegerField(default=DEFAULT_TTL_THRESHOLD)

    def __init__(self, *args, notifiers=None, **kwargs):
        super().__init__(*args, **kwargs)
        if not notifiers:
            self._notifiers = (DbSaveNotifier(),)

    @property
    def status(self):
        return self._status

    @property
    def ui_status(self):
        return self._ui_status

    @notify_update
    def acknowledge(self):
        self.update_status(status=JobStatus.REQUEST_ACK)

    @notify_update
    def running(self):
        self.update_status(status=JobStatus.RUNNING)

    @notify_update
    def fail(self, raise_error=True, reason=''):
        self.update_status(status=JobStatus.FAILED)
        if raise_error:
            raise JobFailedError(f'Job failed, reason={reason}')

    @notify_update
    def success(self, notify):
        self.update_status(status=JobStatus.SUCCESS)

    @notify_update
    def error(self):
        self.update_status(status=JobStatus.ERRORED)

    @notify_update
    def cancel(self, raise_error=True, reason=''):
        self.update_status(status=JobStatus.FAILED)
        if raise_error:
            raise JobCanceledError(f'Job canceled')

    @notify_update
    def success_with_warning(self):
        self.update_status(status=JobStatus.SUCCESS_WITH_WARNING)

    @notify_update
    def request_cancel(self):
        self.update_status(JobStatus.REQUEST_CANCEL)

    def update_status(self, status: JobStatus, ui_status: UiStatus=None):
        assert status or ui_status
        self._status = status.value
        if ui_status is not None:
            self._ui_status = ui_status.value
        else:
            mapped_status = getattr(UiStatus, status.name, None)
            if mapped_status is not None:
                self.update_ui_status(mapped_status)
        self.touch()

    def update_ui_status(self, status: UiStatus):
        self._ui_status = status
        self.touch()

    def touch(self):
        self.updated_at = now()

    @property
    def notifiers(self):
        return self._notifiers

    def notify(self):
        # notifiers are classes which implemets 'notify' method.
        # and accepts job as first argument
        for notifier in self.notifiers:
            notifier.notify(self)

    def act(self):
        raise NotImplementedError()

    def act_resume(self):
        raise NotImplementedError()

    @property
    def has_expired(self):
        return not (now() <  self.created_at + timedelta(seconds=self.ttl))

    @property
    def is_stale(self):
        return self.has_expired

    @property
    def is_running(self):
        return self.status == JobStatus.RUNNING

    @property
    def is_failed(self):
        return self.status == JobStatus.FAILED

    @property
    def is_cancel_requested(self, refresh=True):
        if refresh:
            self.refresh()
        return self.status == JobStatus.CANCEL_REQUESTED

    def refresh(self):
        self.refresh_from_db()

    def delay(self):
        raise NotImplementedError()

    def to_dict(self):
        return type(self).objects.filter(pk=self.pk).values().first()
    
    def to_message(self):
        return self.to_message()

    def on_success(self):
        pass

    def on_failure(self):
        pass

    def finalize(self):
        pass

    def run(self):
        self.acknowledge()
        try:
            if self.is_cancel_requested():
                self.cancel()
            else:
                self.running()
                self.act()
        except (JobFailedError, JobStageFailedError, JobStepFailedError) as e:
            if self.status != JobStatus.FAILED:
                self.fail(raise_error=False, reason=e.args[0])
        except JobCanceledError as e:
            if self.status != JobStatus.FAILED:
                self.cancel(raise_error=False, reason=e.args[0])
        except JobStateError as e:
            logger.exception(e)
        except Exception as e:
            logger.exception(e)
            self.error()
        else:
            if self.status not in FINAL_STATUSES:
                self.success()
        finally:
            try:
                self._process_post_job_hooks()
            except Exception as e:
                logger.exception(e)
                self.success_with_warning()

    def _process_post_job_hooks(self):
        try:
            if self.status in SUCCESS_STATUSES:
                self.on_success()
            if self.status in FAILED_STATUSES:
                self.on_failure()
        finally:
            self.finalize()


class AbstractJobProgressMixin(models.Model):
    class Meta:
        abstract = True
    _progress_total_units = models.IntegerField(default=0)
    _progress_done_units = models.IntegerField(default=0)
    _progress_percent = models.IntegerField(null=True)

    @property
    def total_units(self):
        return self._progress_total_units

    @property
    def done_units(self):
        return self._progress_done_units

    def add_total_units(self, units):
        self._progress_total_units += units

    @notify_update
    def add_done_units(self, units):
        self._progress_done_units += units

    @property
    def remaining_units(self):
        return self.total_units - self.done_units

    @property
    def progress_percent(self):
        if self._progress_percent is not None:
            return self._progress_percent
        if self.total_units == 0:
            return 0
        return (self.done_units * 100) / self.total_units
 
    @progress_percent.setter
    def progress_percent(self, value: int):
        assert 0 <= value <= 100
        self._progress_percent = value


class StepStageJobMixin(abc.ABCMeta):

    @abstractmethod
    def step_success(self, step, **step_data):
        ...

    @abstractmethod
    def step_fail(
        self,
        step,
        severity=Severity.CRITICAL,
        raise_error=True,
        **step_data
    ):
        if raise_error:
            raise JobStepFailedError(step)

    @contextmanager
    def step_context(self, step, **stage_data):
        self.step_start(stage)
        try:
            yield
        finally:
            self.step_end(stage)

    StepContext = step_context

    @abstractmethod
    def stage_start(self, stage, message='Step stared.'):
        ...

    @abstractmethod
    def step_end(self, stage, message='Step completed.'):
        ...

    @abstractmethod
    def stage_success(self, stage, **stage_data):
        ...

    @abstractmethod
    def stage_fail(
        self,
        stage,
        severity=Severity.CRITICAL,
        raise_error=True,
        **stage_data
    ):
        ...

    @abstractmethod
    def stage_start(self, stage, message='Stage stared.'):
        return self._rel_diagnostics.create(
            stage=stage,
            details=dict(message=message),
        )

    @abstractmethod
    def stage_end(self, stage, message='Stage completed.'):
        return self._rel_diagnostics.create(
            stage=stage,
            details=dict(message=message),
        )

    @contextmanager
    def stage_context(self, stage, **stage_data):
        self.stage_start(stage)
        try:
            yield
        finally:
            self.stage_end(stage)

    StageContext = stage_context


class AbstractStepStageProgressJob(
    AbstractJob,
    AbstractJobProgressMixin,
    StepStageJobMixin
):
    class Meta:
        abstract = True


class AbstractDiagnostic(models.Model):
    class Meta:
        abstract = True
    severity = models.IntegerField(default=Severity.INFO)
    created_at =  models.DateTimeField(auto_now_add=True)
    details = models.JSONField(null=True)
    stage = models.CharField(null=True, blank=True, max_length=50)
    step = models.CharField(null=True, blank=True, max_length=50)


