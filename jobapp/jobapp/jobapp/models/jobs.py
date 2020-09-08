import logging
import abc
import time
import functools

from django.utils import timezone
from datetime import datetime, timedelta
from django.db import models
from typing import Type

from .mixins import StepJobMixin, AbstractProgressJobMixin
from .diagnostics import AbstractDiagnostic, AbstractStepDiagnostic
from .notifiers import DbUpdateNotifier
from ..exceptions import (
    JobStateError,
    JobFailedError,
    JobStageFailedError,
    JobStepFailedError,
    JobCanceledError
)


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


ALL_STATUSES = tuple(JobStatus)
FINAL_STATUSES = (
    JobStatus.FAILED,
    JobStatus.ERRORED,
    JobStatus.SUCCESS,
    JobStatus.SUCCESS_WITH_WARNING,
    JobStatus.CANCELED
)
INTERMEDIATE_STATUSES = (JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED, JobStatus.REQUEST_ACK)
GOOD_STATUSES = (JobStatus.SUCCESS, JobStatus.SUCCESS_WITH_WARNING)
BAD_STATUSES = (JobStatus.FAILED, JobStatus.ERRORED)


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

    _status = models.IntegerField(null=True, default=JobStatus.PENDING)
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
            self._notifiers = (DbUpdateNotifier(),)

    @property
    def data(self):
        return self._data

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
    def success(self):
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
            if self.is_cancel_requested:
                self.cancel()
            else:
                self.running()
                self.act()
        except (JobFailedError, JobStageFailedError, JobStepFailedError) as e:
            if self.status != JobStatus.FAILED:
                self.fail(raise_error=False, reason=e.args[0])
        except JobCanceledError as e:
            if self.status != JobStatus.CANCELED:
                self.cancel(raise_error=False, reason=e.args[0])
        except (Exception, JobStateError) as e:
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
            if self.status in GOOD_STATUSES:
                self.on_success()
            if self.status in BAD_STATUSES:
                self.on_failure()
        finally:
            self.finalize()


class AbstractProgressJob(
    AbstractJob,
    AbstractProgressJobMixin
):
    class Meta:
        abstract = True


class AbstractStepJob(
    AbstractJob,
    StepJobMixin
):
    class Meta:
        abstract = True


class AbstractStepProgressJob(
    AbstractProgressJob,
    StepJobMixin
):
    class Meta:
        abstract = True
