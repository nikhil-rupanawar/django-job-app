import logging
import abc
import time
import enum
from django.db import transaction
from django.db import models
from jobapp.jobapp.exceptions import (
    JobStepFailedError,
    JobStageFailedError,
    JobFailedError
)
from jobapp.jobapp.models import (
    AbstractStepProgressJob,
    AbstractStepDiagnostic,
    Severity,
    JobStatus,
)
from django.db.models import Max
from django.contrib.auth.models import AbstractUser
from django.contrib.auth.models import Group
from django.db.models import Count
from polymorphic.models import PolymorphicModel, PolymorphicManager


logger = logging.getLogger(__name__)

############### DEMO ###########

class User(AbstractUser):

    def sync_with_idp(self):
        time.sleep(0.5)
        print(f"{self} - Successfully synced with okta.")

    @property
    def direct_group_names(self):
        return { g.name for g in self.groups.all() }

    @property
    def groupset_groups(self):
        for gs in self.groupsets.prefetch_related('groups'):
            for g in gs.groups.all():
                yield g

    @property
    def groupsets_groups_names(self):
        return { g.name for g in self.groupset_groups }

    @property
    def effective_groups(self):
        return list(self.groups.all()) +  list(self.groupset_groups)

    @property
    def effective_group_names(self):
        return { g.name for g in self.effective_groups }


class Groupset(models.Model):
    name = models.CharField(max_length=255)
    users = models.ManyToManyField(User, related_name='groupsets')
    groups = models.ManyToManyField(Group, related_name='groupsets')

    @property
    def last_job(self):
        return self.groupset_jobs.order_by('-updated_at').first()



# Base model for all job model/table
class Job(PolymorphicModel, AbstractStepProgressJob):
    pass

 
# Diagnostics for all jobs
class JobDiagnostic(AbstractStepDiagnostic):
    job = models.ForeignKey(
        Job,
        on_delete=models.CASCADE,
        related_name='diagnostics'
    )


# MTI (model table inheritance)
class GroupsetJob(Job):
    """ Base class model for groupset user sync job """

    class JobType(models.IntegerChoices):
        UPDATE = 1
        DELETE = 2

    class Step(models.TextChoices):
        ADD_USER = 'ADD_USER'
        REMOVE_USER = 'REMOVE_USER'
        UPDATE_GROUPS = 'UPDATE_GROUPS'
        DELETE_GROUPSET = 'DELETE_GROUPSET'

    class Stage(models.TextChoices):
        USERS_UPADTE = 'USERS_UPDATE'
        GROUPS_UPDATE = 'GROUPS_UPDATE'
        GROUPSET_DELETE = 'DELETE_GROUPSET'

    groupset = models.ForeignKey(
        Groupset,
        on_delete=models.SET_NULL,
        null=True,
        related_name='groupset_jobs'
    )

    def on_step_success(self):    
        return self.diagnostics.create(
            job=self,
            step=self.current_step,
            stage=self.current_stage,
            message='succceded',
            details=self.current_step_data,
        )

    def on_step_fail(self):
        return self.diagnostics.create(
            job=self,
            step=self.current_step,
            stage=self.current_stage,
            severity=Severity.CRITICAL,
            message='failed',
            details=self.current_step_data,
        )

    def on_step_end(self):
        # Increament progress unit by 1, as it we has processed 1 user.
        self.add_progress_done_units(1)
        print(f'Progress: {int(self.percent_progress)}%')

    def on_stage_success(self):
        return self.diagnostics.create(
            job=self,
            stage=self.current_stage,
            message='succeeded',
        )

    def on_stage_fail(self):
        self.diagnostics.create(
            job=self,
            stage=self.current_stage,
            step=self.current_step,
            severity=Severity.CRITICAL,
            details=self.current_stage_data
        )

    def on_stage_start(self):
        return self.diagnostics.create(
            job=self,
            stage=self.current_stage,
            message='started',
        )

    def _job_status_from_diagnostics(self):
        if self.diagnostics.filter(severity=Severity.CRITICAL).exists():
            return JobStatus.FAILED
        return JobStatus.SUCCESS

    def _stage_severity_from_diagnostics(self):
        max_severity = self.diagnostics.aggregate(Max('severity'))['severity__max']
        if max_severity:
            return Severity(max_severity)
        return Severity.INFO

    def add_user(self, user):
        with transaction.atomic():
            self.groupset.users.remove(user)
            self.groupset.save()
            user.sync_with_idp()

    def remove_user(self, user):
        with transaction.atomic():
            self.groupset.users.remove(user)
            self.groupset.save()
            user.sync_with_idp()

    def update_users(self, add_users, remove_users):
        for user in remove_users:
            try:
                with self.StepContext(
                    self.Step.REMOVE_USER,
                    data=dict(username=user.username, user_id=user.id)
                ):
                    self.remove_user(user)
            except JobStepFailedError as e:
                pass

        for user in add_users:
            try:
                with self.StepContext(
                    self.Step.ADD_USER,
                    data=dict(username=user.username, user_id=user.id)
                ):
                    self.add_user(user)
            except JobStepFailedError as e:
                pass

    def add_remove_groups(self, add_groups, remove_groups):
        with transaction.atomic():
            self.groupset.groups.add(*add_groups)
            self.groupset.groups.remove(*remove_groups)
            self.groupset.save()

    def update_groups(self, add_groups, remove_groups):
        # Only single step i.e. update given groups
        with self.StepContext(self.Step.UPDATE_GROUPS):
            self.add_remove_groups(add_groups, remove_groups)

    def act(self):
        add_users = list(
            User.objects.all()
            if '*' in self.data.get('add_user_ids', [])
            else User.objects.filter(id__in=self.data.get('add_user_ids', []))
        )
        remove_users = list(
            self.groupset.users
            if '*' in  self.data.get('remove_user_ids', [])
            else self.groupset.users.filter(id__in=self.data.get('remove_user_ids', []))
        )
        add_groups = list(
            Group.objects.all()
            if '*' in self.data.get('add_group_ids', [])
            else Group.objects.filter(id__in=self.data.get('add_group_ids', []))
        )
        remove_groups = list(
            self.groupset.groups
            if '*' in self.data.get('remove_group_ids', [])
            else self.groupset.groups.filter(id__in=self.data.get('remove_group_ids', []))
        )

        # Calculate and update total units for progress caculations.
        total_units = n_users = len(add_users) + len(remove_users)
        n_groups = len(add_groups) + len(remove_groups)
        if n_groups:
            total_units += 1 # Groups update is unit operation
        self.add_progress_total_units(total_units)

        if n_groups:
            # Groups update stage
            with self.StageContext(self.Stage.GROUPS_UPDATE):
                self.update_groups(add_groups, remove_groups)
                # Should not proceed if this stage fails
                if self._stage_severity_from_diagnostics() == Severity.CRITICAL:
                    self.fail_stage(f'Failed to update roles')

        if n_users:
            # Users update stage
            with self.StageContext(self.Stage.USERS_UPADTE):
                self.update_users(add_users, remove_users)
                # Fail the stage if any step was failed.
                if self._stage_severity_from_diagnostics() == Severity.CRITICAL:
                    self.fail_stage(f'Failed to update users')


class ManagerUpdateGroupset(PolymorphicManager):
    def get_queryset(self):
        return super().get_queryset().filter(
            type=GroupsetJob.JobType.UPDATE   
        )


class UpdateGroupsetJob(GroupsetJob):
    objects = ManagerUpdateGroupset()

    class Meta:
        proxy = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = GroupsetJob.JobType.UPDATE


class ManagerDeleteGroupsetJob(PolymorphicManager):
    def get_queryset(self):
        return super().get_queryset().filter(
            type=GroupsetJob.JobType.DELETE   
        )


class DeleteGroupsetJob(GroupsetJob):
    type = GroupsetJob.JobType.DELETE
    objects = ManagerDeleteGroupsetJob()

    class Meta:
        proxy = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = GroupsetJob.JobType.DELETE

    @property
    def data(self):
        return {
            'remove_group_ids': ['*'],
            'remove_user_ids': ['*']
        }

    def delete_groupset(self):
        with self.StepContext(self.Step.DELETE_GROUPSET):
            with transaction.atomic():
                self.groupset.delete()

    def act(self):
        # additional unit to delete groupset
        self.add_progress_total_units(1)
        super().act()
        with self.StageContext(self.Stage.DELETE_GROUPSET):
            self.delete_groupset()
            if self._stage_severity_from_diagnostics() == Severity.CRITICAL:
                self.fail_stage(f'Failed to delete group')