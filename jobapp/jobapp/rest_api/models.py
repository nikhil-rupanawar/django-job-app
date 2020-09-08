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

from django.contrib.auth.models import AbstractUser
from django.contrib.auth.models import Group
from django.db.models import Count

logger = logging.getLogger(__name__)

############### DEMO ###########

class User(AbstractUser):

    def sync_with_idp(self):
        time.sleep(1)
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
        

# Base model for all job model/table
class Job(AbstractStepProgressJob):
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
        UPDATE = 2
        DELETE = 3

    class Step(models.TextChoices):
        ADD_USER = 'ADD_USER'
        REMOVE_USER = 'REMOVE_USER'
        UPDATE_GROUPS = 'UPDATE_GROUPS'
        DELETE_GROUPSET = 'DELETE_GROUPSET'

    class Stage(models.TextChoices):
        USERS_UPADTE = 'USERS_UPDATE'
        GROUPS_UPDATE = 'GROUPS_UPDATE'
        GROUPSET_DELETE = 'DELETE_GROUPSET'

    groupset = models.ForeignKey(Groupset, on_delete=models.SET_NULL, null=True)

    def on_step_success(self):    
        return self.diagnostics.create(
            job=self,
            step=self.current_step,
            stage=self.current_stage,
            message='succceded',
            details=self.current_step_data,
        )

    def on_step_fail(self):
        self.current_stage_data.update(self.current_step_data)
        self.diagnostics.create(
            job=self,
            step=self.current_step,
            stage=self.current_stage,
            severity=Severity.CRITICAL,
            message='failed',
            details=self.current_step_data,
        )
        super().on_step_fail()

    def on_step_end(self):
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
        super().on_stage_fail()

    def on_stage_start(self):
        return self.diagnostics.create(
            job=self,
            stage=self.current_stage,
            message='started',
        )

    def on_stage_end(self):
        return self.diagnostics.create(
            job=self,
            stage=self.current_stage,
            message='completed'
        )

    def _job_status_from_diagnostics(self):
        if self.diagnostics.filter(severity=Severity.CRITICAL).exists():
            return JobStatus.FAILED
        return JobStatus.SUCCESS

    def add_user(self, user):
        with transaction.atomic():
            self.groupset.users.remove(user)
            self.groupset.save()
            raise JobStepFailedError(f'{user} doest not exist.')
            user.sync_with_idp()

    def remove_user(self, user):
        with transaction.atomic():
            self.groupset.users.remove(user)
            self.groupset.save()
            user.sync_with_idp()

    def update_users(self, add_users, remove_users):
        for user in add_users:
            try:
                with self.StepContext(
                    self.Step.ADD_USER,
                    data=dict(username=user.username, user_id=user.id)
                ):
                    self.add_user(user)
            except JobStepFailedError as e:
                pass

        for user in remove_users:
            try:
                with self.StepContext(
                    self.Step.REMOVE_USER,
                    data=dict(username=user.username, user_id=user.id)
                ):
                    self.remove_user(user)
            except JobStepFailedError as e:
                pass

    def add_remove_groups(self, add_groups, remove_groups):
        with transaction.atomic():
            self.groupset.groups.add(*add_groups)
            self.groupset.groups.remove(*remove_groups)
            self.groupset.save()

    def update_groups(self, add_groups, remove_groups):
        with self.StepContext(self.Step.UPDATE_GROUPS):
            self.add_remove_groups(add_groups, remove_groups)

    def act(self):
        add_users = User.objects.all() if '*' in self.data.get('add_user_ids', []) else User.objects.filter(
            id__in=self.data.get('add_user_ids', [])
        )
        remove_users = self.groupset.users if '*' in  self.data.get('remove_user_ids', []) else self.groupset.users.filter(
            id__in=self.data.get('remove_user_ids', [])
        )
        add_groups = Group.objects.all() if '*' in self.data.get('add_group_ids', []) else Group.objects.filter(
            id__in=self.data.get('add_group_ids', [])
        )
        remove_groups = self.groupset.groups if '*' in self.data.get('remove_group_ids', []) else self.groupset.groups.filter(
            id__in=self.data.get('remove_group_ids', [])
        )

        n_users = add_users.count() + remove_users.count()
        n_groups = add_groups.count() + remove_groups.count()
        total_units = n_users
        if n_groups:
            total_units += 1
        self.add_progress_total_units(total_units)
     
        if n_groups:
            with self.StageContext(self.Stage.GROUPS_UPDATE):
                self.update_groups(add_groups, remove_groups)
        
        if n_users:
            with self.StageContext(self.Stage.USERS_UPADTE):
                self.update_users(add_users, remove_users)

        if self._job_status_from_diagnostics() == JobStatus.FAILED:
            self.fail('One or more steps failed.')


class ManagerUpdateGroupset(models.Manager):
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


class ManagerDeleteGroupsetJob(models.Manager):
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
        with self.StageContext(self.Stage.DELETE_GROUPSET):
            with self.StepContext(self.Step.DELETE_GROUPSET):
                with transaction.atomic():
                    self.groupset.delete()

    def act(self):
        # additional unit to delete groupset
        self.add_progress_total_units(1)
        super().act()
        self.delete_groupset()
