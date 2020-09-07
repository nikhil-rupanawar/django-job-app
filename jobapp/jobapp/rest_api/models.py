import logging
import abc
import time
import enum
from django.db import transaction
from django.db import models
from jobapp.jobapp.models import (
    AbstractStepStageProgressJob,
    AbstractStepStageDiagnostic,
    AbstractDiagnostic,
    Severity
)
from django.contrib.auth.models import AbstractUser
from django.contrib.auth.models import Group
from django.db.models import Count

logger = logging.getLogger(__name__)

############### DEMO ###########

class User(AbstractUser):

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
class Job(AbstractStepStageProgressJob):
    pass


# Diagnostics for all jobs
class JobDiagnostic(AbstractStepStageDiagnostic):
    job = models.ForeignKey(
        Job,
        on_delete=models.CASCADE,
        related_name='diagnostics'
    )


# MTI (model table inheritance)
class GroupsetJob(Job):
    """ Base class model for groupset user sync job """

    class JobType(models.IntegerChoices):
        CREATE = 1
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

    def step_success(self, step, **data):    
        return self.diagnostics.create(
            job=self,
            step=step,
            message='succceded',
            details=data,
        )

    def step_fail(self, step, **data):
        return self.diagnostics.create(
            job=self,
            step=step,
            severity=Severity.CRITICAL,
            message='failed',
            details=data,
        )

    def step_end(self, step, progress_done_units=1, **data):
        self.add_progress_done_units(progress_done_units)
        print(f'Progress: {int(self.percent_progress)}%')

    def stage_success(self, stage, **data):
        return self.diagnostics.create(
            job=self,
            stage=stage,
            message='succeeded',
            details=data
        )

    def stage_fail(self, stage, **data):
        self.diagnostics.create(
            job=self,
            stage=stage,
            severity=Severity.CRITICAL,
            details=data,
        )
        super().stage_fail(stage, **data)

    def stage_start(self, stage, **data):
        return self.diagnostics.create(
            job=self,
            stage=stage,
            message='stared',
        )

    def stage_end(self, stage, **data):
        return self.diagnostics.create(
            job=self,
            stage=stage,
            message='completed.'
        )

    def _job_status_from_diagnostics(self):
        if self.diagnostics.objects.filter(severity=Severity.CRITICAL).exists():
            return JobStatus.FAILED
        return JobStatus.SUCCESS

    def add_user(self, user):
        with self.StepContext(
            self.Step.ADD_USER,
            data=dict(username=user.username)
        ):
            with transaction.atomic():
                self.groupset.users.remove(user)
                self.groupset.save()

    def remove_user(self, user):
       with self.StepContext(
            self.Step.REMOVE_USER,
            data=dict(username=user.username)
        ):
            with transaction.atomic():
                self.groupset.users.remove(user)
                self.groupset.save()

    def update_users(self, add_users, remove_users):
        with self.StageContext(self.Stage.USERS_UPADTE):
            for user in add_users:
                self.add_user(user)
                time.sleep(1)
            for user in remove_users:
                self.remove_user(user)
                time.sleep(1)

    def update_groups(self, add_groups, remove_groups):
        with self.StageContext(self.Stage.GROUPS_UPDATE):
            with self.StepContext(self.Step.UPDATE_GROUPS):
                with transaction.atomic():
                    self.groupset.groups.add(*add_groups)
                    self.groupset.groups.remove(*remove_groups)
                    self.groupset.save()

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
        self.add_progress_total_units(
            add_users.count()
            + remove_users.count()
            + 1 # Groups update runs always so +1
        )
        self.update_groups(add_groups, remove_groups)
        self.update_users(add_users, remove_users)


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





# if __name__ == "__main__":


#     ###### Demo create groupset job #####
#     # Create group
#     # TODO: transaction handling
#     groupset = Groupset(name='test')
#     groupset.save()
#     # Create a job
#     job = CreateGroupsetJob(
#         groupset=groupset,
#         data=dict(
#             add_user_ids=[1, 2, 3],
#             add_group_ids=['*']
#         )
#     )
#     job.save()
#     # Send job to your prefered async queue 
#     job.delay()

#     # On daemon side >>>
#     # You will get it from queue but for demo just convert object to message
#     message = job.to_message()
#     job = GroupsetJob.from_message(message)
#     # just run it! i(t blocks).
#     job.run()
    #### End demo create groupset job #####