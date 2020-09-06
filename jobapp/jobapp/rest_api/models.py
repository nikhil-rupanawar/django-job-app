import logging
import abc
import time
import enum
from django.db import transaction
from django.db import models
from jobapp.jobapp.models import (
    AbstractStepStageProgressJob,
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

    # def create_update_job(
    #     self,
    #     add_users=None,
    #     remove_users=None,
    #     add_groups=None,
    #     remove_groups=None,
    # ):
    #     if self._state.adding is True:
    #         JobCls = CreateGroupsetJob
    #     else:
    #         JobCls = UpdateGroupsetJob

    #     JobCls(data=dict(
            
    #     ))
        

class GroupsetJob(AbstractStepStageProgressJob):

    class JobType(models.IntegerChoices):
        CREATE = 1
        UPDATE = 2
        DELETE = 3

    class Step(models.TextChoices):
        ADD_USER = 'ADD_USER'
        REMOVE_USER = 'REMOVE_USER'

    class Stage(models.TextChoices):
        GROUP_UPDATE = 'GROUPS_UPADTE'
        USERS_UPADTE = 'USERS_UPDATE'
        CREATE_GROUPSET = 'CREATE_GROUPSET'
        DELETE_GROUPSET = 'DELETE_GROUPSET'

    groupset = models.ForeignKey(Groupset, on_delete=models.SET_NULL, null=True)

    def step_success(self, step, **step_data):    
        return self.diagnostics.create(
            step=step,
            details=step_data,
        )

    def step_fail(
        self,
        step,
        severity=Severity.CRITICAL,
        raise_error=True,
        **step_data
    ):
        obj = self.diagnostics.create(
            step=step,
            severity=severity,
            details=step_data,
        )
        super().step_fail(
            step,
            severity=severity,
            raise_error=raise_error,
            step_data=step_data
        )
        return obj

    def step_start(self, stage, message='Step stared.'):
        return self.diagnostics.create(
            step=step,
            details=dict(message=message),
        )

    def step_end(self, stage, message='Step completed.'):
        return self.diagnostics.create(
            step=step,
            details=dict(message=message),
        )

    def stage_success(self, stage, **stage_data):
        return self.diagnostics.create(
            stage=stage,
            details=stage_data
        )

    def stage_fail(
        self,
        stage,
        severity=Severity.CRITICAL,
        raise_error=True,
        **stage_data
    ):
        return self.diagnostics.create(
            *args,
            stage=stage,
            severity=severity,
            details=stage_data,
        )

    def stage_start(self, stage, message='Stage stared.'):
        return self.diagnostics.create(
            stage=stage,
            details=dict(message=message),
        )

    def stage_end(self, stage, message='Stage completed.'):
        return self.diagnostics.create(
            stage=stage,
            details=dict(message=message),
        )

    def job_status_from_diagnostics(self):
        if self.diagnostics.objects.filter(severity=Severity.CRITICAL).exists():
            return JobStatus.FAILED
        return JobStatus.SUCCESS

    def add_user(self, user):
        try:
            with transaction.atomic():
                self.groupset.users.remove(user)
                self.groupset.save()
        except Exception as e:
            self.step_fail(            
                self.Step.ADD_USER,
                groupset_name=self.groupset.name,
                username=user.username,
                raise_error=False
            )
        else:
            self.step_success(            
                self.Step.ADD_USER,
                groupset_name=self.groupset.name,
                username=user.username,
            )
        self.add_done_units(1)

    def remove_user(self, user):
        try:
            with transaction.atomic():
                self.groupset.users.remove(user)
                self.groupset.save()
        except Exception as e:
            self.step_fail(            
                self.Step.REMOVE_USER,
                groupset_name=self.groupset.name,
                username=user.username,
                raise_error=False
            )
        else:
            self.step_success(            
                self.Step.REMOVE_USER,
                groupset_name=self.groupset.name,
                username=user.username,
            )
        self.add_done_units(1)

    @property
    def data(self):
        # TODO real data
        return {
            'assign_group_ids': [1, 4],
            'remove_group_ids': [7, 8],
            'add_user_ids': [1, 2, 3, 5],
            'remove_user_ids': [4, 6],
        }

    def update_users(self, add_users, remove_users):
        with self.StageContext(self.Stage.USERS_UPDATE):
            try:
                for user in add_users:
                    self.step_add_user(user)
                for user in remove_users:
                    self.step_remove_user(user)
            except Exception as e:
                logger.exception(e)
                self.stage_fail(GroupsetJobDiagnostic.Stage.USERS_UPDATE)
            else:
                self.stage_success(GroupsetJobDiagnostic.Stage.USERS_UPDATE)

    def update_groups(self, add_groups, remove_groups):
        with self.AnnounceStage(self.Stage.GROUP_UPADTE):
            try:
                with transaction.atomic():
                    self.groupset.groups.add(add_groups)
                    self.groupset.groups.remove(remove_groups)
            except Exception as e:
                logger.exception(e)
                self.stage_fail(self.Stage.GROUP_UPADTE)
            else:
                self.stage_success(self.Stage.GROUP_UPADTE)

    def act(self):
        add_users = self.groupset.users if '*' in add_group_ids else self.groupset.users.filter(
            id__in=self.data.get('add_user_ids', [])
        )
        remove_users = self.groupset.users if '*' in  remove_user_ids else self.groupset.users.filter(
            id__in=self.data.get('remove_user_ids', [])
        )
        add_groups = self.groupset.groups if '*' in add_group_ids else self.groupset.groups.filter(
            id__in=self.data.get('add_group_ids', [])
        )
        remove_groups = self.groupset.groups if '*' in remove_group_ids else self.groupset.groups.filter(
            id__in=self.data.get('remove_group_ids', [])
        )

        self.add_total_units(
            add_groups.count() +
            remove_groups.count() +
            add_users.count() +
            remove_users.count()
        )

        self.update_groups(add_groups, remove_groups)
        self.update_users(add_users, remove_users)


class ManagerCreateGroupsetJob(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(
            GroupsetJob.JobType.CREATE   
        )


class CreateGroupsetJob(GroupsetJob):
    type = GroupsetJob.JobType.CREATE
    objects = ManagerCreateGroupsetJob()

    class Meta:
        proxy = True

    def create_groupset(self):
        with self.AnnounceStage(self.Stage.CREATE_GROUPSET):
            try:
                with transaction.atomic():
                    self.groupset.delete()
            except Exception as e:
                logger.exception(e)
                self.stage_fail(self.Stage.DELETE_GROUPSET)


class ManagerUpdateGroupset(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(
            GroupsetJob.JobType.CREATE   
        )


class UpdateGroupsetJob(GroupsetJob):
    type = GroupsetJob.JobType.UPDATE
    objects = ManagerUpdateGroupset()
    class Meta:
        proxy = True


class ManagerDeleteGroupsetJob(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(
            GroupsetJob.JobType.CREATE   
        )


class DeleteGroupsetJob(GroupsetJob):
    type = GroupsetJob.JobType.DELETE
    objects = ManagerDeleteGroupsetJob()

    class Meta:
        proxy = True

    def stage_delete_groupset(self):
        with self.AnnounceStage(self.Stage.DELETE_GROUPSET):
            try:
                with transaction.atomic():
                    self.groupset.delete()
            except Exception as e:
                logger.exception(e)
                self.stage_fail(self.Stage.DELETE_GROUPSET)

    def act(self):
        self.add_total_units(1)
        super().act()
        self.stage_delete_groupset()
        self.add_done_units(1)


class GroupsetJobDiagnostic(AbstractDiagnostic):
    job = models.ForeignKey(
        GroupsetJob,
        on_delete=models.CASCADE,
        related_name='diagnostics'
    )


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