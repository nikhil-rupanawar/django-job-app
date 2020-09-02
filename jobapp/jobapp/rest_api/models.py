import logging
import abc
import time
import enum
import django.contrib.postgres.fields as postgres_fields
from django.db import transaction
from django.db import models
from jobapp.jobapp.models import (
    AbstractProgressJob,
    JobStepDiagnosticMixin,
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
    def direct_groups(self):
        return [g.name for g in self.groups.all()]

    @property
    def groupset_groups(self):
        return [
            g.name for gs in self.groupset_set.all()
            for g in gs.groups.all()
        ]

    @property
    def effective_groups(self):
        return set(self.direct_groups + self.groupset_groups)


class Groupset(models.Model):
    name = models.CharField(max_length=255)
    users = models.ManyToManyField(User)
    groups = models.ManyToManyField(Group)


class GroupsetJob(AbstractProgressJob, StepDiagnosticMixin):

    STEP_DIAGNOSTIC_RELATED_NAME = 'diagnostics'

    class GroupsetJobType(models.IntegerChoices):
        CREATE = 1
        UPDATE = 2
        DELETE = 3

    groupset = models.ForeignKey(Groupset, on_delete=models.SET_NULL, null=True)

    def step_add_user(self, user):
        try:
            with transaction.atomic():
                self.groupset.users.remove(user)
                self.groupset.save()
        except Exception as e:
            self.step_fail(            
                GroupsetJobDiagnostic.Step.ADD_USER,
                groupset_name=self.groupset.name,
                username=user.username,
                raise_error=False
            )
        else:
            self.step_success(            
                GroupsetJobDiagnostic.Step.ADD_USER,
                groupset_name=self.groupset.name,
                username=user.username,
            )
        self.add_done_units(1)

    def step_remove_user(self, user):
        try:
            with transaction.atomic():
                self.groupset.users.remove(user)
                self.groupset.save()
        except Exception as e:
            self.step_fail(            
                GroupsetJobDiagnostic.Step.REMOVE_USER,
                groupset_name=self.groupset.name,
                username=user.username,
                raise_error=False
            )
        else:
            self.step_success(            
                GroupsetJobDiagnostic.Step.REMOVE_USER,
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

    def stage_users_update(self, add_users, remove_users):
        with self.AnnounceStage(GroupsetJobDiagnostic.Stage.USERS_UPDATE):
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

    def stage_groups_update(self, add_groups, remove_groups):
        with self.AnnounceStage(GroupsetJobDiagnostic.Stage.GROUP_UPADTE):
            try:
                with transaction.atomic():
                    self.groupset.groups.add(add_groups)
                    self.groupset.groups.remove(remove_groups)
            except Exception as e:
                logger.exception(e)
                self.stage_fail(GroupsetJobDiagnostic.Stage.GROUP_UPADTE)
            else:
                self.stage_success(GroupsetJobDiagnostic.Stage.GROUP_UPADTE)

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
        self.stage_groups_update(add_groups, remove_groups)
        self.stage_users_update(add_users, remove_users)


class GroupsetJobDiagnostic(AbstractDiagnostic):

    class Step(models.TextChoices):
        ADD_USER = 'ADD_USER'
        REMOVE_USER = 'REMOVE_USER'

    class Stage(models.TextChoice):
        GROUP_UPDATE = 'GROUPS_UPADTE'
        USERS_UPADTE = 'USERS_UPDATE'
        DELETE_GROUPSET = 'DELETE_GROUPSET'

    job = models.ForeignKey(
        GroupsetJob,
        on_delete=models.CASCADE,
        related_name='diagnostics'
    )


class ManagerCreateGroupsetJob(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(
            GroupsetJob.GroupsetJobType.CREATE   
        )


class CreateGroupsetJob(GroupsetJob):
    type = GroupsetJob.GroupsetJobType.CREATE
    objects = ManagerCreateGroupsetJob()
    class Meta:
        proxy = True


class ManagerUpdateGroupset(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(
            GroupsetJob.GroupsetJobType.CREATE   
        )


class UpdateGroupsetJob(GroupsetJob):
    type = GroupsetJob.GroupsetJobType.UPDATE
    objects = ManagerUpdateGroupset()
    class Meta:
        proxy = True


class ManagerDeleteGroupsetJob(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(
            GroupsetJob.GroupsetJobType.CREATE   
        )


class DeleteGroupsetJob(GroupsetJob):
    type = GroupsetJob.GroupsetJobType.DELETE
    objects = ManagerDeleteGroupsetJob()
    class Meta:
        proxy = True

    def stage_delete_groupset(self):
        with self.diagnostics.StageContext(GroupsetJobDiagnostic.Stage.DELETE_GROUPSET)
            with transaction.atomic():
                self.groupset.delete()

    def act(self):
        self.add_total_units(1)
        super().act()
        self.stage_delete_groupset()
        self.add_done_units(1)


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