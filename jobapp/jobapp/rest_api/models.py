import logging
import abc
import time
import enum
import django.contrib.postgres.fields as postgres_fields
from django.db import transaction
from django.db import models
from jobapp.jobapp.models import (
    AbstractJob,
    JobProgressMixin,
    JobRunnerMixin,
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


class GroupsetJob(
    AbstractJob,
    JobProgressMixin,
    JobRunnerMixin
):
    class GroupsetJobType(models.IntegerChoices):
        CREATE = 1
        UPDATE = 2
        DELETE = 3

    groupset = models.ForeignKey(Groupset, on_delete=models.SET_NULL)

    def add_user(self, user):
        result = True
        try:
            with transaction.atomic():
                self.groupset.users.add(user)
                self.groupset.save()
                self.add_diagnostic(
                    user=user,
                    message='User added successfully.',
                    serverity=Severity.INFO
                )
        except Exception as e:
            logger.exception(e)
            self.add_diagnostic(
                user=user,
                message='Failed update groups.',
                serverity=Severity.CRITICAL # TODO depends on type of error
            )
            result = False
            self.fail(raise_error=False)
        finally:
            self.report_progress(units=1)
        return result

    def remove_user(self, user):
        try:
            with transaction.atomic():
                self.groupset.users.remove(user)
                self.groupset.save()
                self.add_diagnostic(
                    user=user,
                    message='User removed.',
                    serverity=Severity.INFO
                )
        except Exception as e:
            logger.exception(e)
            self.add_user_diagnostic(
                user=user,
                message='Failed to remove user.',
                serverity=Severity.CRITICAL # TODO depends on type of error
            )
        finally:
            self.report_progress(units=1)
        return result

    @property
    def data(self):
        # TODO real data
        return {
            'assign_group_ids': [1, 4],
            'remove_group_ids': [7, 8],
            'add_user_ids': [1, 2, 3, 5],
            'remove_user_ids': [4, 6],
        }

    def start_stage(self, stage, message=None):
        super().start_stage()
        self.add_diagnostic(message=message)

    def end_stage(self, stage, message=None):
        super().start_stage()
        self.add_diagnostic(message=message)

    def fail_stage(self, stage, reason=None):
        self.dignostics.create(message=reason)
        super().fail_stage(reason=reason)

    def stage_users_update(self, add_users, remove_users):
        self.start_stage(GroupsetJobDiagnostic.Stage.USERS_UPDATE)
        try:
            for user in add_users:
                self.add_users(user)
            for user in remove_users:
                self.remove_users(user)
        except Exception as e:
            logger.exception(e)
        finally:
            self.end_stage(GroupsetJobDiagnostic.Stage.USERS_UPDATE)
        if self.diagnostics.filter(severity=Severity.CRITICAL).exists():
            self.fail_stage(GroupsetJobDiagnostic.Stage.USERS_UPADTE)
            self.fail(reason=f'Stage failed {GroupsetJobDiagnostic.Stage.USERS_UPDATE}')

    def stage_groups_update(self, add_groups, remove_groups):
        self.start_stage(GroupsetJobDiagnostic.Stage.GROUP_UPDATE)
        try:
            with transaction.atomic():
                self.groupset.groups.add(add_groups)
                self.groupset.groups.remove(remove_groups)
        except Exception as e:
            logger.exception(e)
            self.fail_stage(GroupsetJobDiagnostic.Stage.GROUP_UPDATE)
        finally:
            self.report_progress(len(add_groups) + len(remove_groups))
            self.end_stage(GroupsetJobDiagnostic.Stage.GROUP_UPDATE)
        if self.diagnostics.filter(severity=Severity.CRITICAL).exists():
            self.fail_stage(GroupsetJobDiagnostic.Stage.GROUP_UPADTE)
            self.fail(reason=f'Stage failed {GroupsetJobDiagnostic.Stage.GROUP_UPDATE}')

    def add_diagnostic(
        self,
        user=None,
        step=None,
        stage=None,
        message=None,
        details=None,
        severity=Severity.INFO
    ):
        self.diagnostics.create(
            user_id=user.id,
            username=user.username,
            severity=severity,
            step=step,
            stage=stage,
            message=message,
            details=details,
        )

    def act(self):
        if '*' in self.data['add_user_ids']:
            add_users = self.groupset.users
        else:
            add_users = self.groupset.users.filter(id__in=self.data['add_user_ids'])

        if '*' in self.data['remove_user_ids']:
            remove_users = self.groupset.users
        else:
            remove_users = self.groupset.users.filter(id__in=self.data['remove_user_ids'])

        if '*' in self.data['add_group_ids']:
            add_groups = self.groupset.groups
        else:
            add_groups = self.groupset.groups.filter(id__in=self.data['add_group_ids'])

        if '*' in self.data['remove_group_ids']:
            remove_groups = self.groupset.groups
        else:
            remove_groups = self.groupset.groups.filter(id__in=self.data['add_group_ids'])

        self.add_units(
            add_groups.count() +
            remove_groups.count() +
            add_users.count() +
            remove_users.count()
        )
        self.stage_groups_update(add_groups, remove_groups)
        self.stage_users_update(add_users, remove_users)


class GroupsetJobDiagnostic(AbstractDiagnostic):

    class Stage(models.TextChoice):
        UPDATE_UPDATE = 'UPDATE_GROUP'
        USERS_UPADTE = 'UPADTE_GROUP'
        DELETE_GROUPSET = 'DELETE_GROUPSET'

    class Step(models.TextChoices):
        ADD_USER = 'ADD_USER'
        REMOVE_USER = 'REMOVE_USER'

    job = models.ForeignKey(
        GroupsetJob,
        on_delete=models.SET_NULL,
        related_name='diagnostics'
    )
    user_id = models.IntegerField(null=True)
    username = models.CharField(max_length=255, null=True)


class ManagerCreateGroupsetJob(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(
            GroupsetJob.GroupsetJobType.CREATE   
        )


class CreateGroupsetJob(GroupsetJob):
    type = GroupsetJob.GroupsetJobType.CREATE

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


class DeleteGroupsetJobManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(
            GroupsetJob.GroupsetJobType.CREATE   
        )


class DeleteGroupsetJob(GroupsetJob):
    type = GroupsetJob.GroupsetJobType.DELETE

    class Meta:
        proxy = True

    def stage_delete_groupset(self):
        self.start_stage(GroupsetJobDiagnostic.Stage.DELETE_GROUPSET)
        try:
            with transaction.atomic():
                self.groupset.delete()
        except Exception as e:
            logger.exception(e)
            self.fail_stage(GroupsetJobDiagnostic.Stage.DELETE_GROUPSET)
        finally:
            self.end_stage(GroupsetJobDiagnostic.Stage.DELETE_GROUPSET)
        self.report_progress(done_units=1)

    def act(self):
        self.add_units(1)
        super().act()
        self.stage_delete_groupset()

