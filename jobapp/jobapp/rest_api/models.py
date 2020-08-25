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


class Stage(models.TextChoice):
    ROLES_UPDATE = 'Roles update'
    USERS_UPADTE = 'USERS updtae'


class GroupsetSyncJob(
    AbstractJob,
    JobProgressMixin,
    JobRunnerMixin
):

    groupset = models.ForeignKey(Groupset, on_delete=models.SET_NULL)

    def add_user(self, user):
        with transaction.atomic():
            try:
                self.groupset.users.add(user)
                self.groupset.save()
                # user sync_with_okta()
                self.add_diagnostic(
                    user=user,
                    operation='add_user'
                    message='Roles added successfully.',
                    serverity=Severity.INFO
                )
            except Exception as e:
                logger.exception(e)
                self.add_diagnostic(
                    user=user,
                    groupset=groupset,
                    message='Failed update groups.',
                    serverity=Severity.WARNING # TODO depends on type of error
                )
            finally:
                self.report_progress(units=1)

    def remove_user(self, user):
        with transaction.atomic():
            try:
                groupset.users.remove(user)
                groupset.save()
                # user.sync_with_okta()
                self.add_diagnostic(
                    user=user,
                    message='User removed successfully.',
                    serverity=Severity.INFO
                )
            except Exception as e:
                logger.exception(e)
                self.add_user_diagnostic(
                    user=user,
                    message='Failed to remove user',
                    serverity=Severity.WARNING # TODO depends on type of error
                )
            finally:
                self.report_progress(units=1)

    def remove_all_users(self):
        for user in groupset.users:
            self.remove_user(user)

    def add_all_users(self):
        for user in groupset.users:
            self.add_user(user)

    @property
    def data(self):
        # TODO real data
        return {
            'assign_group_ids': [1, 4],
            'remove_group_ids': [7, 8],
            'add_user_ids': [1, 2, 3, 5],
            'remove_user_ids': [4, 6],
        }

    def start_stage(self, stage):
        super().start_stage()
        self.dignostics.create(message=f'Stage started {self.stage.value}.')

    def end_stage(self, stage):
        super().start_stage()
        self.dignostics.create(message=f'Stage completed {self.stage.value}.')

    def _stage_groups_update(self):
        self.start_stage(JobStage.ROLES_UPDATE)
        try:
            with transaction.atomic():
                # TODO: not found exception
                for group_id in self.data.get('add_group_ids', []):
                    group = Group.objects.get(id=group_id)
                    self.groupset.groups.add(group)
                for group_id in self.data.get('remove_group_ids', []):
                    group = Group.objects.get(id=group_id)
                    self.groupset.groups.remove(group)
                self.report_progress(len(assign_groups) + len(remove_groups))
        except Exception as e:
            logger.exception(e)
            self.fail(reason=f'Failed to assign groups.')
            self.dignostics.create(message=f'Stage failed {self.stage}.', severity=Severity.CRITICAL) 
        finally:
            self.end_stage(JobStage.ROLES_UPDATE)

    def _stage_users_update(self):
        self.start_stage(JobStage.USERS_UPDATE)
        try:
            for user_id in self.data.get('add_user_ids', []):
                user = User.objects.get(id=user_id)
                self._add_users(add_users)
            for user_id in self.data.get('remove_user_ids', []):
                user = User.objects.get(id=user_id)
                self._remove_users(add_users)
        except Exception as e:
            logger.exception(e)
            self.fail(reason=f'Failed to update users.')
        finally:
            self.end_stage(JobStage.USERS_UPDATE)

    def init_progress(self):
        self.add_units(
            len(self.data.get('add_user_ids', [])) +
            len(self.data.get('remove_user_ids', [])) +
            len(self.data.get('add_group_ids',[])) +
            len(self.data.get('remove_group_ids', []))
        )


    def add_diagnostic(
        self,
        user=None,
        operation=None,
        message=None,
        details=None,
        severity=Severity.INFO
    ):
        self.diagnostics.create(
            severity=severity,
            operation=operation,
            user_id=user.id,
            message=message,
            details=details,
        )


class GroupsetSyncJobDiagnostic(AbstractDiagnostic):
    job = models.ForeignKey(
        GroupsetSyncJob,
        on_delete=models.SET_NULL,
        related_name='diagnostics'
    )
    groupset_id = models.IntegerField(null=True)
    user_id = models.IntegerField(null=True)
    operation = models.CharField(max_length=10, null=True)


class UpdateGroupsetSyncJob(GroupsetSyncJob):
    type = JobType.UPDATE_GROUPSET.value

    class Meta:
        proxy = True

    def act(self):
        self.init_progress()
        self._stage_update_groups()
        self._stage_update_users()


class CreateGroupsetSyncJob(GroupsetSyncJob):
    type = JobType.CREATE_GROUPSET.value

    def _stage_create_groupset(self):
        pass

    def act(self):
        self.init_progress()
        self._stage_create_groupset()
        self._stage_update_groups()
        self._stage_update_users()

    class Meta:
        proxy = True


class DeleteGroupsetSyncJob(GroupsetSyncJob):
    type = JobType.DELETE_GROUPSET.value

    class Meta:
        proxy = True

    def _stage_delete_groupset(self):
        pass

    def act(self):
        self.init_progress()
        self._stage_update_groups()
        self._stage_update_users()
        self._stage_delete_groupset()
