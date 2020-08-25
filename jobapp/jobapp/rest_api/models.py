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


class Stage(models.TextChoice):
    ROLES_UPDATE = 'Roles update'
    USERS_UPADTE = 'USERS updtae'


class GroupsetSyncJob(
    AbstractJob,
    JobProgressMixin,
    JobRunnerMixin
):

    groupset = models.ForeignKey(Groupset, on_delete=models.SET_NULL)

    def _add_user_to_groups(self, user):
        with transaction.atomic():
            try:
                self.groupset.users.add(user)
                self.groupset.save()
                # user sync_with_okta()
                self.add_diagnostic(
                    user=user,
                    message='Roles added successfully.',
                    serverity=Severity.INFO
                )
            except Exception as e:
                logger.exception(e)
                self.add_diagnostic(
                    user=user,
                    groupset=groupset,
                    message='Failed update roles.',
                    serverity=Severity.WARNING # TODO depends on type of error
                )
            finally:
                self.report_progress(units=1)

    def _remove_user_to_groupset(self, user, groupset):
        with transaction.atomic():
            try:
                groupset.users.remove(user)
                groupset.save()
                # user.sync_with_okta()
                self.add_user_diagnostic(
                    user=user,
                    message='Roles removed successfully.',
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

    def _remove_all_users(self, groupset):
        for user in groupset.users:
            self._remove_user_from_groupset(user, groupset)

    def _add_all_users(self, groupset):
        for user in groupset.users:
            self._add_user_to_groupset(user, groupset)

    def _remove_groupset(self, groupset):
        self.total_units += 1 # one additional operation to delete group
        self.notify()
        self._remove_all_users(groupset)
        # TODO: delete job and diagnostics history?
        # Override delete()?
        with transaction.atomic():
            groupset.delete()
        self.report_progress(units=1)

    @property
    def data(self):
        # TODO real data
        return {
            'operation': 'update_groupset',
            'assign_role_ids': [1, 4],
            'remove_role_ids': [7, 8],
            'add_user_ids': [1, 2, 3, 5],
            'remove_user_ids': [4, 6],
        }
        '''
        return {
            'operation': 'delete_groupset'
        }
        '''

    def start_stage(self, stage):
        super().start_stage()
        self.dignostics.create(message=f'Stage started {self.stage.value}.')

    def end_stage(self, stage):
        super().start_stage()
        self.dignostics.create(message=f'Stage completed {self.stage.value}.')

    def _stage_roles_update(self, assign_roles, remove_roles):
        self.start_stage(JobStage.ROLES_UPDATE)
        try:
            with transaction.atomic():
                # TODO: not found exception
                role = self.groupset.roles.get(id=role_id)
                for role_id in assign_role_ids:
                    self.groupset.roles.add(role)
                for role_id in assign_role_ids:
                    self.groupset.roles.remove(role)
                self.report_progress(len(assign_roles) + len(remove_roles))
        except Exception as e:
            logger.exception(e)
            self.fail(reason=f'Failed to assign roles.')
            self.dignostics.create(message=f'Stage failed {self.stage}.', severity=Severity.CRITICAL) 
        finally:
            self.end_stage(JobStage.ROLES_UPDATE)

    def _stage_users_update(self, add_users, remove_users):
        self.start_stage(JobStage.USERS_UPDATE)
        try:
            self._add_users(add_users)
            self._remove_users(remove_users)
        except Exception as e:
            logger.exception(e)
            self.fail(reason=f'Failed to update users.')
        finally:
            self.end_stage(JobStage.USERS_UPDATE)

    def act(self):
        (op,
         add_user_ids,
         remove_user_ids,
         assign_roles,
         remove_roles) = (
            self.data['operation'],
            self.data['add_user_ids'],
            self.data['remove_user_ids']
            self.data['assign_role_ids']
            self.data['remove_role_ids']
        )
        self.add_units(
            len(add_user_ids) +
            len(remove_user_ids) +
            len(assign_roles) +
            len(remove_roles) +
        )
        if self.type == GroupSyncJobType.CREATE_NEW:
            self._stage_update_roles(assign_roles, remove_roles)
            self._stage_update_users(add_user_ids, remove_user_ids)
        if self.type == GroupSyncJobType.UPDATE:
            self._stage_update_roles(assign_roles, remove_roles)
            self._stage_update_users(add_user_ids, remove_user_ids)
        if self.type == GroupSyncJobType.DELETE:
            self._stage_update_roles(assign_roles, remove_roles)
            self._stage_update_users(add_user_ids, remove_user_ids)
            self.groupset.delete()
        print(f'job completed')

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
        GroupsetUserSyncJob,
        on_delete=models.SET_NULL,
        related_name='diagnostics'
    )
    groupset_id = models.IntegerField(null=True)
    user_id = models.IntegerField(null=True)
    operation = models.CharField(max_length=10)


class DeleteGroupsetJob(GroupsetSyncJob):
    type = JobType.DELETED_GROUPSET.value
    class Meta:
        proxy = True


