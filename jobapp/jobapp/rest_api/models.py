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


class GroupsetUserSyncJob(
    AbstractJob,
    JobProgressMixin,
    JobRunnerMixin
):

    def _add_user_to_groupset(self, user, groupset):
        with transaction.atomic():
            try:
                groupset.users.add(user)
                groupset.save()
                user.sync_with_okta()
                self.add_user_diagnostic(
                    user=user,
                    groupset=groupset,
                    message='Ok',
                    serverity=Severity.INFO
                )
            except Exception as e:
                logger.exception(e)
                self.add_user_diagnostic(
                    user=user,
                    groupset=groupset,
                    message='Failed to add user',
                    serverity=Severity.WARNING # TODO depends on type of error
                )
            finally:
                self.report_progress(units=1)

    def _remove_user_to_groupset(self, user, groupset):
        with transaction.atomic():
            try:
                groupset.users.remove(user)
                groupset.save()
                user.sync_with_okta()
                self.add_user_diagnostic(
                    user=user,
                    groupset=groupset,
                    message='Ok',
                    serverity=Severity.INFO
                )
            except Exception as e:
                logger.exception(e)
                self.add_user_diagnostic(
                    user=user,
                    groupset=groupset,
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
        return [
            {'operation': 'add_user', 'groupset_id': 1, 'user_id': 1},
            {'operation': 'add_user', 'groupset_id': 1, 'user_id': 1},
            {'operation': 'remove_user', 'groupset_id': 1, 'user_id': 4},
            {'operation': 'remove_user', 'groupset_id': 1, 'user_id': 5},
            {'operation': 'add_user', 'groupset_id': 1, 'user_id': 6},
            {'operation': 'add_all_users', 'groupset_id': 4, 'user_id': None},
            {'operation': 'remove_all_users', 'groupset_id': 5, 'user_id': None},
            {'operation': 'remove_groupset', 'groupset_id': 7, 'user_id': None},
        ]

    def act(self):
        for entry in self.data.items():
            operation, user_id, groupset_id = (
                entry['operation'],
                entry['user_id'],
                entry['groupset_id']
            )
            groupset = Groupset.objects.get(id=groupset_id)
            user = User.objects.get(id=user_id)
            if operation == 'add':
                self._add_user_to_groupset(user, groupset)
            if operation == 'remove':
                self._remove_user_to_groupset(user, groupset)
            if operation == 'add_all_users':
                self._add_all_users(groupset)
            if operation == 'remove_all_users':
                self._remove_all_users(groupset)
            if operation == 'remove_groupset':
                self._remove_groupset(groupset)
        print(f'job completed')

    def add_diagnostic(
        self,
        user,
        groupset,
        operation,
        message=None,
        details=None,
        severity=Severity.INFO
    ):
        self.diagnostics.create(
            severity=severity,
            operation=operation,
            groupset_id=groupset.id,
            user_id=user.id,
            message=message,
            details=details,
        )


class GroupsetUserSyncJobDiagnostic(AbstractDiagnostic):
    job = models.ForeignKey(
        GroupsetUserSyncJob,
        on_delete=models.CASCADE,
        related_name='diagnostics'
    )
    groupset_id = models.IntegerField()
    user_id = models.IntegerField()
    operation = models.CharField(max_length=10)


class DeleteGroupsetJob(GroupsetUserSyncJob):
    class Meta:
        proxy = True


