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


class GroupsetUserSyncJob(AbstractJob, JobProgressMixin, JobRunnerMixin):
    @classmethod
    def _add_user_to_groupset(self, user, groupset):
        with transaction.atomic():
            try:
                groupset.users.add(user)
                groupset.save()
                user.save()
                user.sync_with_okta()
            except Exception as e:
                logger.exception(e)
                print(e)

    @property
    def data(self):
        # TODO real data
        return [
            {'operation': 'add', 'groupset_id': 1, 'user_id': 1},
            {'operation': 'add', 'groupset_id': 1, 'user_id': 1},
            {'operation': 'remove', 'groupset_id': 1, 'user_id': 4},
            {'operation': 'remove', 'groupset_id': 1, 'user_id': 5},
            {'operation': 'add', 'groupset_id': 1, 'user_id': 6},
        ]

    def act(self):
        groupset = Groupset.objects.get(id=self.data['groupset_id'])
        to_add_users = User.objects.filter(id__in=self.data['to_add_users'])
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
                self._add_user_to_groupset(user, groupset)
            print(f'Prcessed: {operation} {user.username} {groupset.name}')
            self.report_progress(units=1)
            # handle exception and add dignostics
            self.add_user_diagnostic(
                user=user,
                groupset=groupset,
                message='Ok',
                serverity=Severity.INFO
            )
        # TODO: remove user from groupset
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


