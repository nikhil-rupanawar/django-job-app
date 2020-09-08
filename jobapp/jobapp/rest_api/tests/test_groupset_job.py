from django.test import TestCase
from jobapp.rest_api.tests import factories
from jobapp.rest_api import models


class TestGroupsetJob(TestCase):

    def setUp(self):
        self.to_remove_groups = factories.GroupFactory.create_batch(5)
        self.to_add_groups = factories.GroupFactory.create_batch(5)
        self.to_remove_users = factories.UserFactory.create_batch(5)
        self.to_add_users = factories.UserFactory.create_batch(5)
        self.groupset = factories.GroupsetFactory.create(
            groups=self.to_remove_groups,
            users=self.to_remove_users,
        )

    def tearDown(self):
        self.groupset.delete()
        models.Group.objects.all().delete()
        models.User.objects.all().delete()

    def test_update_groupset_users_job(self):
        data = {
            'add_user_ids': [user.id for user in self.to_add_users],
            'remove_user_ids': [user.id for user in self.to_remove_users],
            'add_group_ids': [g.id for g in self.to_add_groups],
            'remove_group_ids': [g.id for g in self.to_remove_groups]
        }
        job = models.UpdateGroupsetJob(
            groupset=self.groupset,
            _data=data
        )
        job.save()

        # if not any 
        job.run()

        print()
        print(f"{'#' * 50} Job Diagnostics {'#' * 50}")
        for dc in job.diagnostics.order_by('id').all():
            print(
                f'[{dc.created_at}] | severity={dc.severity} | stage={dc.stage} | step={dc.step} | message={dc.message} | details={dc.details}'
            )
        print(job.to_dict())
        self.assertEquals(self.groupset.last_job, job)