from django.test import TestCase
from jobapp.rest_api.tests import factories
from jobapp.rest_api import models


class TestGroupsetJob(TestCase):

    def setUp(self):
        self.groupset_groups = factories.GroupFactory.create_batch(5)
        self.already_added_users = self.remove_users = factories.UserFactory.create_batch(5)
        self.groupset = factories.GroupsetFactory.create(
            groups=self.groupset_groups,
            users=self.already_added_users,
        )
        self.add_users = factories.UserFactory.create_batch(5)

    def tearDown(self):
        self.groupset.delete()
        models.Group.objects.all().delete()
        models.User.objects.all().delete()

    def test_update_groupset_users_job(self):
        data = {
            'add_user_ids': [user.id for user in self.add_users],
            'remove_user_ids': [user.id for user in self.already_added_users]
        }
        job = models.UpdateGroupsetJob(
            groupset=self.groupset,
            _data=data
        )
        job.save()
        job.run()
        print("######################################################################################")
        for dc in job.diagnostics.order_by('id').all():
            print(
                f'stage={dc.stage} | step={dc.step} | message={dc.message} | details={dc.details}'
            )
