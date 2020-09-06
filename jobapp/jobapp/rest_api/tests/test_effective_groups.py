from django.test import TestCase
from jobapp.rest_api.tests import factories
from jobapp.rest_api import models


class TestDbRelationships(TestCase):

    def setUp(self):
        self.direct_groups = factories.GroupFactory.create_batch(5)
        self.groupset_groups = factories.GroupFactory.create_batch(5)
        self.common_groups = factories.GroupFactory.create_batch(2)
        self.direct_groups.extend(self.common_groups)
        self.groupset_groups.extend(self.common_groups)
        groupset = factories.GroupsetFactory.create(groups=self.groupset_groups)
        user = factories.UserFactory.create(groups=self.direct_groups)
        groupset.users.add(user)
        groupset.save()
        self.user = user
        self.groupset = groupset

        g = factories.GroupsetFactory.build()
        g.save_async()

    def tearDown(self):
        self.groupset.delete()
        self.user.delete()
        models.Group.objects.all().delete()

    def test_relationships_user_groupset_group(self):
        self.assertEquals(list(self.user.groups.all()), self.direct_groups)
        self.assertEquals(self.user.groupsets.first(), self.groupset)

    def test_user_properties(self):
        self.assertEquals(set(self.user.groupset_groups), set(self.groupset_groups))
        self.assertEquals(set(
            self.user.effective_groups),
            set(self.groupset_groups + self.direct_groups + self.common_groups)
        )

    def test_effective_group_names(self):
        expected_direct_group_names = [g.name for g in self.direct_groups]
        expected_groupset_group_names = [g.name for g in self.groupset_groups]
        expected_common_group_names = [g.name for g in self.common_groups]
        expected_effective_group_names = set(
            (
                expected_direct_group_names +
                expected_groupset_group_names +
                expected_common_group_names
            )
        )
        self.assertEquals(
            set(self.user.groupsets_groups_names)
            .intersection(
                expected_common_group_names
            ),
            set(expected_common_group_names)
        )
        self.assertEquals(
            set(self.user.direct_group_names)
            .intersection(
                expected_common_group_names
            ),
            set(expected_common_group_names)
        )
        self.assertEquals(
            set(expected_direct_group_names),
            self.user.direct_group_names
        )
        self.assertEquals(
            set(expected_groupset_group_names),
            self.user.groupsets_groups_names
        )
        self.assertEquals(
            expected_effective_group_names,
            self.user.effective_group_names
        )

