from django.test import TestCase
from jobapp.rest_api.tests import factory
from jobapp.rest_api import models


class TestEffectiveGroups(TestCase):

    def test_effective_groups(self):
        created_direct_groups = []
        created_groupset_groups = []
        created_common_groups = []

        for _ in range(5):
            created_direct_groups.append(factory.GroupFactory.create())

        for _ in range(5):
            created_groupset_groups.append(factory.GroupFactory.create())

        for _ in range(2):
            cg = factory.GroupFactory.create()
            created_direct_groups.append(cg)
            created_groupset_groups.append(cg)
            created_common_groups.append(cg)

        groupset = factory.GroupsetFactory.create(groups=created_groupset_groups)
        user = factory.UserFactory.create(groups=created_direct_groups)
        groupset.users.add(user)
        groupset.save()

        expected_direct_group_names = [g.name for g in created_direct_groups]
        expected_groupset_group_names = [g.name for g in created_groupset_groups]
        expected_common_group_names = [g.name for g in created_common_groups]
        expected_effective_group_names = set(
            expected_direct_group_names + expected_groupset_group_names + expected_common_group_names
        )

        self.assertEquals(
            set(user.direct_group_names).intersection(expected_common_group_names),
            set(expected_common_group_names)
        )

        self.assertEquals(
            set(expected_direct_group_names),
            user.direct_group_names
        )
        self.assertEquals(
            set(expected_effective_group_names),
            user.effective_group_names
        )





