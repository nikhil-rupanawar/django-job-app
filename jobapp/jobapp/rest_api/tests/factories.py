
import factory.django
from factory import (
    fuzzy,
    SubFactory,
    Sequence,
    Faker,
    LazyFunction,
    LazyAttribute,
)
from jobapp.rest_api import models
from django.contrib.auth import get_user_model, models as auth_models


User = get_user_model()


class UserFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = User
    #id = Sequence(lambda n: n)
    username = Faker('user_name')
    password = Faker('password')
    first_name = Sequence(lambda n: f'User{n}')
    email = LazyAttribute(lambda e: f'{e.username}@example.com')

    @factory.post_generation
    def groups(self, create, extracted, **kwargs):
        if not create:
           return
        if extracted:
            for group in extracted:
                self.groups.add(group)
        self.save()

    @factory.post_generation
    def groupsets(self, create, extracted, **kwargs):
        if not create:
           return
        if extracted:
            for groupset in extracted:
                self.groupsets.add(group)


class GroupFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = auth_models.Group
    name = Sequence(lambda n: f'Group{n}')

    @factory.post_generation
    def users(self, create, extracted, **kwargs):
        if not create:
           return
        if extracted:
            for user in extracted:
                self.users.add(group)

    @factory.post_generation
    def groupsets(self, create, extracted, **kwargs):
        if not create:
           return
        if extracted:
            for groupset in extracted:
                self.groupsets.add(group)


class GroupsetFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = models.Groupset
    name = factory.Sequence(lambda n: "Groupset #%s" % n)

    @factory.post_generation
    def users(self, create, extracted, **kwargs):
        if not create:
           return
        if extracted:
            for user in extracted:
                self.users.add(user)

    @factory.post_generation
    def groups(self, create, extracted, **kwargs):
        if not create:
           return
        if extracted:
            for group in extracted:
                self.groups.add(group)


class CreateGroupsetJobFactoty(factory.django.DjangoModelFactory):
    class Meta:
        model = models.CreateGroupsetJob
    groupset = SubFactory(GroupsetFactory)


class UpdateGroupsetJobFactoty(factory.django.DjangoModelFactory):
    class Meta:
        model = models.UpdateGroupsetJob
    groupset = SubFactory(GroupsetFactory)


class DeleteGroupsetJobFactoty(factory.django.DjangoModelFactory):
    class Meta:
        model = models.DeleteGroupsetJob
    groupset = SubFactory(GroupsetFactory)
