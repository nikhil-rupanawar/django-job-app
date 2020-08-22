import graphene
from graphene import relay
from jobapp.jobapp import models
from django.contrib.auth import models as django_authmodels
from graphene_django import DjangoObjectType
from graphene_django.filter import DjangoFilterConnectionField

class User(DjangoObjectType):
    class Meta:
        model = models.User
        interfaces = (relay.Node, )
        filter_fields = {
            "id": ("exact", ),
            "first_name": ("icontains", "iexact"),
            "last_name": ("icontains", "iexact"),
            "username": ("icontains", "iexact"),
            "email": ("icontains", "iexact"),
            "is_staff": ("exact", ),
        }


class Group(DjangoObjectType):
    class Meta:
        model = django_authmodels.Group
        interfaces = (relay.Node, )
        filter_fields = ['name']


class Groupset(DjangoObjectType):
    class Meta:
        model = models.Groupset
        interfaces = (relay.Node, )
        filter_fields = ['name']
    users = DjangoFilterConnectionField(User)
    groups = DjangoFilterConnectionField(User)


class GroupsetIdpSyncJob(DjangoObjectType):
    class Meta:
        model = models.GroupsetIdpSyncJob
        interfaces = (relay.Node, )
    

class GroupsetIdpSyncJobDiagnostic(DjangoObjectType):
    class Meta:
        model = models.GroupsetIdpSyncJobDiagnostic
        interfaces = (relay.Node, )


class Job(graphene.Union):
    class Meta:
        types = (GroupsetIdpSyncJob,)


class Query(graphene.ObjectType):
    all_users = DjangoFilterConnectionField(User)
    all_groups =  DjangoFilterConnectionField(Group)
    all_groupset = DjangoFilterConnectionField(Groupset)
    jobs = graphene.List(Job)

schema = graphene.Schema(query=Query) 
