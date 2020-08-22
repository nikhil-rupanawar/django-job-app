import graphene
from graphene_django.types import DjangoObjectType
from jobapp.jobapp import models


class SchedulerDirector(DjangoObjectType):
    class Meta:
        model = models.SchedulerDirector


class ClockNode(DjangoObjectType):
    class Meta:
        model = models.ClockNode


class Query(graphene.ObjectType):
    all_scheduler_director = graphene.List(SchedulerDirector)
    all_clock_nodes = graphene.List(ClockNode)

    def resolve_all_scheduler_director(self, info, **kwargs):
        return models.SchedulerDirector.objects.all()

    def resolve_all_clock_nodes(self, info, **kwargs):
        return models.ClockNode.objects.all()


schema = graphene.Schema(query=Query)
