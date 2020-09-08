import  abc

class AbstractJobNotifier(abc.ABC):
    @abc.abstractmethod
    def notify(self, job):
        ...


class DbUpdateNotifier(AbstractJobNotifier):
    """ Simply save the job state to db """
    def notify(self, job):
        job.save()