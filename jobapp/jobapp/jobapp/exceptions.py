class JobStateError(Exception):
    pass


class JobFailedError(JobStateError):
    pass


class JobStageFailedError(JobStateError):
    pass


class JobStepFailedError(JobStateError):
    pass


class JobCanceledError(JobStateError):
    pass