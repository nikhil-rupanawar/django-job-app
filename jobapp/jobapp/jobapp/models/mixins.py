from django.db import  models
from contextlib import contextmanager
from ..exceptions import (
    JobStateError
)

class AbstractProgressJobMixin(models.Model):
    class Meta:
        abstract = True
    _progress_total_units = models.IntegerField(default=0)
    _progress_done_units = models.IntegerField(default=0)
    _percent_progress = models.IntegerField(null=True)

    @property
    def progress_total_units(self):
        return self._progress_total_units

    @property
    def progress_done_units(self):
        return self._progress_done_units

    def add_progress_total_units(self, units):
        self._progress_total_units += units

    def add_progress_done_units(self, units, notify=True):
        self._progress_done_units += units
        if notify:
            self.notify()

    @property
    def remaining_progress_units(self):
        return self.progress_total_units - self.progress_done_units

    @property
    def percent_progress(self):
        if self._percent_progress is not None:
            return self._percent_progress
        if self.progress_total_units == 0:
            return 0
        return (self.progress_done_units * 100) / self.progress_total_units
 
    @percent_progress.setter
    def percent_progress(self, value: int):
        assert 0 <= value <= 100
        self._percent_progress = value


class StepJobMixin:
    
    def __init__(self):
        self.current_stage = None
        self.current_step = None
        self.current_stage_data = None
        self.current_step_data = None

    def on_step_success(self, *args, **kwargs):
        pass

    def on_step_fail(self):
        pass

    @contextmanager
    def step_context(self, step, **data):
        self.current_step = step
        self.current_step_data = data
        self.on_step_start()
        try:
            yield
        except JobStateError as e:
            self.current_step_data.update(
                {'error': str(e) }
            )
            self.on_step_fail()
            raise          
        except Exception as e:
            print(e)
            self.current_data.update(
                {'error': str(e) }
            )
            self.on_step_fail()
            raise
        else:
            self.on_step_success()
        finally:
            self.on_step_end()
            self.current_step = None
            self.current_step_data = None

    StepContext = step_context

    def on_step_start(self):
        pass

    def on_step_end(self):
        pass

    def on_stage_start(self):
        pass

    def on_stage_end(self):
        pass

    def on_stage_success(self):
        pass

    def on_stage_fail(self):
        pass

    @contextmanager
    def stage_context(self, stage, **data):
        self.current_stage = stage
        self.current_stage_data = data
        self.on_stage_start()
        try:
            yield
        except JobStateError as e:
            self.current_stage_data.update(
                {'error': str(e)}
            )
            self.on_stage_fail()
        except Exception as e:
            print(e)
            self.current_stage_data.update(
                {'error': 'Something went wrong'}
            )
            self.on_stage_fail()
            raise
        else:
            self.on_stage_success()
        finally:
            self.on_stage_end()
            self.current_stage = None
            self.current_stage_data = None

    StageContext = stage_context
