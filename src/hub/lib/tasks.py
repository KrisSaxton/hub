'''
Hub task routines
'''
# Core modules
import os
import uuid
import logging
import subprocess
from tempfile import NamedTemporaryFile

# Own modules
import error
from common import State

# 3rd part modules
import pika
import json
import time
import hub.lib.config as config


class Task(object):
    '''
    Superclass for all Task objects. Tasks are defined units of work
    '''
    def __init__(self, state=None, parent_id=None, async=False):
        self.log = logging.getLogger(__name__)
        if not state:
            state = State()
        self.state = state
        if parent_id is not None:
            self.state.parent_id = parent_id
        if self.state.start_time is None:
            if self.__class__.__name__ == 'Job':
                self.state.start_time = time.time()
        self.async = async
        if not self.state.id:
            self.state.id = uuid.uuid1().__str__()
        if not self.state.task_name:
            self.state.task_name = ''
        if not self.state.status:
            self.state.status = 'PENDING'

    def __repr__(self):
        return self.save()
    
    def __str__(self):
        return self.save()

    def _validate(self):
        '''Ensure task is in a valid format.'''
        if not self.name:
            raise error.ValidationError('Invalid task format')

    def load(self, taskrecord):
        self.state.load(taskrecord)
        return self

    def save(self):
        return self.state.save()

    def set_status(self, status):
        self.state.status = status
        return self

    def get_status(self):
        return self.state.status

    def run(self):
        raise error.MethodNotImplemented('All tasks must have a run method')

    def extrun(self, cmd, ser='json'):
        '''
        Run external commands via json request/reply objects
        '''
        request_file = NamedTemporaryFile(delete=False)
        reply_file = NamedTemporaryFile(delete=False)
        request_file.write(self.state.save())
        request_file.close()
        self.log.info('Running external command {0} {1} {2}'.format(
            cmd, request_file.name, reply_file.name))
        subprocess.check_call([cmd, request_file.name, reply_file.name])
        reply = open(reply_file.name)
        replydata = json.load(reply)
        reply.close()
        os.unlink(request_file)
        os.unlink(reply_file)
        return replydata



class WrappedCallableTask(Task):
    '''
    Wraps a given callable transparently, while marking it as a valid Task.
    Generally used via @task decorator and not directly.
    '''
    def __init__(self, callable, *args, **kwargs):
        super(WrappedCallableTask, self).__init__(*args, **kwargs)
        self.wrapped = callable
        if hasattr(callable, '__name__'):
            self.__name__ = self.name = callable.__name__
        if hasattr(callable, '__doc__'):
            self.__doc__ = callable.__doc__

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        return self.wrapped(*args, **kwargs)

    # This allows us to add/retrive arbitrary attributes from the task
    # Won't work until we move task data into it's own context object
    # and revert to normal get/set attr methods in the Task class
    def __getattr__(self, k):
        return getattr(self.wrapped, k)
