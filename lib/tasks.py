# Core modules
import uuid
# Own modules
import error
from common import State
# 3rd part modules
import pika
import json

#class TaskState(State):
#
#    '''Class representing a task's state.  Essentially a dictionary which can be easily
#       de/serialised for passing through the messaging network.'''
#
#    def __init__(self):
#        super(TaskState, self).__init__()

class Task(object):

    '''Superclass for all Task objects. Tasks are defined units of work.'''

    def __init__(self, state=None, parent_id=None, async=False):
        if not state:
            state = State()
        self.state = state
        if parent_id is not None:
            self.state.parent_id = parent_id
        self.async = async
        self._set_id()

    def _validate(self):
        '''Ensure task is in a valid format.'''
        if not self.name:
            raise error.ValidationError('Invalid task format')

    def _set_id(self):
        '''Generate a job id. uuid1 includes a hardware address component
        so will be unique across dispatchers.'''
        if not self.state.id:
            self.state.id = uuid.uuid1().__str__()
        return self # Do I need that?

    def _set_parent_id(self, parent_id):
        self.state.parent_id = parent_id
        return self

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
        raise error.MethodNotImplemented('All tasks must implement the run method')

    def post_result(self):
        '''Post task results into the results queue.'''
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='ks-test-02'))
        channel = connection.channel()
        print 'Sending results to dispatcher with id: %s' % self.state.parent_id
        print self._state
        channel.basic_publish(exchange='',
                      routing_key='hub_results',
                      properties=pika.BasicProperties(
                      correlation_id = str(self.state.parent_id),
                      content_type='application/json',),
                      body=self.state.save())

class WrappedCallableTask(Task):
    """
    Wraps a given callable transparently, while marking it as a valid Task.
    Generally used via @task decorator and not directly.

    """
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
