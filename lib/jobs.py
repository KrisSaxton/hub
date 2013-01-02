# Core modules
import uuid
# Own modules
import error
from common import State
from tasks import Task
# 3rd part modules
import pika
import json

class Job(Task):

    '''Superclass for all job objects. A job is a collection of tasks.'''

    def __init__(self):
        super(Job, self).__init__()

    def _validate(self):
        '''Ensure job is in a valid format.'''
        if not self.state.tasks:
            raise error.ValidationError('Invalid job envelope')

    def load(self, jobrecord):
        '''Populate a job's state from a job record.'''
        self.state.load(jobrecord)
        if self.state.tasks:
            task_objects = []
            for task in self.state.tasks:
                task_obj = Task(parent_id=self.state.id)
                # Add task values from new task object to those from the jobrecord
                task_obj.state._state = dict(task_obj.state._state.items() + task.items())
                task_objects.append(task_obj)
            self.state.tasks = task_objects
        return self

    def save(self):
        '''Save a job's state as a job record.'''
        task_list = []
        #let's preserve the tasks as we don't want to actually change them
        original_tasks = self.state.tasks
        if self.state.tasks:
            for task in self.state.tasks:
                task_list.append(task.state.__str__())
        self.state.tasks = task_list
        #set the return and then put the tasks back how we found them
        ret = self.state.save()
        self.state.tasks = original_tasks
        return ret

    def check_status(self):
        statuses = []
        for task in self.state.tasks:
            statuses.append(task.state.status)
        if any(status == 'FAILED' for status in statuses):
            return 'FAILED'
        if any(status == 'RUNNING' for status in statuses):
            return 'RUNNING'
        if all(status == 'PENDING' for status in statuses):
            return 'PENDING'
        if all(status == 'SUCCESS' for status in statuses):
            return 'SUCCESS'
        return 'UNKNOWN'

    def set_status(self):
        self.state.status = self.check_status()
        return self
        
    def get_tasks(self, task_name=None):
        '''Get task objects.'''
        if not task_name:
            return self.state.tasks
        for task in self.state.tasks:
            if task.state.name == task_name:
                return task
        return None

    def get_next_tasks_to_run(self):
        '''Examines task statues; returns list of next tasks to run.'''
        tasks_to_run = []
        for task in self.state.tasks:
            if task.state.status in ['SUCCESS', 'FAILED', 'RUNNING']:
                pass
            elif not task.state.depends and task.state.status == 'PENDING':
                print 'Pending task %s has no dependents, publishing...' % task.state.name
                tasks_to_run.append(task)
            else:
                waiting_on_deptask = []
                for task_name in task.state.depends:
                    deptask = self.get_tasks(task_name)
                    if deptask.state.status != 'SUCCESS':
                        waiting_on_deptask.append(deptask.state.name)
                if not waiting_on_deptask:
                    tasks_to_run.append(task)
                else:
                    print 'Task %s waiting on results of %s...' % (task.state.name, waiting_on_deptask)
        return tasks_to_run

    def update_tasks(self, task, force=False):
        '''Update job with new task object.'''
        for i,t in enumerate(self.state.tasks):
            if t.state.id == task.state.id:
                print 'Updating task %s with new results' % t.state.name
                if not force:
                    #prevent updating parent_id, name and args
                    task.state.name = t.state.name
                    task.state.parent_id = t.state.parent_id
                    task.state.args = t.state.args
                self.state.tasks[i] = task
        return self

    def update_task_args(self, task):
        '''Attempt update of parameterised task arguments.'''
        for i,arg in enumerate(task.state.args):
            if str(arg).startswith('_'):
                print 'Found parametrised arg %s in task %s' % (arg, task.state.name)
                task_name, task_key = arg.lstrip('_').split('.')
                source_task = self.get_tasks(task_name)
                value = source_task.state.__getattr__(task_key)
                task.state.args[i] = value
        return task

    def update_output(self):
        '''Attempt update of parameterised job output variables.'''
        for i,var in enumerate(self.state.output):
            if str(var).startswith('_'):
                print 'Found parametrised var %s in job %s' % (var, self.state.name)
                task_name, task_key = var.lstrip('_').split('.')
                source_task = self.get_tasks(task_name)
                #Would be good to check this exists, catch error MMB
                value = source_task.state.__getattr__(task_key)
                self.state.output[i] = value
        return self

    def post_result(self):
        '''Post job results somewhere?'''
        pass
