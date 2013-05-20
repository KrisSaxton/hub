#!/usr/bin/env python
'''
This is the Hub worker which processes tasks on behalf of the dispatcher.
'''
# core modules
import os
import sys
import logging
import zmq

# own modules
import hub.lib.error as error
import hub.lib.config as config
from hub.lib.common import Daemon

# 3rd party modules
import pika
import json


class WorkerDaemon(Daemon):
    '''
    Subclass of Daemon class with run method to launch worker.
    '''
    def run(self, *args):
        self.log = logging.getLogger(__name__)
        (broker, lib_dir) = args
        try:
            Worker(lib_dir).start(broker)
        except Exception, e:
            self.log.error(e)


class Worker():
    '''
    Class representing workers that processes tasks.
    '''
    def __init__(self, tasks_dir):
        '''Load all worker task plugins, connect to messaging system.'''
        self.log = logging.getLogger(__name__)
        self.modules = self._load_task_modules(tasks_dir)

    def _load_task_modules(self, tasks_dir):
        self.tasks_dir = tasks_dir
        os.sys.path.append(self.tasks_dir)
        plugins = []
        modules = []
        # Scan the plugins directory for files ending in .py and import them
        for found_plugin in os.listdir(self.tasks_dir):
            if found_plugin.endswith('.py'):
                plugin_name = found_plugin.rpartition('.py')[0]
                try:
                    self.log.debug('Importing task {0}'.format(
                        plugin_name))
                    modules.append(__import__(plugin_name))
                except Exception, e:
                    self.log.warn('Failed to import task {0}'.format(
                        plugin_name))
                    self.log.error(e)
        self.log.debug('Active modules {0}'.format(modules))
        # TODO load plugins only as they are called?
        return modules

    def _run_task(self, module, record, taskrecord):
        args = []
        kwargs = {}
        if 'args' in record:
            args = record['args']
        task = getattr(module, module.__name__).load(taskrecord)
        self.log.info('Running task: {0}'.format(task.state.name))
        try:
            task.state.data = task(*args, **kwargs)
            if task.async:
                task.state.status = 'RUNNING'
            else:
                task.state.status = 'SUCCESS'
        except Exception, e:
            self.log.error('task {0} failed'.format(task.state.name))
            self.log.error(e)
            task.state.status = 'FAILED'
            task.state.msg = str(e)
        finally:
            self.post_result(task)

    def start(self, broker):
        self.broker = broker
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.connect("tcp://{0}:5560".format(broker))
        while True:
            message = self.socket.recv()
            self.run(message)
            
       

    def run(self, taskrecord):
        '''
        Checks task name for matching module and class,
        instanciates and calls run method with task args
        '''
        self.log.info('Received task: {0}'.format(taskrecord))
        record = json.loads(taskrecord)
        for module in self.modules:
            if module.__name__ == record['name']:
                self._run_task(module, record, taskrecord)
        for module in self.modules:
            if module.__name__ == record['task_name'] and \
                    module.__name__ != record['name']:
                self._run_task(module, record, taskrecord)

    def post_result(self, task):
        '''Post task results into the results queue.'''
        taskrecord = task.save()
        self.socket.send(taskrecord)

if __name__ == '__main__':
    '''
    Run worker directly by executing this module, passing the broker
    hostname/IP and the lib dir for the plugins as arguments.
    '''
    Worker(sys.argv[1], sys.argv[2])
