#!/usr/bin/env python
'''
This is the Hub worker which processes tasks on behalf of the dispatcher.
'''
# core modules
import os
import sys
import logging
import zmq
import time

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
        (broker, lib_dir, id) = args
        try:
            Worker(lib_dir, id).start(broker)
        except Exception, e:
            self.log.error(e)


class Worker():
    '''
    Class representing workers that processes tasks.
    '''
    def __init__(self, tasks_dir, id):
        '''Load all worker task plugins, connect to messaging system.'''
        self.log = logging.getLogger(__name__)
        self.modules = self._load_task_modules(tasks_dir)
        self.id = id

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
        #reload the module, otherwise we get weird cross-population
        reload(module)
        task = getattr(module, module.__name__).load(taskrecord)
        self.log.info('Running task: {0}'.format(task.state.name))
        task.state.status = 'RUNNING'
        self.post_result(task)
        try:
            task.state.data = task(*args, **kwargs)
        except Exception, e:
            self.log.error('task {0} failed'.format(task.state.name))
            self.log.error(e)
            task.state.status = 'FAILED'
            task.state.msg = str(e)
        finally:
            if not task.async:
                task.state.status = 'SUCCESS'
                self.post_result(task)

    def start(self, broker):
        self.broker = broker
        self.context = zmq.Context()
        self.jobs = self.context.socket(zmq.SUB)
        self.return_queue = self.context.socket(zmq.DEALER)
        self.jobs.connect("tcp://{0}:5561".format(broker))
        self.return_queue.connect("tcp://{0}:5560".format(broker))
        self.jobs.setsockopt(zmq.SUBSCRIBE, self.id)
        self.jobs.setsockopt(zmq.SUBSCRIBE, "CALL_HOME")
#        time.sleep(10)
        self.log.info("Announcing READY")
        data = {'key':'announce', 'data':str(self.id)}
        self.log.info(json.dumps(data))
        self.return_queue.send(json.dumps(data))
        self.log.info("MMB")
        #self.jobs.send("READY")
        while True:
            [addr, request] = self.jobs.recv_multipart()
            self.log.info(addr)
            self.log.info(request)
            if request == "DISPATCHER_STARTED":
                # the dispatcher has started we better
                # remind it who we are...
                self.log.info("Dispatcher restarted so announcing READY")
                data = {'key':'announce', 'data':self.id}
                self.return_queue.send(json.dumps(data))
            else:
                self.run(request)
                self.log.info("Announcing READY")
                data = {'key':'announce', 'data':self.id}
                self.return_queue.send(json.dumps(data))            
       

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
        data = {'key':'task_result', 'data':json.loads(taskrecord)}
        res = json.dumps(data)
        self.return_queue.send(res)

if __name__ == '__main__':
    '''
    Run worker directly by executing this module, passing the broker
    hostname/IP and the lib dir for the plugins as arguments.
    '''
    Worker(sys.argv[1], sys.argv[2])
