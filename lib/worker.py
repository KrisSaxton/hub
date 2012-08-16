#!/usr/bin/env python
#core modules
import os
# own modules
import error
from tasks import Task
# 3rd party modules
import pika
import json

LIBDIR = '/usr/local/share/plugins'

class Worker():
    '''Class representing workers that process jobs.'''
    def __init__(self):
        '''Load all job plugins'''
        plugin_dir = LIBDIR + '/hub/jobs' 
        os.sys.path.append(plugin_dir)
        plugins = []
        # Scan the plugins directory for files ending in .py and import them
        for found_plugin in os.listdir(plugin_dir):
            if found_plugin.endswith('.py'):
                plugin_name = found_plugin.rpartition('.py')[0]
                plugins.append(plugin_name)
        self.modules = map(__import__, plugins) # Could load plugins as they are called with imp?
        
        # Setup connection to broker and declare the work queue
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='ks-test-02'))
        channel = connection.channel()
        channel.queue_declare(queue='hub_run')
        channel.basic_consume(self.run, queue='hub_run', no_ack=True)
        print 'Starting worker, waiting for jobs...'
        channel.start_consuming()

    def run(self, ch, method, properties, taskrecord):
        '''Checks task name for matching module and class, 
        instanciates and calls run method with task args'''
        print "Received task: %r" % (taskrecord,)
        task_dict = json.loads(taskrecord)
        for module in self.modules:
            if module.__name__ == task_dict['name']:
                args = []
                kwargs = {}
                if 'args' in task_dict:
                    args = task_dict['args']
                task = getattr(module, module.__name__).load(taskrecord)
                print 'Running task: %s' % task.state.name
                taskdata = Task().load(taskrecord)
                try:
                    taskdata.state.data = task(*args, **kwargs)
                    if task.async:
                        taskdata.state.status = 'RUNNING'
                    else:
                        taskdata.state.status = 'SUCCESS'
                except:
                    taskdata.state.status = 'FAILED'
                    raise
                self.post_result(taskdata)

    def post_result(self, task):
        '''Post task results into the results queue.'''
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='ks-test-02'))
        channel = connection.channel()
        print 'Sending task results to dispatcher with job id: %s' % task.state.parent_id
        print task.state
        channel.basic_publish(exchange='',
                      routing_key='hub_results',
                      properties=pika.BasicProperties(
                      correlation_id = str(task.state.parent_id),
                      content_type='application/json',),
                      body=task.state.save())

if __name__ == '__main__':
        worker = Worker()
