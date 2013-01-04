#!/usr/bin/env python
# core modules
import os
# own modules
import hub.lib.config as config
import hub.lib.error as error
# 3rd party modules
import pika
import json

config_file = '/usr/local/pkg/hub/etc/hub.conf'

try:
    conf = config.setup(config_file)
except error.ConfigError, e:
    print e.msg
    raise e

class Worker():
    '''Class representing workers that process jobs.'''
    def __init__(self):
        '''Load all job plugins'''
        self.conf = config.setup()
        self.broker = conf.get('HUB', 'broker')
        self.libdir = conf.get('HUB', 'libdir')
        plugin_dir = self.libdir + '/hub/tasks' 
        os.sys.path.append(plugin_dir)
        plugins = []
        # Scan the plugins directory for files ending in .py and import them
        for found_plugin in os.listdir(plugin_dir):
            if found_plugin.endswith('.py'):
                plugin_name = found_plugin.rpartition('.py')[0]
                plugins.append(plugin_name)
        self.modules = map(__import__, plugins) # Could load plugins as they are called with imp?
        
        # Setup connection to broker and declare the work queue
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.broker))
        channel = connection.channel()
        channel.queue_declare(queue='hub_run')
        channel.basic_consume(self.run, queue='hub_run', no_ack=True)
        print 'Starting worker, waiting for jobs...'
        channel.start_consuming()

    def _run_task(self, module, record, taskrecord):
        args = []
        kwargs = {}
        if 'args' in record:
            args = record['args']
        task = getattr(module, module.__name__).load(taskrecord)
        print 'Running task: %s' % task.state.name
        try:
            task.state.data = task(*args, **kwargs)                  
            #task(*args, **kwargs)
            if task.async:
                task.state.status = 'RUNNING'
            else:
                task.state.status = 'SUCCESS'
        except Exception, e:
            task.state.status = 'FAILED'
            task.state.msg = str(e)
        finally:
            self.post_result(task)
        

    def run(self, ch, method, properties, taskrecord):
        '''Checks task name for matching module and class, 
        instanciates and calls run method with task args'''
        print "Received task: %r" % (taskrecord,)
        record = json.loads(taskrecord)
        for module in self.modules:
            if module.__name__ == record['name']:
                self._run_task(module, record, taskrecord)
        for module in self.modules:
            if module.__name__ == record['task_name'] and module.__name__ != record['name']:
                self._run_task(module, record, taskrecord)

    def post_result(self, task):
        '''Post task results into the results queue.'''
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.broker))
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
