#!/usr/bin/env python
'''
This is the Hub dispatcher which performs job management functions
'''
# core modules
import sys
import uuid
import logging
import traceback

# own modules
import hub.lib.error as error
import hub.lib.config as config
from hub.lib.jobs import Job
from hub.lib.tasks import Task
from hub.lib.common import Daemon

# 3rd party modules
import pika
import json


class DispatcherDaemon(Daemon):
    '''
    Subclass of Daemon class with run method to launch dispatcher
    '''
    def run(self, *args):
        self.log = logging.getLogger(__name__)
        broker = args[0]
        try:
            Dispatcher().start(broker)
        except Exception, e:
            self.log.exception(e)


class Dispatcher():
    '''
    Class representing dispatcher that performs job management functions
    '''
    def __init__(self):
        '''
        Setup connection to broker and listen for incoming jobs and results
        '''
        self.log = logging.getLogger(__name__)
        self.registered_jobs = {}

    def start(self, broker):
        self.broker = broker
        try:
            self.conn = pika.BlockingConnection(pika.ConnectionParameters(
                                                host=self.broker))
            self.channel = self.conn.channel()
            self.channel.queue_declare(queue='hub_jobs')
            self.channel.queue_declare(queue='hub_status')
            self.channel.queue_declare(queue='hub_results')
            self.channel.basic_consume(self.process_jobs,
                                       queue='hub_jobs', no_ack=True)
            self.channel.basic_consume(self.process_results,
                                       queue='hub_results', no_ack=True)
            self.channel.basic_consume(self.get_job,
                                       queue='hub_status', no_ack=True)
            self.log.info(
                'Starting dispatcher, listening for jobs and results...')
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError, e:
            self.log.exception(e)
            msg = ('Problem connectting to broker {0}'.format(self.broker))
            self.log.error(msg)
            raise error.MessagingError(msg, e)

    def _register_job(self, job):
        '''
        Register job with dispatcher
        '''
        self.registered_jobs[job.state.id] = job
        return self.registered_jobs

    def _deregister_job(self, job):
        '''
        Register job with dispatcher
        '''
        self.registered_jobs.pop(job.state.id)

    def _start_next_task(self, job):
        tasks_to_run = job.get_next_tasks_to_run()
        if len(tasks_to_run) == 0:
            # We're done, calculate overall job status and exit
            job.set_status()
            if job.state.status == 'SUCCESS':
                job.update_output()
            self.log.info('No more tasks to run for job {0}'.format(
                job.state.name))
            self.log.debug('Persisting job {0} to DB and deregistering'.format(
                job.state.name))
            # Will activate this once DB persistence layer exists
            #self._deregister_job(job)
            self.log.info('Job {0} completed. Status: {1}, Output: {2}'.format(
                          job.state.id, job.state.status, job.state.output))

        for task in tasks_to_run:
            # Sub tagged inputs with the associated results of completed tasks
            if task.state.status != 'RUNNING' and task.state.args is not None:
                task = job.update_task_args(task)
            task.state.status = 'SUBMITTED'
            self.publish_task(task.state.save())

    def get_job(self, ch, method, properties, jobid):
        '''
        Work out dependancies and order
        '''
        # Load the jobid from the JSON object
        self.log.info('Received status request for job {0}'.format(jobid))
        jobid = json.loads(jobid)
        # Get the job from the store of registered jobs
        jobs = dict()
        if jobid == 'all':
            for id, job in self.registered_jobs.iteritems():
                jobs[id] = str(job.save())
            msg = str(jobs)
        else:
            try:
                job = self.registered_jobs[jobid]
                msg = str(job.save())
            except KeyError:
                msg = 'Job %s not found' % jobid
        # Return job to client
        self.log.info('Returning msg {0}'.format(msg))
        _prop = pika.BasicProperties(correlation_id=properties.correlation_id)
        self.channel.basic_publish(exchange='',
                                   routing_key=properties.reply_to,
                                   properties=_prop,
                                   body=msg)

    def process_jobs(self, ch, method, properties, jobrecord):
        '''
        Work out dependancies and order
        '''
        # Create a Job instance from the job record
        job = Job().load(jobrecord)
        # Register the job with the dispatcher
        self._register_job(job)
        self.log.info('Registered job: {0}'.format(job.state.id))
        # Return registration success message to client
        _prop = pika.BasicProperties(correlation_id=properties.correlation_id)
        self.channel.basic_publish(exchange='',
                                   routing_key=properties.reply_to,
                                   properties=_prop,
                                   body=str(job.state.id))

        # Work out the first tasks to run
        self.log.debug('Decomposing job; calculating first tasks to run')
        tasks_to_run = job.get_next_tasks_to_run()
        #This was added to fill out any id args in tasks right at the beginning
        for task in tasks_to_run:
            if task.state.args is not None:
                task = job.update_task_args(task)
        for task in tasks_to_run:
            task.state.status = 'SUBMITTED'
            self.publish_task(task.state.save())

    def publish_task(self, task):
        '''
        Publish tasks to the work queue
        '''
        self.log.info('Publishing task {0} to the work queue'.format(task))
        _prop = pika.BasicProperties(content_type='application/json',)
        self.channel.basic_publish(exchange='',
                                   routing_key='hub_tasks',
                                   properties=_prop,
                                   body=task)

    def process_results(self, ch, method, properties, taskrecord):
        '''
        Processing results received from workers and end points
        '''
        self.log.info(
            'Received task results for job {0}'.format(
                properties.correlation_id))
        # Check if task is registered to this dispatcher
        if properties.correlation_id in self.registered_jobs:
            self.log.info('Task results: {0}'.format(taskrecord))
            # Turn the taskrecord into a project Task instance
            updated_task = Task().load(taskrecord)
            # Get the related Job for this task
            job = self.registered_jobs[updated_task.state.parent_id]
            # Update the job with the new task results
            job.update_tasks(updated_task, force=True)
            self._start_next_task(job)
        elif properties.correlation_id == 'update_task':
            self.log.info('Task results: {0}'.format(taskrecord))
            # Turn the taskrecord into a project Task instance
            updated_task = Task().load(taskrecord)
            number_of_updated_tasks = 0
            for job_id in self.registered_jobs:
                job = self.registered_jobs[job_id]
                for task in job.state.tasks:
                    if updated_task.state.id == task.state.id:
                        job.update_tasks(updated_task)
                        number_of_updated_tasks += 1
                        self._start_next_task(job)
            if number_of_updated_tasks == 0:
                self.log.warn('Task with id {0} not found in any job'.format(
                              updated_task.state.id))
        else:
            self.log.warn('Discarding task results for unknown job {0}'.format(
                          properties.correlation_id))

if __name__ == '__main__':
    '''
    Run dispatcher directly by executing this module, passing the broker
    hostname/IP as the only argument.
    '''
    try:
        Dispatcher().start(sys.argv[1])
    except Exception, e:
        print(e)
