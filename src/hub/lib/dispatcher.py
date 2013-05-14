#!/usr/bin/env python
'''
This is the Hub dispatcher which performs job management functions
'''
# core modules
import sys
import uuid
import logging
import traceback
import time
import threading

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
        # Setup config
        try:
            self.conf = config.setup('/usr/local/pkg/hub/etc/dispatcher.conf')
        except error.ConfigError, e:
            print e.msg
            raise e
        self.databaseType = self.conf.get('DATABASE','type')
        self.databaseHost = self.conf.get('DATABASE','host')
        self.databasePort = self.conf.get('DATABASE','port')
        self.databaseInstance = self.conf.get('DATABASE','instance')
        
        self.databaseModule = __import__('hub.lib.database',fromlist = [self.databaseType])
        self.db = getattr(self.databaseModule, self.databaseType)
        
        #threading.Thread(target=self._caretaker).start()
        #self._caretaker()

    def _caretaker(self):
        self.log.info("Caretaker waiting on lock...")
        lock = threading.Lock().acquire()
        self.log.info("Caretaker Running...")
        dbI=self.db(self.databaseHost,self.databasePort,self.databaseInstance)
        incomplete = dbI.getincompletetasks()
        #self.log.info(str(incomplete))
        for task_id in incomplete:
            jobid = dbI.getjobid(task_id)
            jobrecord = dbI.getjob(jobid)
            #TODO get task record not job record
            job = Job().load(jobrecord)
            self.log.info(str(job.state.tasks))
            for task in job.state.tasks:
                if task.state.id == task_id and task.state.timeout and task.state.start_time:
                    if task.state.timeout < (time.time() - task.state.start_time):
                        self.log.info("Setting task {0} from job {1} as FAILED".format(task.state.id,job.state.id))
                        task.state.status = 'FAILED'
                        task.state.end_time = time.time()
                        job.state.status = 'FAILED'
                        job.state.end_time = time.time()                        
                        job.save()
                        dbI.putjob(job)
        lock.release()            

    def _persist_job(self, job):
        
        self.db(self.databaseHost,self.databasePort,self.databaseInstance).putjob(job)
        
    def _retreive_job(self, job_id):
        
        job = self.db(self.databaseHost,self.databasePort,self.databaseInstance).getjob(job_id)
        return job
    
    def _retreive_jobid(self, task_id):
        jobid = self.db(self.databaseHost,self.databasePort,self.databaseInstance).getjobid(task_id)
        return jobid

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
                job.state.end_time = time.time()
                job.update_output()
            self.log.info('No more tasks to run for job {0}'.format(
                job.state.name))
            self.log.debug('Persisting job {0} to DB and deregistering'.format(
                job.state.name))
            self._persist_job(job)
            # DONE Will activate this once DB persistence layer exists
            self._deregister_job(job)
            self.log.info('Job {0} completed. Status: {1}, Output: {2}'.format(
                          job.state.id, job.state.status, job.state.output))

        for task in tasks_to_run:
            # Sub tagged inputs with the associated results of completed tasks
            if task.state.status != 'RUNNING' and task.state.args is not None:
                task = job.update_task_args(task)
            task.state.status = 'SUBMITTED'
            if not task.state.start_time:
                task.state.start_time = time.time()
            if task.state.timeout:
                self.log.debug("Task {0} timeout in {1}".format(task.state.id,str(task.state.timeout)))
                threading.Timer(task.state.timeout, self._caretaker).start()
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
                #so let's check the database
                job = self._retreive_job(jobid)
                if job is not None:
                    msg = job
                else:
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
            if not task.state.start_time:
                task.state.start_time = time.time()
            if task.state.timeout:
                self.log.debug("Task {0} timeout in {1}".format(task.state.id,str(task.state.timeout)))
                threading.Timer(task.state.timeout, self._caretaker).start()
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
        elif self._retreive_job(properties.correlation_id) is not None:
            # Re-Register the job with the dispatcher
            jobrecord = self._retreive_job(properties.correlation_id)
            job = Job().load(jobrecord)
            self._register_job(job)
            self.log.info('Found in DB so Re-Registered job: {0}'.format(job.state.id))
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
                self.log.warn('Task with id {0} not found in any registered job now check DB'.format(
                              updated_task.state.id))
                jobid = self._retreive_jobid(updated_task.state.id)
                jobrecord = self._retreive_job(jobid)
                if jobrecord is not None:
                    job = Job().load(jobrecord)
                    # Re-Register the job with the dispatcher
                    self._register_job(job)
                    self.log.info('Found in DB so Re-Registered job: {0}'.format(job.state.id))
                    for task in job.state.tasks:
                        if updated_task.state.id == task.state.id:
                            job.update_tasks(updated_task)
                            number_of_updated_tasks += 1
                            self._start_next_task(job)
                    if number_of_updated_tasks == 0:
                        self.log.warn('Task with id {0} not found in ANY job'.format(
                                      updated_task.state.id))
                        self._deregister_job(job)
                else:
                    self.log.warn('Task with id {0} not found in ANY job'.format(
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
