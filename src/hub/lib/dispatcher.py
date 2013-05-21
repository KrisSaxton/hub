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
import zmq

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
        self.ct_lock = threading.Lock()
        
        #threading.Thread(target=self._caretaker).start()
        #self._caretaker()

    def _caretaker(self):
        self.log.info("Caretaker waiting on lock...")
        #TODO: Make this check the task that triggered it first, then cleanup
        self.ct_lock.acquire()
        self.log.info("Caretaker Running...")
        dbI=self.db(self.databaseHost,self.databasePort,self.databaseInstance)
        incomplete = dbI.getincompletetasks()
        for task_id in incomplete:
            jobid = dbI.getjobid(task_id)
            jobrecord = dbI.getjob(jobid)
            #TODO get task record not job record
            job = Job().load(jobrecord)
            for task in job.state.tasks:
                if task.state.id == task_id and task.state.timeout and task.state.start_time:
                    if task.state.timeout < (time.time() - task.state.start_time):
                        self.log.info("Setting task {0} from job {1} as FAILED".format(task.state.id,job.state.id))
                        task.state.status = 'FAILED'
                        task.state.end_time = time.time()
                        job.state.status = 'FAILED'
                        job.state.end_time = time.time()                        
                        job.save()
                        dbI.updatejob(job)
        self.ct_lock.release()            

    def _persist_job(self, job):
        
        self.db(self.databaseHost,self.databasePort,self.databaseInstance).putjob(job)
        
    def _update_job(self, job):
        self.db(self.databaseHost,self.databasePort,self.databaseInstance).updatejob(job)
        
    def _retreive_job(self, job_id):
        
        job = self.db(self.databaseHost,self.databasePort,self.databaseInstance).getjob(job_id)
        return job
    
    def _retreive_jobid(self, task_id):
        jobid = self.db(self.databaseHost,self.databasePort,self.databaseInstance).getjobid(task_id)
        return jobid

    def start(self, broker):
        self.log.info('Starting dispatcher, listening for jobs and results...')
        self.context = zmq.Context()
        self.status = self.context.socket(zmq.ROUTER)
        self.job_queue = self.context.socket(zmq.ROUTER)
        self.task_q = self.context.socket(zmq.DEALER)
        self.backend = self.context.socket(zmq.ROUTER)
        self.status.bind("tcp://*:5559")
        self.job_queue.bind("tcp://*:5560")
        self.backend.bind("tcp://*:5561")
        self.task_q.setsockopt(zmq.IDENTITY, "TASK_Q")
        self.task_q.connect("tcp://localhost:5560")
        
        # Initialize poll set
        self.poller = zmq.Poller()
        self.poller.register(self.status, zmq.POLLIN)
        self.poller.register(self.job_queue, zmq.POLLIN)
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.task_q, zmq.POLLIN)
        
        # Switch messages between sockets
        msgs = []
        workers = 0
        workers_addr = []
        while True:
            self.socks = dict(self.poller.poll())
            if self.socks.get(self.job_queue) == zmq.POLLIN:
                message = self.job_queue.recv()
                more = self.job_queue.getsockopt(zmq.RCVMORE)
                if more:
                    msgs.append(message)
                else:
                    incoming = json.loads(message)
                    to_reply = []
                    to_publish = []

#                        if incoming['key'] == 'status':
#                            #Do something to find the job resulting in...
#                            jobid = incoming['data']['id']
#                            job = self.get_job(jobid)
#                            to_reply = [job]
                        #self.frontend.send(job)
                    if incoming['key'] == 'task_update':
                        #Do something to find the job resulting in...
                        task = incoming['data']
                        to_publish = self.process_results(json.dumps(task), fromWorker=False)
                    elif incoming['key'] == 'task_result':
                        #Do something to find the job resulting in...
                        task = incoming['data']
                        to_publish = self.process_results(json.dumps(task), fromWorker=True)
                    elif incoming['key'] == 'job':
                        #Do something with the job resulting in...
                        job = incoming['data']
                        result = self.process_jobs(json.dumps(job))
                        to_publish = result[0]
                        to_reply = [result[1]]
                    for reply in to_reply:
                        for msg in msgs:
                            self.job_queue.send(msg, zmq.SNDMORE)   
                        self.job_queue.send(reply)
                                             
                    for publish in to_publish:
                        self.job_queue.send("TASK_Q",zmq.SNDMORE)
                        self.job_queue.send("",zmq.SNDMORE)
                        self.job_queue.send(publish)
                        
                    msgs = []
                        
            if workers > 0:
                if self.socks.get(self.task_q) == zmq.POLLIN:
                    blank = self.task_q.recv()
                    message = self.task_q.recv()
                    workers -= 1
                    work_addr = workers_addr.pop()
                    self.log.info("Task {0} is being sent to worker {1}".format(message, work_addr))
                    self.backend.send(work_addr, zmq.SNDMORE)
                    self.backend.send("", zmq.SNDMORE)
                    self.backend.send(message)

            if self.socks.get(self.backend) == zmq.POLLIN:
                addr = self.backend.recv()                    
                empty = self.backend.recv()
                message = self.backend.recv()
                if message == "READY":
                    self.log.info("Worker {0} is READY".format(addr))
                    workers+=1
                    workers_addr.append(addr)


            if self.socks.get(self.status) == zmq.POLLIN:
                message = self.status.recv()
                more = self.status.getsockopt(zmq.RCVMORE)
                if more:
                    msgs.append(message)
                else:
                    incoming = json.loads(message)
                    to_reply = []
                    if incoming['key'] == 'status':
                        #Do something to find the job resulting in...
                        jobid = incoming['data']['id']
                        job = self.get_job(jobid)
                        to_reply = [job]
                        for reply in to_reply:
                            for msg in msgs:
                                self.status.send(msg, zmq.SNDMORE)   
                            self.status.send(reply)
                    msgs = []                  
#        self.broker = broker
#        try:
#            self.conn = pika.BlockingConnection(pika.ConnectionParameters(
#                                                host=self.broker))
#            self.channel = self.conn.channel()
#            self.channel.queue_declare(queue='hub_jobs')
#            self.channel.queue_declare(queue='hub_status')
#            self.channel.queue_declare(queue='hub_results')
#            self.channel.basic_consume(self.get_job,
#                                       queue='hub_status', no_ack=True)
#            self.channel.basic_consume(self.process_results,
#                                       queue='hub_results', no_ack=True)
#            self.channel.basic_consume(self.process_jobs,
#                                       queue='hub_jobs', no_ack=True)
#
#            self.log.info(
#                'Starting dispatcher, listening for jobs and results...')
#            self.channel.start_consuming()
#        except pika.exceptions.AMQPConnectionError, e:
#            self.log.exception(e)
#            msg = ('Problem connectting to broker {0}'.format(self.broker))
#            self.log.error(msg)
#            raise error.MessagingError(msg, e)


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
            self.log.debug('Updating job {0} in DB'.format(
                job.state.name))
            self._update_job(job)
            # DONE Will activate this once DB persistence layer exists
            self.log.info('Job {0} completed. Status: {1}, Output: {2}'.format(
                          job.state.id, job.state.status, job.state.output))
            self.backend.send("None")
        ret = []
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
            ret.append(task.state.save())
            #Now we've decided what to do NEXT with the Job lets update the DB
            self.log.debug("Updating to DB job: ".format(job.state.id))
            self._update_job(job)
        return ret
        

    def get_job(self, jobid):
        '''
        Work out dependancies and order
        '''
        self.log.info('Received status request for job {0}'.format(jobid))
#        jobid = json.loads(jobid)
        # Get the job from the store of registered jobs
        jobs = dict()
        if jobid == 'all':
            for id, job in self.registered_jobs.iteritems():
                jobs[id] = str(job.save())
            msg = str(jobs)
        else:
            job = self._retreive_job(jobid)
            if job is not None:
                msg = job
            else:
                msg = str('Job %s not found' % jobid)
        # Return job to client
        return msg

    def process_jobs(self, jobrecord):
        '''
        Work out dependancies and order
        '''
        ret = []
        # Create a Job instance from the job record
        job = Job().load(jobrecord)
        # Register the job with the dispatcher
        #self._register_job(job)
        self._persist_job(job)
        self.log.info('Registered job: {0} in DB'.format(job.state.id))



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
            ret.append(task.state.save())
        #Now we've decided what to do with Job lets update the DB
        self.log.debug("Updating to DB job: {0}".format(job.state.id))
        self._update_job(job)
        return (ret, json.dumps(job.state.id))

#    def publish_task(self, task):
#        '''
#        Publish tasks to the work queue
#        '''
#        self.log.info('Publishing task {0} to the work queue'.format(task))
#        self.backend.send(task)

    def process_results(self, taskrecord, fromWorker=False):
        '''
        Processing results received from workers and end points
        '''
        started_tasks = []
        # Check if task is registered to this dispatcher
        if fromWorker:
            jobid = json.loads(taskrecord)['parent_id']
            self.log.info(
            'Received task results for job {0}'.format(
                jobid))
            jobrecord = self._retreive_job(jobid)
            if jobrecord is None:
                self.log.warn('No parent job found with id {0}'.format(
                                      jobid))
            else:
                job = Job().load(jobrecord)
                self.log.info('Found job in DB: {0}'.format(job.state.id))
                self.log.info('Task results: {0}'.format(taskrecord))
                # Turn the taskrecord into a project Task instance
                updated_task = Task().load(taskrecord)            
                # Update the job with the new task results
                job.update_tasks(updated_task, force=True)
                started_tasks = self._start_next_task(job)
        else:
            self.log.info('Task results: {0}'.format(taskrecord))
            # Turn the taskrecord into a project Task instance
            updated_task = Task().load(taskrecord)
            number_of_updated_tasks = 0
            jobid = self._retreive_jobid(updated_task.state.id)
            jobrecord = self._retreive_job(jobid)
            if jobrecord is not None:
                job = Job().load(jobrecord)
                self.log.info('Found in DB job: {0}'.format(job.state.id))
                for task in job.state.tasks:
                    if updated_task.state.id == task.state.id:
                        job.update_tasks(updated_task)
                        number_of_updated_tasks += 1
                        started_tasks = self._start_next_task(job)
                if number_of_updated_tasks == 0:
                    self.log.warn('Task with id {0} not found in its parent job (possible?)'.format(
                                  updated_task.state.id))
            else:
                self.log.warn('No parent job found for Task with id {0}'.format(
                                      updated_task.state.id))
       
        return started_tasks

if __name__ == '__main__':
    '''
    Run dispatcher directly by executing this module, passing the broker
    hostname/IP as the only argument.
    '''
    try:
        Dispatcher().start(sys.argv[1])
    except Exception, e:
        print(e)
