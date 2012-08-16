#!/usr/bin/env python
# core modules
import uuid
# own modules
import error
from jobs import Job
from tasks import Task
# 3rd party modules
import pika
import json

BROKER='ks-test-02'

class Dispatcher():
    '''Class representing dispatchers that send jobs to workers.'''
    def __init__(self):
        '''Setup connection to broker; listen for incoming jobs and results.'''
        self.registered_jobs = {}
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=BROKER))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='hub_jobs')
        self.channel.queue_declare(queue='hub_results')
        self.channel.queue_declare(queue='hub_status')
        self.channel.basic_consume(self.process_jobs, queue='hub_jobs', no_ack=True)
        self.channel.basic_consume(self.process_results, queue='hub_results', no_ack=True)
        self.channel.basic_consume(self.get_job, queue='hub_status', no_ack=True)
        print 'Starting dispatcher, listening for jobs and results...'
        self.channel.start_consuming() 

    def _register_job(self, job):
        '''Register job with dispatcher.'''
        self.registered_jobs[job.state.id] = job
        return self.registered_jobs

    def get_job(self, ch, method, properties, jobid):
        '''Work out dependancies and order.'''
        # Load the jobid from the JSON object
        jobid = json.loads(jobid)
        print 'Received status request for job with id: %s' % jobid
        # Get the job from the store of registered jobs
        job = self.registered_jobs[jobid]
        # Return job to client
        self.channel.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id = \
                                                        properties.correlation_id),
                        body=str(job.save()))

    def process_jobs(self, ch, method, properties, jobrecord):
        '''Work out dependancies and order.'''
        # Create a Job instance from the job record
        job = Job().load(jobrecord)
        # Register the job with the dispatcher
        self._register_job(job)
        # Return registration success message to client
        self.channel.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id = \
                                                        properties.correlation_id),
                        body=str(job.state.id))
        # Work out the first tasks to run
        print 'Received job: %s' % job.state
        print 'Decomposing job; calculating first tasks to run'
        tasks_to_run = job.get_next_tasks_to_run()
        for task in tasks_to_run:
            self.publish_task(task.state.save())

    def publish_task(self, task):
        '''Publish tasks to the work queue.'''
        print 'Publishing the following task to the work queue %s:' % task
        self.channel.basic_publish(exchange='',
                      routing_key='hub_run',
                      properties=pika.BasicProperties(
                      content_type='application/json',),
                      body=task)

    def process_results(self, ch, method, properties, taskrecord):
        '''Processing results received from workers and end points.'''
        print 'Received task results for job with id: %s' % properties.correlation_id
        # Check if task is registered to this dispatcher
        if properties.correlation_id in self.registered_jobs:
            print 'Task results %r' % taskrecord
            # Turn the taskrecord into a project Task instance
            task = Task().load(taskrecord)
            # Get the related Job for this task
            job = self.registered_jobs[task.state.parent_id]
            # Update the job with the new task results
            job.update_tasks(task)
            
            # Now work out what's next
            tasks_to_run = job.get_next_tasks_to_run()
            if not tasks_to_run:
                # We're done, calculate overall job status and exit
                job.update_output()
                print 'No more tasks to run for job: %s' % task.state.parent_id
                print 'Job %s completed with status: %s and output %s' % \
                        (job.state.id, job.get_status(), job.state.output)
                
            for task in tasks_to_run:
                # Substitute tagged inputs with the associated results of completed tasks
                task = job.update_task_args(task)
                self.publish_task(task.state.save())
        else:
            print 'Discarding task results for unregistered job id: %s' % properties.correlation_id

if __name__ == '__main__':
    dispatcher = Dispatcher()
