'''
This is the Hub client which submits, updates, queries and deletes jobs
'''
import sys
import json
import uuid
import logging
import zmq
import hub.lib.error as error


class Client(object):
    '''
    Class representing things that can submit and query jobs.
    '''
    def __init__(self, broker):
        self.broker = broker
        self.log = logging.getLogger(__name__)
        self.context = zmq.Context()
        self.status = self.context.socket(zmq.REQ)
        self.status.connect("tcp://{0}:5559".format(broker))
        self.job_q = self.context.socket(zmq.REQ)
        self.job_q.connect("tcp://{0}:5560".format(broker))        


    def _post(self, jobid, request_type, blocking=True, taskdata=None,
              job=None):
        '''
        Send job to messaging system
        '''
        self.response = None
        if request_type is 'create':
            self.routing_key = 'job'
            job = json.loads(job)
            req = {'key':'job', 'data': job}
            self.body = json.dumps(req)
            self.job_q.send(self.body)
            if blocking is True:
                self.response = self.job_q.recv()
            return str(self.response)
        elif request_type is 'update':
            self.routing_key = 'task_update'
            taskdata = json.loads(taskdata)
            req = {'key':'task_update', 'data': taskdata}
            self.body = json.dumps(req)
            self.job_q.send(self.body)
        elif request_type is 'get':
            self.routing_key = 'status'
            req = {'key':'status', 'data': {'id': jobid}}
            self.body = json.dumps(req)
            self.status.send(self.body)



#        self.socket.send(self.body)
        
            if blocking is True:
                self.response = self.status.recv()
            return str(self.response)

    def create(self, job):
        '''
        Posts a new job
        '''
        self.log.info('Submitting new job to queue')
        res = self._post(None, 'create', blocking=True, job=job)
        return res

    def update(self, taskdata):
        '''
        Update a job
        '''
        self.log.info('Submitting task results to queue')
        res = self._post('update_task', 'update', blocking=False,
                         taskdata=taskdata)
        return res

    def get(self, jobid=None):
        '''
        Get status on a current job
        ''' 
        if jobid is None:
            jobid = 'all'  # Keyword recoginised by dispatcher
            self.log.info('Requesting status for all jobs')
        else:
            self.log.info('Requesting status for job {0}'.format(jobid))
        res = self._post(jobid, 'get', blocking=True)
        return res
