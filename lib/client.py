#!/usr/bin/env python
import sys
import pika
import json
import uuid
import hub.lib.config as config
import hub.lib.error as error

class Client(object):
    '''
    Class representing things that can submit and query jobs.
    '''

    def __init__(self, config_file):
        try:
            conf = config.setup(config_file)
        except error.ConfigError, e:
            print e.msg
            raise e
        self.conf = config.setup()
        self.broker = conf.get('HUB', 'broker')
        self.conn = pika.BlockingConnection(
                        pika.ConnectionParameters(host=self.broker))
        self.channel = self.conn.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
                        self.on_response,
                        no_ack=True,
                        queue=self.callback_queue)

    def on_response(self, channel, method, properties, body):
        if self.corr_id == properties.correlation_id:
            self.response = body    

    def _post(self, jobid, request_type, blocking=True, taskdata=None, job=None):
        '''Used by create get update to send request to middleware'''
        #Are we doing create, update, get?        
        if request_type is 'create':
            self.routing_key='hub_jobs'
            self.body = job
        elif request_type is 'update':
            self.routing_key='hub_results'
            self.body = json.dumps(taskdata)
        else:
            self.routing_key='hub_status'
            self.body = json.dumps(jobid)
        self.response = None
        if request_type is 'update':
            self.corr_id = str(jobid)
        else:
            self.corr_id = str(uuid.uuid4())
        _prop = pika.BasicProperties(content_type='application/json',
                                     reply_to = self.callback_queue,
                                     correlation_id = self.corr_id)
        self.channel.basic_publish(exchange='',
                         routing_key=self.routing_key,
                         properties=_prop,
                         body=self.body)
        if blocking is True:
            while self.response is None:
                self.conn.process_data_events()
        return str(self.response)
    

    def create(self, job):
        '''Posts a new job'''
        print 'Submitting new job to queue'
        res = self._post(None, 'create', blocking=True, job=job)
        return res

    def update(self, taskdata):
        '''Update a job'''
        res = self._post('update_task', 'update', blocking=False, taskdata=taskdata)
        return res
    
    def get(self, jobid):
        '''Get status on a current job'''
        print 'Requesting status for job {0}'.format(jobid)
        res = self._post(jobid, 'get', blocking=True)
        return res
