#!/usr/bin/env python
import sys
import pika
import json
import uuid

BROKER='ks-test-02'

class Client(object):

    '''Class representing things that can get jobs.'''

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=BROKER))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
                        self.on_response,
                        no_ack=True,
                        queue=self.callback_queue)

    def on_response(self, channel, method, properties, body):
        if self.corr_id == properties.correlation_id:
            self.response = body    

    def post(self, jobid):
        '''Takes a job as python dict and posts as json object.'''
        self.response = None
        self.corr_id = str(uuid.uuid4())
        print 'Submitting status request for job with id %s' % jobid
        self.channel.basic_publish(exchange='',
                         routing_key='hub_status',
                         properties=pika.BasicProperties(
                              content_type='application/json',
                              reply_to = self.callback_queue,
                              correlation_id = self.corr_id,
                              ),
                         body=json.dumps(jobid))
        while self.response is None:
            self.connection.process_data_events()
        return str(self.response)

if __name__ == '__main__':
    jobid = sys.argv[1]
    client = Client()
    response = client.post(jobid)
    print response
