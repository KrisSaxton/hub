#!/usr/bin/env python
import sys
import pika
import json
import uuid
import hub.lib.config as config

config_file = '/usr/local/pkg/hub/etc/hub.conf'

try:
    conf = config.setup(config_file)
except error.ConfigError, e:
    print e.msg
    raise e


class Client(object):

    '''Class representing things that can submit jobs.'''

    def __init__(self):
        self.conf = config.setup()
        self.broker = conf.get('HUB', 'broker')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=self.broker))
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

    def post(self, job):
        '''Takes a job as a json ojbect and posts to dispatcher.'''
        self.response = None
        self.corr_id = str(uuid.uuid4())
        print 'Submitting job with submission id %s' % self.corr_id
        self.channel.basic_publish(exchange='',
                         routing_key='hub_jobs',
                         properties=pika.BasicProperties(
                              content_type='application/json',
                              reply_to = self.callback_queue,
                              correlation_id = self.corr_id,
                              ),
                         body=job)
        print "Listening for response with id: %s " % self.corr_id
        while self.response is None:
            self.connection.process_data_events()
        return str(self.response)

if __name__ == '__main__':
    with open(sys.argv[1]) as f:
        job = f.read()
        client = Client()
        response = client.post(job)
        print 'Successfully submitted job with job id: %s' % response
