#!/usr/bin/env python
import sys
import pika
import json
import uuid

BROKER='ks-test-02'

class Client(object):

    '''Class representing things that can submit jobs.'''

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=BROKER))
        self.channel = self.connection.channel()

    def post(self, job_id, taskdata):
        '''Takes task data as a json ojbect and posts to dispatcher.'''
        self.response = None
        self.channel.basic_publish(exchange='',
                         routing_key='hub_results',
                         properties=pika.BasicProperties(
                              correlation_id = str(job_id),
                              content_type='application/json',
                              ),
                         body=json.dumps(taskdata))
        while self.response is None:
            self.connection.process_data_events()
        return str(self.response)

if __name__ == '__main__':
    job_id = sys.argv[1]
    taskdata = {'status': 'SUCCESS', 'name': 'add', 'args': [1, 2], 'parent_id': job_id, 'data': 4, 'id': '277e2421-e7a0-11e1-af41-109adda719a8'}
    client = Client()
    client.post(job_id, taskdata)
    print 'Successfully submitted task results %s for job with id: %s' % (taskdata, job_id)
