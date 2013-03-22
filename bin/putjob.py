#!/usr/bin/env python
import sys
import hub.lib.config as config
from hub.lib.client import Client

if __name__ == '__main__':
    with open(sys.argv[1]) as f:
        job = f.read()
        client = Client('localhost')
        response = client.create(job)
        print 'Successfully submitted job with job id: %s' % response
        print 'And body:'
        print job
