#!/usr/bin/env python
import sys
import hub.lib.config as config

config_file = '/usr/local/pkg/hub/etc/hub.conf'

from hub.lib.client import Client

if __name__ == '__main__':
    with open(sys.argv[1]) as f:
        job = f.read()
        client = Client(config_file)
        response = client.post_wait(None, 'create', job=job)
        print 'Successfully submitted job with job id: %s' % response
        print 'And body:'
        print job
