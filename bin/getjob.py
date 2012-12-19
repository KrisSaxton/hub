#!/usr/bin/env python
import sys
import hub.lib.config as config

config_file = '/usr/local/pkg/hub/etc/hub.conf'

from hub.lib.client import Client

if __name__ == '__main__':
    jobid = sys.argv[1]
    client = Client(config_file)
    response = client.post_wait(jobid, 'get')
    print response
