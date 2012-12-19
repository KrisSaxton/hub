#!/usr/bin/env python
import sys
import hub.lib.config as config

config_file = '/usr/local/pkg/hub/etc/hub.conf'

from hub.lib.client import Client

if __name__ == '__main__':
    job_id = sys.argv[1]
    taskdata = {'status': 'SUCCESS', 'name': 'add', 'args': [1, 2], 'parent_id': job_id, 'data': 4, 'id': '277e2421-e7a0-11e1-af41-109adda719a8'}
    client = Client(config_file)
    client.post(job_id, 'update', taskdata=taskdata)
    print 'Successfully submitted task results %s for job with id: %s' % (taskdata, job_id)
