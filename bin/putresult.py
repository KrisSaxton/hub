#!/usr/bin/env python
import sys
import hub.lib.config as config

config_file = '/usr/local/pkg/hub/etc/hub.conf'

from hub.lib.client import Client

if __name__ == '__main__':
    task_id = sys.argv[1]
    taskdata = {'status': 'SUCCESS', 'data': 4, 'id': task_id }
    client = Client(config_file)
    client.update(taskdata=taskdata)
    print 'Successfully submitted task results %s for task with id: %s' % (taskdata, task_id)
