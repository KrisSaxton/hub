#!/usr/bin/env python

from api import task
import salt.client
import sys

salt_host='Matt-MacBook-Air.local'

client = salt.client.LocalClient('/Users/kris/dev/salt/etc/salt/master')

@task(async=True)
def salt_sleep(secs, job_id, parent_id):
    sleep_results = client.cmd_async(salt_host, 'salthub.sleep', [secs,'salt_sleep',job_id,parent_id, "DONE"])
    return job_id

