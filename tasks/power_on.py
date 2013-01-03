#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='Matt-MacBook-Air.local'
client = salt.client.LocalClient()

@task(async=True)
def power_on(uuid_input):
    hostname = uuid_input['hostname']
    client.cmd(salthost, 'vm.power_create', [hostname])
    
    return None
