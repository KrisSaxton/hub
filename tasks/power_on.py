#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='xen03.aethernet.local'
client = salt.client.LocalClient('/Users/matthew/python/salt/etc/salt/minion')

@task(async=True)
def power_on(uuid_input):
    hostname = uuid_input['hostname']
    client.cmd(salthost, 'vm.power_create', [hostname])
    
    return None
