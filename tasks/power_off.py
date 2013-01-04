#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='xen03.aethernet.local'
client = salt.client.LocalClient('/Users/matthew/python/salt/etc/salt/minion')

@task
def power_off(uuid_input):
    hostname = uuid_input['hostname']
    power_results = client.cmd(salthost, 'vm.power_modify', [hostname, 'forceoff'])
    
    return power_results
