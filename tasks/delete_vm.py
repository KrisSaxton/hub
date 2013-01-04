#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='xen03.aethernet.local'
client = salt.client.LocalClient('/Users/matthew/python/salt/etc/salt/minion')

@task
def delete_vm(uuid_input):
    hostname = uuid_input['hostname']
    delete_results = client.cmd(salthost, 'vm.store_delete', [hostname])
    
    return delete_results
