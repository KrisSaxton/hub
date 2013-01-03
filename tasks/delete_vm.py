#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='Matt-MacBook-Air.local'
client = salt.client.LocalClient()

@task
def delete_vm(uuid_input):
    hostname = uuid_input['hostname']
    delete_results = client.cmd(salthost, 'vm.store_delete', [hostname])
    
    return delete_results
