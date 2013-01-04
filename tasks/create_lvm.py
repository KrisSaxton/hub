#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='xen03.aethernet.local'


client = salt.client.LocalClient('/Users/matthew/python/salt/etc/salt/minion')

@task
def create_lvm(uuid_input):
    hostname = uuid_input['hostname']
    lvm_results = client.cmd(salthost, 'lvm.create', ['vg01', hostname, 10])    
#    create(vg_name, lv_name, lv_size):
    return lvm_results
