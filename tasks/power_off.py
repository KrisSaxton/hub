#!/usr/bin/env python

from api import task
import salt.client
import sys

# To be replaced with proper config
# in the meantime, create your own tempconfig (keep out of Git)
import tempconfig
salt_master_conf = tempconfig.salt_master_conf
salthost = tempconfig.vm_salthost

# Initiate salt client
client = salt.client.LocalClient(salt_master_conf)


@task
def power_off(uuid_input):
    hostname = uuid_input['hostname']
    power_results = client.cmd(salthost, 'vm.power_modify', [hostname, 'forceoff'])
    
    return power_results
