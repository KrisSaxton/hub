#!/usr/bin/env python

from api import task
import salt.client
import sys

# To be replaced with proper config
# in the meantime, create your own tempconfig (keep out of Git)
import tempconfig
salt_master_conf = tempconfig.salt_master_conf
salthost = tempconfig.lvm_salthost

# Initiate salt client
client = salt.client.LocalClient(salt_master_conf)


@task
def create_lvm(uuid_input):
    hostname = uuid_input['hostname']
    result = client.cmd(salthost, 'lvm.create', [hostname, 10])    
    if result[salthost]['exit_code'] != 0:
        raise error.HubError(result)
    return result
