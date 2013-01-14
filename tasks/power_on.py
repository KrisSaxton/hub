#!/usr/bin/env python

from api import task
import error

import salt.client
import sys

# To be replaced with proper config
# in the meantime, create your own tempconfig (keep out of Git)
import tempconfig
salt_master_conf = tempconfig.salt_master_conf
salthost = tempconfig.vm_salthost

# Initiate salt client
client = salt.client.LocalClient(salt_master_conf)

@task(async=True)
def power_on(uuid_input):
    hostname = uuid_input['hostname']
    result = client.cmd(salthost, 'vm.power_create', [hostname])
    if result[salthost]['exit_code'] != 0:
        raise error.HubError(result)
    return result
