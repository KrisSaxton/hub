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
def create_vm(uuid_input, host_input, install):
    uuid = uuid_input['uuid']
    hostname = uuid_input['hostname']
    mem = host_input['mem']
    cpu = host_input['cpu']
    net = host_input['net_layout'][0]
    family = host_input['family'][0]
    storage = host_input['storage'][0]

    result = client.cmd(salthost, 'vm.store_create', [family, hostname, uuid, mem, cpu, storage, net, install])
    if result[salthost]['exit_code'] != 0:
        raise error.HubError(result)
    return True
