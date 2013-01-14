#!/usr/bin/env python

from api import task
import salt.client
import sys

orgname = 'testorg'
mem = 256
cpu = 1
storage_type = 'basic'
network = 'no_internet'

# To be replaced with proper config
# in the meantime, create your own tempconfig (keep out of Git)
import tempconfig
salt_master_conf = tempconfig.salt_master_conf
salthost = tempconfig.host_salthost

# Initiate salt client
client = salt.client.LocalClient(salt_master_conf)


@task
def create_host(input):
    uuid = input['uuid']
    hostname = orgname + '-' + str(uuid)
    result = client.cmd(salthost, 'host.host_create', [orgname, hostname, uuid, mem, cpu, 'xen', 'para', storage_type, network])
    if result[salthost]['exit_code'] != 0:
        raise error.HubError(result)
    net = result[salthost]['data'][0][1]['aenetHostNetworkLayout']
    storage = result[salthost]['data'][0][1]['aenetHostStorageLayout']
    family = result[salthost]['data'][0][1]['aenetHostFamily']
    return {'mem': mem, 'cpu': cpu, 'net_layout': net, 'family': family, 'storage': storage}
