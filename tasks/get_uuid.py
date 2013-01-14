#!/usr/bin/env python

from api import task
import salt.client
import sys

orgname = 'testorg'
mem = 256
cpu = 1
storage = 'basic'
network = 'no_internet'

# To be replaced with proper config
# in the meantime, create your own tempconfig (keep out of Git)
import tempconfig
salt_master_conf = tempconfig.salt_master_conf
salthost = tempconfig.uuid_salthost

# Initiate salt client
client = salt.client.LocalClient(salt_master_conf)


@task
def get_uuid():
    result = client.cmd(salthost, 'host.uuid_reserve', ['get_mac=True'])
    if result[salthost]['exit_code'] != 0:
        raise error.HubError(result)
    uuid = result[salthost]['data'][0][0]
    mac = result[salthost]['data'][1][0]
    hostname = orgname + '-' + str(uuid)
    return {'uuid':uuid, 'mac':mac, 'hostname':hostname}
