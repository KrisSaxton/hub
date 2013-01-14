#!/usr/bin/env python

from api import task
import salt.client
import sys

# To be replaced with proper config
# in the meantime, create your own tempconfig (keep out of Git)
import tempconfig
salt_master_conf = tempconfig.salt_master_conf
salthost = tempconfig.dhcp_salthost

# Initiate salt client
client = salt.client.LocalClient(salt_master_conf)

@task
def create_dhcp(uuid_input, ip_input):
    hostname = uuid_input['hostname']
    mac = uuid_input['mac']
    ip = ip_input['ip']
    result = client.cmd(salthost, 'dhcp.reservation_create', [hostname, mac, ip])
    if result[salthost]['exit_code'] != 0:
        raise error.HubError(result)
    return result
