#!/usr/bin/env python

from api import task
import salt.client
import sys

# To be replaced with proper config
# in the meantime, create your own tempconfig (keep out of Git)
import tempconfig
salt_master_conf = tempconfig.salt_master_conf
salthost = tempconfig.ip_salthost

# Initiate salt client
client = salt.client.LocalClient(salt_master_conf)


@task
def reserve_ip():
    result = client.cmd(salthost, 'ip.reserve', ['192.168.0.1', 24, 1])
    if result[salthost]['exit_code'] != 0:
        raise error.HubError(result)
    ip = result[salthost]['data'][0]
    return {'ip':ip}
