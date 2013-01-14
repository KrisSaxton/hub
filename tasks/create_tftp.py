#!/usr/bin/env python

from api import task
import salt.client
import sys

# To be replaced with proper config
# in the meantime, create your own tempconfig (keep out of Git)
import tempconfig
salt_master_conf = tempconfig.salt_master_conf
salthost = tempconfig.tftp_salthost

# Initiate salt client
client = salt.client.LocalClient(salt_master_conf)

@task
def create_tftp(uuid_input, run_id):
    hostname = uuid_input['uuid']
    mac = uuid_input['mac']
    result = client.cmd(salthost, 'tftp.create', [mac, 'xendomu.template', run_id])
#    create(mac=None, template=None, run_id=None):
    if result[salthost]['exit_code'] != 0:
            raise error.HubError(result)
    return result
