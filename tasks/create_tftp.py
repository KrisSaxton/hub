#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='dhcp01.aethernet.local'


client = salt.client.LocalClient('/Users/matthew/python/salt/etc/salt/minion')

@task
def create_tftp(uuid_input, run_id):
    hostname = uuid_input['uuid']
    mac = uuid_input['mac']
    tftp_results = client.cmd(salthost, 'tftp.create', [mac, 'xendomu.template', run_id])
#    create(mac=None, template=None, run_id=None):
    return tftp_results
