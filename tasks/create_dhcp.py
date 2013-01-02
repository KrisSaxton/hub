#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='Matt-MacBook-Air.local'


client = salt.client.LocalClient()

@task
def create_dhcp(uuid_input, ip_input):
    hostname = uuid_input['uuid']
    mac = uuid_input['mac']
    ip = ip_input['ip']
    dhcp_results = client.cmd(salthost, 'dhcp.reservation_create', ['dhcp01', 'group1', hostname, mac, ip])
#    reservation_create(dhcp_server, dhcp_group, host_name, mac,  ip):
    return dhcp_results
