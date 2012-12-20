#!/usr/bin/env python

from api import task
import salt.client
import sys

ldap='Matt-MacBook-Air.local'
dhcp='Matt-MacBook-Air.local'

orgname = 'automationlogic'
mem = 256
cpu = 1
storage = 'basic'
network = 'no_internet'

client = salt.client.LocalClient()

@task
def create_host(input):
    uuid = input['uuid']
    hostname = orgname + '-' + str(uuid)
    host_results = client.cmd(ldap, 'host.host_create', [orgname, hostname, uuid, mem, cpu, 'xen', 'para', storage, network])
    return host_results

