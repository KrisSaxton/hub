#!/usr/bin/env python

from api import task
import salt.client
import sys

ldap='Matt-MacBook-Air.local'
dhcp='Matt-MacBook-Air.local'

orgname = 'aethernet'
mem = 256
cpu = 1
storage = 'basic'
network = 'no_internet'

client = salt.client.LocalClient()

@task
def get_uuid():
    uuid_results = client.cmd(ldap, 'host.uuid_reserve', ['get_mac=False'])
    uuid = uuid_results[ldap]['data'][0][0]
    mac = uuid_results[ldap]['data'][1][0]
    return {'uuid':uuid, 'mac':mac}
    #return uuid
