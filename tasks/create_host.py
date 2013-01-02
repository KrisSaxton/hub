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
    h_mem = host_results[ldap]['data'][0][1]['aenetHostMem']
    h_cpu = host_results[ldap]['data'][0][1]['aenetHostCPU']
    net = host_results[ldap]['data'][0][1]['aenetHostNetworkLayout']
    storage = host_results[ldap]['data'][0][1]['aenetHostStorageLayout']
    family = host_results[ldap]['data'][0][1]['aenetHostFamily']
    return {'mem': h_mem, 'cpu': h_cpu, 'net_layout': net, 'family': family, 'storage': storage}
