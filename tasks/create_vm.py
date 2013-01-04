#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='xen03.aethernet.local'
client = salt.client.LocalClient('/Users/matthew/python/salt/etc/salt/minion')

@task
def create_vm(uuid_input, host_input, install):
    uuid = uuid_input['uuid']
    hostname = uuid_input['hostname']
    mem = host_input['mem']
    cpu = host_input['cpu']
    net = host_input['net_layout'][0]
    family = host_input['family'][0]
    storage = host_input['storage'][0]
    
    vm_results = client.cmd(salthost, 'vm.store_create', [family, hostname, uuid, mem, cpu, storage, net, install])
    
#    store_create(vm_family, vm_name, vm_uuid, vm_mem, vm_cpu,
#                vm_storage_layout, vm_network_layout,
#                vm_install=True):
    
    
    return True
