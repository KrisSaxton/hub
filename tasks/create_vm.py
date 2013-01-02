#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='Matt-MacBook-Air.local'
client = salt.client.LocalClient()

@task
def create_vm(uuid_input, host_input):
    uuid = uuid_input['uuid']
    hostname = uuid_input['hostname']
    mem = host_input['mem']
    cpu = host_input['cpu']    
    net = host_input['net_layout']
    family = host_input['family']
    storage = host_input['storage']
    
    vm_results = client.cmd(salthost, 'vm.store_create', [family, hostname, uuid, mem, cpu, storage, net, True])
    
#    store_create(vm_family, vm_name, vm_uuid, vm_mem, vm_cpu,
#                vm_storage_layout, vm_network_layout,
#                vm_install=True):
    
    
    return vm_results
