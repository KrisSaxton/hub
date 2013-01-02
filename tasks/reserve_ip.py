#!/usr/bin/env python

from api import task
import salt.client
import sys

salthost='Matt-MacBook-Air.local'


client = salt.client.LocalClient()

@task
def reserve_ip():
    ip_results = client.cmd(salthost, 'ip.reserve', ['10.0.20.0', 24, 1])
    ip = ip_results[salt_host]['data'][0]
    return {'ip':ip}
