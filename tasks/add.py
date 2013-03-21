#!/usr/bin/env python

from api import task

@task(async=True)
def add():
    output_file = '/tmp/cmdline'
    with open(output_file, 'w') as f:
        msg = 'kernel runid=%s consoletty' % runid
        f.write(msg)
        f.close()
