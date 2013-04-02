#!/usr/bin/env python

from api import task

@task(async=True)
def add(arg1, arg2):
    print 'Passing parent id to end point %s' % add.state.parent_id
    return arg1 + arg2
