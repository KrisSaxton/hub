#!/usr/bin/env python

from hub.lib.api import task

@task
def multiply(arg1, arg2):
    return arg1 * arg2
