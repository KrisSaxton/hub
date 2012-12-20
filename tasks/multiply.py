#!/usr/bin/env python

from api import task

@task
def multiply(arg1, arg2):
    return arg1 * arg2
