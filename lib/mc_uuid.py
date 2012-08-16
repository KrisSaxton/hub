#!/usr/bin/env python
# core modules
import sys

# own modules
import config
import logger
import mc_helper as mc
from host import SpokeHostUUID
       
config_file = '/usr/local/pkg/spoke/etc/spoke.conf'
config = config.setup(config_file)

if __name__ == '__main__':
    log = logger.setup('main', verbose=False, quiet=True)
    mc = mc.MCollectiveAction()
    request = mc.request()
    try:
        uuid_start = request['data']['uuid_start']
    except KeyError:
        uuid_start = None
    try:
        qty = request['data']['qty']
    except KeyError:
        qty = 1
    if request['action'] == 'create':
        try:
            mc.data = SpokeHostUUID().create(uuid_start)['data']
        except Exception as e:
            mc.fail(e.msg, e.exit_code)
    elif request['action'] == 'get':
        try:
            mc.data = SpokeHostUUID().get()['data']
        except Exception as e:
            mc.fail(e.msg, e.exit_code)
    elif request['action'] == 'delete':
        try:
            mc.data = SpokeHostUUID().delete()['data']
        except Exception as e:
            mc.fail(e.msg, e.exit_code)
    elif request['action'] == 'reserve':
        try:
            mc.data = SpokeHostUUID().modify(increment=qty)['data']
        except Exception as e:
            mc.fail(e.msg, e.exit_code)
    else:
        msg = "Unknown action: " + request['action']
        mc.fail(msg, 2)
    log.info('Result via Mcollective: %s' % mc.data)
    sys.exit(0)