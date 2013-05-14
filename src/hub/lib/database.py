'''
Database persistance

Classes:
HubDatabase - Base class.
'''
import json
import logging

class HubDatabase():
    '''
    Base class for databases in this application
    '''
    pass

class HubRedis(HubDatabase):
    
    def __init__(self, host, port, instance, user=None, password=None ):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.instance = int(instance)
        import redis
        self.db = redis.StrictRedis(host=self.host,port=self.port,db=self.instance) 
        self.log = logging.getLogger(__name__)

    
    def putjob(self, job):
        self.db.set(job.state.id, job)
        for task in job.state.tasks:
#            self.db.set(task.state.id, 'id', job.state.id)
            for k, v in task.state._state.iteritems():
#                self.log.info(k + ":" + str(v))
                if k == "status" and v not in ['SUCCESS', 'FAILURE']:
                    self.db.sadd('INCOMPLETE', task.state.id)
                elif k == "status":
                    self.db.srem('INCOMPLETE', task.state.id)
                self.db.hset(task.state.id, k, v)
                
        return True
    
    def getjob(self, jobid):
        ret = self.db.get(jobid)
        return ret
    
    def getjobid(self, taskid):
        ret = self.db.hget(taskid, 'parent_id')
        return ret