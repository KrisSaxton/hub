'''
Database persistance

Classes:
HubDatabase - Base class.
'''
import json

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
    
    def putjob(self, job):
        self.db.set(job.state.id, job)
        for task in job.state.tasks:
            self.db.set(task.state.id, job.state.id)
        return True
    
    def getjob(self, jobid):
        ret = self.db.get(jobid)
        return ret
    
    def getjobid(self, taskid):
        ret = self.db.get(taskid)
        return ret