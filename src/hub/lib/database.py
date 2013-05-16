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

    def updatejob(self, job):
        self.putjob(job)
        return True
    
    def putjob(self, job):
        self.db.hset(job.state.id, 'job', job)
        for k, v in job.state._state.iteritems():
            self.db.hset(job.state.id, k, v)
        for task in job.state.tasks:
            self.db.hset(task.state.id, 'task', task)
            for k, v in task.state._state.iteritems():
                self.db.hset(task.state.id, k, v)
                if k == "status" and v not in ['SUCCESS', 'FAILED']:
                    self.db.sadd('INCOMPLETE', task.state.id)
                elif k == "status":
                    self.log.debug("Removing task {0} from INCOMPLETE".format(task.state.id))
                    self.db.srem('INCOMPLETE', task.state.id)
                #Do this so that if the parent is failed we don't keep in INCOMPLETE
                if k =="status" and job.state.status =="FAILED":
                    self.log.debug("Removing task {0} from INCOMPLETE".format(task.state.id))
                    self.db.srem('INCOMPLETE', task.state.id)
                self.db.hset(task.state.id, k, v)
                
        return True
    
    def getjob(self, jobid):
        ret = self.db.hget(jobid, 'job')
        self.log.info(ret)
        return ret
    
    def gettask(self, taskid):
        ret = self.db.hget(taskid, 'task')
        return ret
        
    def getjobid(self, taskid):
        ret = self.db.hget(taskid, 'parent_id')
        return ret
    
    def getincompletetasks(self):
        ret = self.db.smembers('INCOMPLETE')
        return ret
    
class HubSqlite(HubDatabase):
    
    def __init__(self, host, port, instance, user=None, password=None ):
        self.host = host
        self.user = user
        self.password = password
        import sqlite3
        self.conn = sqlite3.connect(self.host)
        self.conn.row_factory = self._dict_factory
        self.db = self.conn.cursor()
        self.log = logging.getLogger(__name__)
        
    def _dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    
    def putjob(self,job):
        for task in job.state.tasks:
            self.log.info(str(task))
            keys = []
            values =[]
            for k, v in task.state._state.iteritems():
                keys.append(str(k))
                values.append(json.dumps(v))
            columns = ', '.join(task.state._state.keys())
            qmarks = ', '.join('?' * len(task.state._state))
#            values = "\""
            
#            values += "\""
            qry = "INSERT INTO hub_tasks ({0}) VALUES ({1})".format(columns,qmarks)
            self.log.info(qry)
            self.log.info(tuple(values))
            self.db.execute(qry, tuple(values))
        keys = []
        job.state._state.pop('tasks')
        values=[]
        for k, v in job.state._state.iteritems():
            keys.append(str(k))
            values.append(json.dumps(v))
        columns = ', '.join(job.state._state.keys())
        qmarks = ', '.join('?' * len(job.state._state))
#        values = "\""
#        values += '", "'.join(str(v) for v in job.state._state.values())
#        values += "\""
#        self.log.info(type(job.state._state))
#        self.log.info(keys)
#        self.log.info(values)
        qry = "INSERT INTO hub_jobs ({0}) VALUES ({1})".format(columns,qmarks)
        self.log.info(qry)
        self.log.info(tuple(values))
        self.db.execute(qry, tuple(values))
        self.conn.commit()

    def getjob(self,jobid):
        qry = "SELECT * FROM hub_tasks WHERE parent_id='{0}'".format(json.dumps(jobid))
        self.db.execute(qry)
        tasks = self.db.fetchall()
        for task in tasks:
            dellist=[]
            for k,v in task.iteritems():
                if v is None:
                    dellist.append(k)
            for k in dellist:
                task.pop(k) 
            for k,v in task.iteritems():
                task[k]=json.loads(v.encode())
        
        qry = "SELECT * FROM hub_jobs WHERE id='{0}'".format(json.dumps(jobid))
        self.db.execute(qry)
        job = self.db.fetchone()
        if job is None:
            ret = None
        else:

            dellist=[]
            for k,v in job.iteritems():
                if v is None:
                    dellist.append(k)
            for k in dellist:
                job.pop(k) 

            for k,v in job.iteritems():
                job[k]=json.loads(v.encode())
            job['tasks'] = tasks
            ret = json.dumps(job)
            self.log.info(ret)
        return ret
        
    def getjobid(self, taskid):
        qry = "SELECT parent_id FROM hub_tasks WHERE id='{0}'".format(json.dumps(taskid))
        self.db.execute(qry)
        ret = json.loads(self.db.fetchone()['parent_id'])
        
        return ret
#        for k, v in job.state._state.iteritems():
#            self.log.info('k: {0}, v: {1}'.format(k,v))
#            if k == 'tasks':
#                pass
#            else:
#                self.db.execute('INSERT INTO hub_jobs ({0}) VALUES ({1})'.format(k,v))
#            for task in job.state.tasks:
#                for k, v in task.state._state.iteritems():
#                    self.db.execute('INSERT INTO hub_tasks ({0}) VALUES ({1})'.format(k,v))
    #def getjob(self, jobid):
     #   tasks = 
        