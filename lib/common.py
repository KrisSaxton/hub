# Core modules
# 3rd part modules
import json

class State(object):

    '''Class representing a unit of work's state.  Essentially a dictionary which can be easily
       de/serialised for passing through the messaging network.'''

    def __init__(self):
        self._state = {}
        if not self.status: # Do I need this?
            self.status = 'PENDING'

    def __str__(self):
        return '%s' % self._state

    def __setattr__(self, name, value):
        if name.startswith('_'):
            object.__setattr__(self, name, value)
        else:
            self.__dict__['_state'][name] = value

    def __getattr__(self, name):
        if name.startswith('_'):
            return self.__dict__.get(name, None)
        else:
            return self.__dict__['_state'].get(name, None)

    def load(self, record):
        '''Load job state from json object.'''
        self._record = json.loads(record)
        # merge existing uuid and status with record info
        self._state = dict(self._state.items() + self._record.items())
        return self

    def save(self):
        '''Save job state to json object.'''
        return json.dumps(self._state)

