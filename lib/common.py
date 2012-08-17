# Core modules
import sys
import traceback
# 3rd part modules
import json

class State(object):

    '''Class representing a unit of work's state.  Essentially a dictionary which can be easily
       de/serialised for passing through the messaging network.'''

    def __init__(self):
        self._state = {}

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
        # merge state with record info
        self._state = dict(self._state.items() + self._record.items())
        return self

    def save(self):
        '''Save job state to json object.'''
        return json.dumps(self._state)

class ExternalTaskState(object):

    def __init__(self):

        self._info  = sys.__stdout__
        self._error = sys.__stderr__ 

        self._state = {}

        self._arguments = sys.argv[1:]

        if len(self._arguments) < 2:
            print 'Missing request or reply filename %s ' % self._arguments

        self._request_file, self._reply_file = self._arguments

        if len(self._request_file) == 0 or len(self._reply_file) == 0:
            print "Both request and reply files have to be set."

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

    def __del__(self):
        if self._state:
            try:
                file = open(self._reply_file, 'w')
                file.write(json.dumps(self._state))
                file.close()
            except IOError:
                trace = traceback.format_exc()
                print "Unable to open reply file: %s" % self._reply_file
                raise 

    def info(self, message):
        print >> self._info, message
        self._info.flush()

    def error(self, message):
        print >> self._error, message
        self._error.flush()

    def fail(self, message, exit_code=1):
        self.error(message)
        sys.exit(exit_code)

    def reply(self):
        return self._state

    def request(self):
        if self._state:
            return self._state
        else:
            try:
                file = open(self._request_file, 'r')
                self._state = json.loads(file.read())
                file.close()
            except IOError:
                trace = traceback.format_exc()
                print "Unable to open request file: %s" % self._request_file
                raise 
            #except json.JSONDecodeError:
            #    trace = traceback.format_exc()
            #    msg = "An error parsing JSON data in file :%s" % self._request_file
            #    raise error.JSONError(msg, trace)
            #    file.close()

            return self._state
