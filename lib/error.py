'''Common application exceptions

Classes:
Exception - Base class.

'''

class HubError(Exception):
    '''Base class for exceptions in this application'''
    def __init__(self, msg, traceback=None):
        Exception.__init__(self, msg)
        self.exit_code = 1
        self.msg = msg
        self.info = 'for more information on Hub errors: visit http://<tbc>/'
        self.traceback = traceback


class InputError(HubError):
    def __init__(self, msg):
        self.exit_code = 2
        self.msg = msg

class ConfigError(InputError):
    def __init__(self, msg):
        self.msg = msg

class MethodNotImplemented(HubError):
    '''Raised on missing essential method in subclass.'''
    pass

class ValidationError(HubError):
    '''Raising on input validation failures.'''
    pass

class MessagingError(HubError):
    '''Raised on problems connecting to messaging system.'''
    pass 
