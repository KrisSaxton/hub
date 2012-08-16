'''Common application exceptions

Classes:
Exception - Base class.

'''

class HubError(Exception):
    '''Base class for exceptions in this application'''
    def __init__(self, msg, traceback=None):
        Exception.__init__(self, msg)
        self.exit_code = self._set_exit_code()
        self.msg = msg
        self.info = 'for more information on Hub errors: visit http://<tbc>/'
        self.traceback = traceback

    def _set_exit_code(self):
        return 1

class MethodNotImplemented(HubError):
    '''Raised on missing essential method in subclass.'''
    pass

class ValidationError(HubError):
    '''Raising on input validation failures.'''
    pass
