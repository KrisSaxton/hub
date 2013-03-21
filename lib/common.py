'''Common fucntions shared by dispatchers and workers
'''
# Core modules
import os
import sys
import time
import atexit
import logging
import traceback
from signal import SIGTERM 

# 3rd part modules
import json


class State(object):

    '''Class representing a unit of work's state.  Essentially a dictionary which can be easily de/serialised for passing through the messaging network.'''

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


class Daemon():
	"""
	A generic daemon class by Sander Marechal.
	
	Usage: subclass the Daemon class and override the run() method
	"""
	def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
		self.stdin = stdin
		self.stdout = stdout
		self.stderr = stderr
		self.pidfile = pidfile
		self.log = logging.getLogger(__name__)
	
	def daemonize(self):
		"""
		do the UNIX double-fork magic, see Stevens' "Advanced 
		Programming in the UNIX Environment" for details (ISBN 0201563177)
		http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
		"""
		try: 
			pid = os.fork() 
			if pid > 0:
				# exit first parent
				sys.exit(0) 
		except OSError, e: 
			sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
			sys.exit(1)
	
		# decouple from parent environment
		os.chdir("/") 
		os.setsid() 
		os.umask(0) 
	
		# do second fork
		try: 
			pid = os.fork() 
			if pid > 0:
				# exit from second parent
				sys.exit(0) 
		except OSError, e: 
			sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
			sys.exit(1) 
	
		# redirect standard file descriptors
		sys.stdout.flush()
		sys.stderr.flush()
		si = file(self.stdin, 'r')
		so = file(self.stdout, 'a+')
		se = file(self.stderr, 'a+', 0)
		os.dup2(si.fileno(), sys.stdin.fileno())
		os.dup2(so.fileno(), sys.stdout.fileno())
		os.dup2(se.fileno(), sys.stderr.fileno())
	
		# write pidfile
		atexit.register(self.delpid)
		pid = str(os.getpid())
		file(self.pidfile,'w+').write("%s\n" % pid)
	
	def delpid(self):
		os.remove(self.pidfile)

	def start(self, *args):
		"""
		Start the daemon
		"""
		# Check for a pidfile to see if the daemon already runs
		try:
			pf = file(self.pidfile,'r')
			pid = int(pf.read().strip())
			pf.close()
		except IOError:
			pid = None
	
		if pid:
			message = "pidfile %s already exist. Daemon already running?\n"
			sys.stderr.write(message % self.pidfile)
			sys.exit(1)
		
		# Start the daemon
		self.daemonize()
		self.run(*args)

	def stop(self):
		"""
		Stop the daemon
		"""
		# Get the pid from the pidfile
		try:
			pf = file(self.pidfile,'r')
			pid = int(pf.read().strip())
			pf.close()
		except IOError:
			pid = None
	
		if not pid:
			message = "pidfile %s does not exist. Daemon not running?\n"
			sys.stderr.write(message % self.pidfile)
			return # not an error in a restart

		# Try killing the daemon process	
		try:
			while 1:
				os.kill(pid, SIGTERM)
				time.sleep(0.1)
		except OSError, err:
			err = str(err)
			if err.find("No such process") > 0:
				if os.path.exists(self.pidfile):
					os.remove(self.pidfile)
			else:
				print str(err)
				sys.exit(1)

	def restart(self):
		"""
		Restart the daemon
		"""
		self.stop()
		self.start()

	def run(self):
		"""
		You should override this method when you subclass Daemon. It will be called after the process has been
		daemonized by start() or restart().
		"""
