#!/usr/bin/env python
"""Provides application-wide configuration information

Classes:
HubConfig - extends ConfigParser with a save method.

Functions:
setup - reads a configuration file and returns a HubConfig object

Exceptions:
ConfigError - raised when config file missing or empty during initial call.
"""
# core modules
import ConfigParser

# own modules
import hub.lib.error as error

__CONFIG_FILE = False

__author__ = "Kris Saxton"

def setup(config_file=None):
    '''Read configuration file as argument (or from global var) and return 
HubConfig object. Set global var to current config file.'''
    global __CONFIG_FILE
    if not config_file and not __CONFIG_FILE:
        raise error.ConfigError('First call needs a config file')
    _file = __CONFIG_FILE
    if config_file:
        _file = config_file
    conf = HubConfig(_file)
    __CONFIG_FILE = _file
    return conf

class HubConfig(ConfigParser.ConfigParser):
    
    """Extend ConfigParser with save method.  Return a configuration object."""
    
    def __init__(self, config_file):
        """Instantiate ConfigParser object. Parse configuration file."""
        self.config_file = config_file
        ConfigParser.ConfigParser.__init__(self)
        if self.read(self.config_file) == []:
            raise error.ConfigError('Config file %s is missing or empty' % 
                                    self.config_file)
            
    def get(self, section, key, default=None):
        """Get method extended with with specific exceptions."""
        try:
            result = ConfigParser.ConfigParser.get(self, section, key)
        except ConfigParser.NoSectionError:
            if default == None:
                msg = '%s missing required [%s] section' % \
                                                    (self.config_file, section)
                raise error.ConfigError(msg)
            result = default
        except ConfigParser.NoOptionError:
            if default == None:
                msg = '%s missing required \'%s = <value>\' entry' % \
                                                    (self.config_file, key)
                raise error.ConfigError(msg)
            result = default
        return result

    def items(self, section):
        """Lists items in key value pairs from a section"""
        try:
            result = ConfigParser.ConfigParser.items(self, section)
        except ConfigParser.NoSectionError:
            msg = '%s missing required [%s] section' % \
                                                    (self.config_file, section)
            raise error.ConfigError(msg)
        except ConfigParser.NoOptionError:
            msg = '%s missing required \'<key> = <value>\' entry' % \
                                                    (self.config_file)
            raise error.ConfigError(msg)
        return result

    def save(self):
        """Save current configuration to a file"""
        try:
            config_file = open(self.config_file, 'wb')
            self.write(config_file)
            config_file.close()
        except Exception, e:
            raise e
        
if __name__ == '__main__':
    pass
