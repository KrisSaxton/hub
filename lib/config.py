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

gConfig = None

__author__ = "Kris Saxton"

def setup(config_file=None, mco=False):
    """Instantiate (once only) and return HubConfig object."""
    global gConfig
# Commented this out to make sure MCollective agents re read the config if it changes
# It may break plenty of other things, who knows! -MMB
    if gConfig is not None and mco == False:
        pass
    else:
        gConfig = HubConfig(config_file)
    return gConfig

class HubConfig(ConfigParser.ConfigParser):
    
    """Extend ConfigParser with save method.  Return a configuration object."""
    
    def __init__(self, config_file):
        """Instantiate ConfigParser object. Parse configuration file."""
        if config_file is None:
            raise error.ConfigError('First call needs a config file')
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
