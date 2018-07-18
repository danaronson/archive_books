import sys
import ConfigParser
import os
import logging

# get config object, reading config file from startup directory
def get_config(config_file_name):
    # read from same directory as this
    base_name = os.path.dirname(sys.argv[0])
    if '' == base_name:
        config_path = "."
    else:
        config_path = base_name
    config = ConfigParser.SafeConfigParser()
    config_file_name = config_path + "/" + config_file_name
    if 0 == len(config.read(config_file_name)):
        raise IOError("Could not find config file: '%s'\n" % config_file_name)
    return config


LOG_LEVELS = {"CRITICAL":logging.CRITICAL,"ERROR":logging.ERROR,"WARNING":logging.WARNING,"INFO":logging.INFO,"DEBUG":logging.DEBUG,"NOTSET":logging.NOTSET}


def setup_logging(config, name):
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVELS[config.get('logging', 'level')])
    # create console handler and set level to debug
    ch = logging.StreamHandler()

    # create console handler and set level to debug
    logging_file = config.get('logging', 'file')
    if 'stdout' == logging_file:
        ch = logging.StreamHandler()
    else:
        ch = logging.FileHandler(logging_file)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    if 0 == len(logger.handlers):
        logger.addHandler(ch)
    return logger
