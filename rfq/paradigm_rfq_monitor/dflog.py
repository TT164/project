'''
    Follows Bastion lib but makes it independent of the Bastion lib
'''

_author_ = "Michael Vasquez"
_email_ = "mailto:mv@bastiontrade.com"

import logging.handlers
import os
import time
from datetime import datetime
from logging import INFO, Formatter, Logger, StreamHandler
from typing import Text

# creation_time = datetime.fromtimestamp(time.time()).strftime("%Y%m%dT%H%M%S")
creation_time = datetime.fromtimestamp(time.time()).strftime("%Y%m%d")

def get_logger(name: str,
               level = INFO,
               fmt: Text = '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
               file_prefix="ommlog",
               live_stream=True) -> Logger:
    """
    Prepare the logger for the system

    Parameters:
        name: The name of the logger used to log the call.
        handler: The handler of the logger process.
            For more information, please refer to: https://docs.python.org/3/library/logging.html#handler-objects
        level: Set the threshold of the logger to the `level`.
            Logging messages which are less severe than `level` will be ignored;
            logging messages which have same severity level or higher will be emitted by the handler service the logger.
            The default level is INFO.
            For the logging level, please refer to: https://docs.python.org/3/library/logging.html#logging-levels
        fmt: The logging message format.
            The default output format looks like: `2020-03-12 12:54:48,312 | root | WARN | Testing log formatting!`.
            For the detail format component, you can refer to:
            https://docs.python.org/3/library/logging.html#logrecord-attributes

    Returns:
        The wrapped logger object.
    """

    logger = Logger(name)
    logger.setLevel(level)
    f = Formatter(fmt)
    # if True:
    #     handler = logging.handlers.RotatingFileHandler(f'logs/ommlog_{creation_time}.log', maxBytes=1024*1024*100, backupCount=5)
    # else:
    #     handler = StreamHandler()

    if not os.path.exists('logs'):
        os.makedirs('logs')
    handler = logging.handlers.RotatingFileHandler(f'logs/{file_prefix}.log', maxBytes=1024*1024*1024, backupCount=5) # 1GB
 
    sz = os.path.getsize(f'logs/{file_prefix}.log')
    maxBytes = 1024*1024
    if sz >= maxBytes-50:
        f=open(f'logs/{file_prefix}.log', "r+")
        f.truncate()
        f.close()

    handler.setFormatter(f)
    logger.addHandler(handler)

    if live_stream:
        handler = StreamHandler()
        handler.setFormatter(f)
        logger.addHandler(handler)
    return logger
