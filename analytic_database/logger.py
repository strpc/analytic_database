# -*- coding: utf-8 -*-
import os
import logging
import logging.handlers as handlers
import inspect

from config import DIR_NAME_LOG, FILE_NAME_LOG, FILESIZE_LOG, COUNT_BACKUP_LOG



if not os.path.exists(os.path.join(os.getcwd(), DIR_NAME_LOG)):
    try:
        os.mkdir(os.path.join(os.getcwd(), DIR_NAME_LOG))
    except Exception as e:
        logging.error('Ошибка при создании папок для логов; {0}'.format(e))

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )
handler = handlers.RotatingFileHandler(os.path.join(DIR_NAME_LOG, FILE_NAME_LOG),
                                       maxBytes=FILESIZE_LOG*1024*1024,
                                       backupCount=COUNT_BACKUP_LOG,
                                       encoding='utf-8')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG) # or ERROR
logger.addHandler(handler)

def create(*args):
    line = ''
    for i in args:
        if i != args[-1]:
            line += str(i) + ' '
        else:
            line += str(i)
    if inspect.stack()[1].function != '<module>':
        line += "; Method " + inspect.stack()[1].function + '()'
    logger.error(line)

