import os
import logging
import logging.handlers as handlers

DIR_NAME_LOG = 'logs'
FILE_NAME_LOG = 'analytic.log'
FILESIZE_LOG = 5 # in Mb
COUNT_BACKUP_LOG = 5  

if not os.path.exists(DIR_NAME_LOG):
    try:
        os.mkdir(DIR_NAME_LOG)
    except:
        logger.error('Ошибка при создании папок для логов')

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

def create(msg):
    logger.warning(msg)