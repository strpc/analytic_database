# enable dwr_service.py:
DEVICE_WITHOUT_RECEIPTS = True

# Database settings:
PG_USER = 'postgres'
PG_PASSWORD = '1'
PG_HOST = 'localhost'
PG_DB = 'base_full'

# Timesettings:
LAST_EVENT_TIME = 900
TIMEDELTA_CHECK = 420
TIME_SLEEP = 900 # in second

# Logging:
DIR_NAME_LOG = 'logs'
FILE_NAME_LOG = 'analytic.log'
FILESIZE_LOG = 5 # in Mb
COUNT_BACKUP_LOG = 5
