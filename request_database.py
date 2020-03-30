import asyncpg

import asyncio


class Alarm():
    '''Класс, содержащий информацию о уведомлениях'''

    alarm_time = ''
    alarm_id = int()
    alarm_device_id = int()
    alarm_type = ''

    def __init__(self,
                 alarm_id,
                 alarm_time,
                 alarm_device_id,
                 alarm_type):
        self.alarm_id = alarm_id
        self.alarm_time = alarm_time
        self.alarm_device_id = alarm_device_id
        self.alarm_type = alarm_type

    def __str__(self):
        return f'{self.alarm_type}'

    def __repr__(self):
        return f'{self.alarm_type}'


class Device():
    '''Класс, содержащий информацию о устройствах для упаковки.'''

    name = str()
    device_id = int()
    alarm_list = list()  # УВЕДОМЛЕНИЯ
    issue_list = list()  # ОПОВЕЩЕНИЯ
    receipts_list = list()  # ЧЕКИ
    suitcases_list = list()  # УПАКОВКИ
    # service:
    line_event = list()
    broken_line_event = list()
    list_receipt_with_suitcase = list()
    list_receipt_without_suitcase = list()

    def __init__(self,
                 device_id,
                 name):
        self.name = name
        self.device_id = device_id
        self.alarm_list = list()
        self.issue_list = list()
        self.receipts_list = list()
        self.suitcases_list = list()
        self.line_event = list()
        self.broken_line_event = list()
        self.list_receipt_with_suitcase = list()
        self.list_receipt_without_suitcase = list()

    def __repr__(self):
        return f'{self.device_id}, {self.name}, {self.receipts}'

    def __str__(self):
        return f'{self.device_id}, {self.name}, {self.receipts}'


class Issue():
    '''Класс, содержащий информацию о оповещениях.'''

    suitcase_id = int()
    issue_time = ''
    issue_device_id = int()
    issue_type = ''

    def __init__(self,
                 suitcase_id,
                 issue_time,
                 device_id,
                 issue_type):
        self.suitcase_id = suitcase_id
        self.issue_time = issue_time
        self.device_id = device_id
        self.issue_type = issue_type

    def __str__(self):
        return f'{self.issue_type}'

    def __repr__(self):
        return f'{self.issue_type}'


class Receipts():
    '''Класс, содержащий информацию о чеках.'''

    receipts_id = int()
    receipts_timestamp = ''
    device_id = int()
    quantitypackageone = ''
    quantitypackagedouble = ''

    def __init__(self,
                 receipts_id,
                 receipts_timestamp,
                 device_id,
                 quantitypackageone,
                 quantitypackagedouble):
        self.receipts_id = receipts_id
        self.receipts_timestamp = receipts_timestamp
        self.device_id = device_id
        self.quantitypackageone = quantitypackageone
        self.quantitypackagedouble = quantitypackagedouble

    def __str__(self):
        return f'quantitypackageone: {self.quantitypackageone}'

    def __repr__(self):
        return f'quantitypackagedouble: {self.quantitypackagedouble}'


class Suitcase():
    '''Класс, содержащий информацию о упаковках'''

    suitcase_id = int()
    suitcase_start = ''
    suitcase_finish = ''
    package_type = ''

    def __init__(self,
                 suitcase_id,
                 suitcase_start,
                 suitcase_finish,
                 package_type):
        self.suitcase_id = suitcase_id
        self.suitcase_start = suitcase_start
        self.suitcase_finish = suitcase_finish
        self.package_type = package_type

    def __str__(self):
        return f'package_type suitcase: {self.package_type}, suitcase_start: {self.suitcase_start}'

    def __repr__(self):
        return f'package_type suitcase: {self.package_type}, suitcase_start: {self.suitcase_start}'


class Get_request():
    '''Класс с запросами к базе.'''

    def __init__(self,
                 pg_user,
                 pg_password,
                 pg_host,
                 pg_db,
                 date_start,
                 date_finish):
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_host = pg_host
        self.pg_db = pg_db
        self.date_start = date_start
        self.date_finish = date_finish

    async def connect(self):
        '''Создание подключения к базе.'''

        conn = await asyncpg.connect(
            f'postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}/{self.pg_db}')
        return conn

    async def get_devices(self):
        '''Получение списка активных устройств упаковки из базы. 
        Создание списка экземпляров классов Device'''

        conn = await self.connect()

        rows = await conn.fetch('\
            SELECT id, title FROM polycomm_device \
            INNER JOIN timestamps \
                on polycomm_device.code = CAST(timestamps.devicecode as int) \
                and timestamps.ready=True and timestamps.status_type_device=1 \
            ORDER BY id; \
            ')
        await conn.close()

        devices_list = list()
        for row in rows:
            devices_list.append(Device(device_id=row['id'], name=row['title']))

        return devices_list.copy()

    async def get_alarm(self, device_id):
        '''
        Получение уведомлений из базы. 
        Создание списка экземпляров классов Alarm
        
        :param device_id: - id активного устройства, полученный
        методом get_devices()
        '''

        conn = await self.connect()

        alarm_list = list()
        rows = await conn.fetch(f"\
            SELECT polycommalarm.id, localdate, device, polycomm_alarm_type.title \
            FROM polycommalarm \
            INNER JOIN polycomm_alarm_type ON \
                polycomm_alarm_type.id = polycommalarm.alarmtype \
            WHERE localdate > timestamp '{self.date_start}' \
                and localdate < timestamp '{self.date_finish}' \
                and device = {device_id} \
            ORDER BY localdate \
            ")
        await conn.close()

        for row in rows:
            alarm_list.append(Alarm(alarm_id=row['id'],
                                    alarm_time=row['localdate'],
                                    alarm_device_id=row['device'],
                                    alarm_type=row['title']
                                    ))
        # print(*alarm_list)
        return alarm_list.copy()

    async def get_issue(self, device_id):
        '''
        Получение оповещений из базы. 
        Создание списка экземпляров классов Issue
        
        :param device_id: - id активного устройства, полученный
        методом get_devices()
        '''

        conn = await self.connect()

        rows = await conn.fetch(f"\
            SELECT suitcase, localdate, device, polycomm_issue_type.title \
            FROM polycommissue \
            INNER JOIN polycomm_issue_type ON \
                polycommissue.type = polycomm_issue_type.id \
            WHERE localdate > timestamp '{self.date_start}' \
                and localdate < timestamp '{self.date_finish}' \
                and device = {device_id} \
            ORDER BY localdate \
            ")
        await conn.close()

        issue_list = list()
        for row in rows:
            issue_list.append(Issue(suitcase_id=row['suitcase'],
                                    issue_time=row['localdate'],
                                    device_id=row['device'],
                                    issue_type=row['title']))
        return issue_list.copy()

    async def get_receipts(self, device_id):
        '''
        Получение чеков из базы. 
        Создание списка экземпляров классов Receipts
        
        :param device_id: - id активного устройства, полученный
        методом get_devices()
        '''

        conn = await self.connect()

        #WORKED: dateopen
        rows = await conn.fetch(f"\
        SELECT DISTINCT receipts.receipt_id, dateclose, device_id, receipts.quantitypackageone, receipts.quantitypackagedouble \
        FROM receipts \
        LEFT JOIN polycomm_suitcase on \
            receipts.receipt_id = polycomm_suitcase.receipt_id \
        WHERE \
            dateclose > timestamp '{self.date_start}' and \
            dateclose < timestamp '{self.date_finish}' and \
            device_id = {device_id} \
        ORDER BY \
            dateclose \
        ")

        await conn.close()

        receipts_list = list()
        for row in rows:
            receipts_list.append(Receipts(receipts_id=row['receipt_id'],
                                          receipts_timestamp=row['dateclose'],
                                          device_id=row['device_id'],
                                          quantitypackageone=row['quantitypackageone'],
                                          quantitypackagedouble=row['quantitypackagedouble']
                                          )
                                 )
        return receipts_list.copy()

    async def get_suitcases(self, device_id  ):
        '''
        Получение упаковок из базы. 
        Создание списка экземпляров классов Suitcase
        
        :param device_id: - id активного устройства, полученный
        методом get_devices()
        '''

        conn = await self.connect()

        rows = await conn.fetch(f"\
        SELECT id, dateini_local, local_date, package_type \
        FROM polycomm_suitcase \
        WHERE \
        dateini_local > timestamp '{self.date_start}' and \
        dateini_local < timestamp '{self.date_finish}' and \
        device_id = {device_id} \
        order by dateini_local;")

        await conn.close()

        suitcases_list = list()
        for row in rows:
            suitcases_list.append(Suitcase(suitcase_id=row['id'],
                                           suitcase_start=row['dateini_local'],
                                           suitcase_finish=row['local_date'],
                                           package_type=row['package_type'])
                                  )

        return suitcases_list.copy()