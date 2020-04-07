import asyncio
from datetime import datetime

import asyncpg


class Alarm():
    '''Класс, содержащий информацию о уведомлениях'''
    def __init__(self, alarm_id, alarm_time, 
                 alarm_device_id, alarm_type, status):
        self.alarm_id = alarm_id
        self.alarm_time = alarm_time
        self.alarm_device_id = alarm_device_id
        self.alarm_type = alarm_type
        self.status = status

    def __str__(self):
        return f'{self.alarm_type}'

    def __repr__(self):
        return f'{self.alarm_type}'


class Device():
    '''Класс, содержащий информацию о устройствах для упаковки.'''
    def __init__(self, device_id, name):
        self.name = name
        self.device_id = device_id
        self.line_event = list()
        self.broken_line_event = list()
        self.alarm_list = list()# УВЕДОМЛЕНИЯ
        self.issue_list = list() # ОПОВЕЩЕНИЯ
        self.receipts_list = list() # ЧЕКИ
        self.suitcases_list = list() # УПАКОВКИ

    def __repr__(self):
        return f'{self.device_id}, {self.name}, {self.receipts}'

    def __str__(self):
        return f'{self.device_id}, {self.name}, {self.receipts}'


class Issue():
    '''Класс, содержащий информацию о оповещениях.'''
    def __init__(self, suitcase_id, issue_time, device_id, issue_type, status):
        self.suitcase_id = suitcase_id
        self.issue_time = issue_time
        self.device_id = device_id
        self.issue_type = issue_type
        self.status = status

    def __str__(self):
        return f'{self.issue_type}'

    def __repr__(self):
        return f'{self.issue_type}'
    

class Receipts():
    '''Класс, содержащий информацию о чеках.'''
    def __init__(self, receipt_id, receipts_timestamp, device_id, 
                 quantitypackageone, quantitypackagedouble, status):
        self.receipt_id = receipt_id
        self.receipts_timestamp = receipts_timestamp
        self.device_id = device_id
        self.quantitypackageone = quantitypackageone
        self.quantitypackagedouble = quantitypackagedouble
        self.status = status

    def __str__(self):
        return f'quantitypackageone: {self.quantitypackageone}'

    def __repr__(self):
        return f'quantitypackagedouble: {self.quantitypackagedouble}'


class Suitcase():
    '''Класс, содержащий информацию о упаковках'''
    def __init__(self, suitcase_id, suitcase_start, suitcase_finish, 
                 package_type, polycom_id, totalid, status, duration, device_id):
        self.device_id = device_id
        self.suitcase_id = suitcase_id
        self.suitcase_start = suitcase_start
        self.suitcase_finish = suitcase_finish
        self.package_type = package_type
        self.polycom_id = polycom_id
        self.totalid = totalid
        self.receipt_id = ''
        self.package_type_by_receipt = None
        self.csp = str()
        self.unpaid = str()
        self.to_account = str()
        self.issue_list = {
            'id': '',
            'total': '',
            'suitcase': '',
            'date': '',
            'localdate': '',
        }
        self.status = status
        self.duration = duration

    def __str__(self):
        return f'polycom_id: {self.polycom_id}, package_type suitcase: \
{self.package_type}, suitcase_start: {self.suitcase_start}'

    def __repr__(self):
        return f'polycom_id: {self.polycom_id}, package_type suitcase: \
{self.package_type}, suitcase_start: {self.suitcase_start}'


class Request():
    '''Класс с запросами к базе.'''
    def __init__(self, pg_user, pg_password, pg_host, pg_db, 
                 date_start, date_finish):
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_host = pg_host
        self.pg_db = pg_db
        self.date_start = date_start
        self.date_finish = date_finish
    
    async def _connect_database(self):
        '''Создание подключения к базе.'''
        conn = await asyncpg.connect(
            f'postgresql://{self.pg_user}:{self.pg_password}'
                        f'@{self.pg_host}/{self.pg_db}')
        return conn

    async def get_devices(self):
        '''Получение списка активных устройств упаковки из базы. 
        Создание списка экземпляров классов Device'''
        conn = await self._connect_database()
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
        conn = await self._connect_database()
        alarm_list = list()
        rows = await conn.fetch(f"\
            SELECT polycommalarm.id, localdate, device, \
                polycomm_alarm_type.title, polycommalarm.status \
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
            if row['status'] == 0:
                alarm_list.append(Alarm(alarm_id=row['id'],
                                        alarm_time=row['localdate'],
                                        alarm_device_id=row['device'],
                                        alarm_type=row['title'],
                                        status=row['status']
                                        ))
        return alarm_list.copy()

    async def get_issue(self, device_id):
        '''
        Получение оповещений из базы. 
        Создание списка экземпляров классов Issue
        
        :param device_id: - id активного устройства, полученный
        методом get_devices()
        '''
        conn = await self._connect_database()
        rows = await conn.fetch(f"\
            SELECT suitcase, localdate, device, polycomm_issue_type.title, status \
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
            if row['status'] == 0:
                issue_list.append(Issue(suitcase_id=row['suitcase'],
                                        issue_time=row['localdate'],
                                        device_id=row['device'],
                                        issue_type=row['title'],
                                        status=row['status']))
        return issue_list.copy()

    async def get_issue_type(self):
        '''Получение id и названий типов оповещений'''
        conn = await self._connect_database()
        rows = await conn.fetch(f"SELECT id, title from polycomm_issue_type")
        await conn.close()
        
        issue_type = dict()
        for row in rows:
            issue_type[row['id']] = row['title']
        return issue_type

    async def get_receipts(self, device_id):
        '''
        Получение чеков из базы. 
        Создание списка экземпляров классов Receipts
        
        :param device_id: - id активного устройства, полученный
        методом get_devices()
        '''
        conn = await self._connect_database()
        rows = await conn.fetch(f"\
        SELECT DISTINCT receipts.receipt_id, dateclose, polycomm_device.id, \
            receipts.quantitypackageone, receipts.quantitypackagedouble, \
            receipts.status \
        FROM receipts \
        LEFT JOIN polycomm_device on \
             CAST(receipts.devicecode as int) = polycomm_device.code \
        WHERE dateclose > timestamp '{self.date_start}' and \
              dateclose < timestamp '{self.date_finish}' and \
              polycomm_device.id = {device_id} \
        ORDER BY dateclose")
        await conn.close()

        receipts_list = list()
        for row in rows:
            if row['status'] == 0:
                receipts_list.append(Receipts(
                            receipt_id=row['receipt_id'],
                            receipts_timestamp=row['dateclose'],
                            device_id=row['id'],
                            quantitypackageone=row['quantitypackageone'],
                            quantitypackagedouble=row['quantitypackagedouble'],
                            status=row['status']))
        return receipts_list.copy()
    

    async def get_suitcases(self, device_id):
        '''
        Получение упаковок из базы. 
        Создание списка экземпляров классов Suitcase
        
        :param device_id: - id активного устройства, полученный
        методом get_devices()
        '''
        conn = await self._connect_database()
        rows = await conn.fetch(f"\
        SELECT id, dateini_local, local_date, package_type, receipt_id \
            polycom_id, totalid, status, duration \
        FROM polycomm_suitcase \
        WHERE \
            dateini_local > timestamp '{self.date_start}' and \
            dateini_local < timestamp '{self.date_finish}' and \
            device_id = {device_id} \
        ORDER BY dateini_local;")
        await conn.close()

        suitcases_list = list()
        for row in rows:
            if row['status'] == 0:
                suitcases_list.append(Suitcase(suitcase_id=row['id'],
                                           suitcase_start=row['dateini_local'],
                                           suitcase_finish=row['local_date'],
                                           package_type=row['package_type'],
                                           polycom_id=row['polycom_id'],
                                           totalid=row['totalid'],
                                           status=row['status'],
                                           duration=row['duration'],
                                           device_id=device_id)
                                  )
        return suitcases_list.copy()
    
    
    async def create_polycommissue_event(self, event):
        '''
        Создание записи в таблице polycommissue.
        Suitcase.issue_list['type'] in {7, 8, 9}
        
        :param event: - упаковка, с искусственным оповещением. 
        '''
        # conn = await self._connect_database() #control_db
        conn = await asyncpg.connect('postgresql://postgres:1@localhost/test')
        await conn.execute("\
        INSERT INTO polycommissue(id, \
                                  localdate, \
                                  device, \
                                  total, \
                                  suitcase, \
                                  duration, \
                                  type, \
                                  date, \
                                  createtime) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            event.issue_list['id'],
            event.issue_list['localdate'],
            event.device_id,
            event.issue_list['total'],
            event.issue_list['suitcase'],
            event.duration,
            event.issue_list['type'],
            event.issue_list['date'],
            datetime.now())
        


    # a) id
    # b) localdate
    # c) device
    # d) total
    # e) suitcase
    # f) duration
    # g) type
    # h) date
    # i) createtime


    # id               bigint not null,
    # guid             text,
    # localdate        timestamp,
    # device           bigint,
    # total            integer,
    # resolved         boolean,
    # comment          text,
    # pid_did          integer,
    # suitcase         bigint,
    # duration         integer,
    # type             bigint,
    # packer_error     boolean,
    # starttime        timestamp,
    # endtime          timestamp,
    # responsible      bigint,
    # createtime       timestamp,
    # video            bigint,
    # date             timestamp,
    # videorequesttime timestamp,
    # videostatus      bigint,
    # prevsuitcase     bigint,
    # callback         boolean default false,
    # status           integer default 0