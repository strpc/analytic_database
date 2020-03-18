import asyncio
import asyncpg

import logging


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - \
                    %(levelname)s - %(message)s')


PG_USER = 'postgres'
PG_PASSWORD = '1'
PG_HOST = 'localhost'
PG_DB = 'control_db'
DATE_START = '2020-01-01'
DATE_FINISH = '2020-01-02'


class Alarm():
    '''КЛАСС С УВЕДОМЛЕНИЯМИ'''
    
    alarm_time = ''
    alarm_id = int()
    alarm_device_id = int()
    
    def __init__(self, alarm_id, 
                       alarm_time,
                       alarm_device_id):
        self.alarm_id = alarm_id
        self.alarm_time = alarm_time
        self.alarm_device_id = alarm_device_id
        
    def __str__(self):
        return f'{self.alarm_id}, {self.alarm_time}, {self.alarm_device_id}'
    
    def __repr__(self):
        return f'{self.alarm_id}, {self.alarm_time}, {self.alarm_device_id}'


class Device():
    '''КЛАСС ДЕВАЙСОВ'''
    name = str()
    device_id = int()
    receipts = list()
    
    def __init__(self, device_id, 
                       name):
        self.name = name
        self.device_id = device_id
        self.receipts = list()
    
    def __repr__(self):
        return f'{self.device_id}, {self.name}, {self.receipts}'
    
    def __str__(self):
        return f'{self.device_id}, {self.name}, {self.receipts}'


class Issue():
    '''КЛАСС С ОПОВЕЩЕНИЯМИ'''
    suitcase_id = int()
    issue_time = ''
    issue_device_id = int()

    def __init__(self, suitcase_id, 
                       issue_time, 
                       device_id):
        self.suitcase_id = suitcase_id
        self.issue_time = issue_time
        self.device_id = device_id
    
    def __str__(self):
        return f'{self.suitcase_id}, {self.issue_time}, {self.device_id}'
    
    def __repr__(self):
        return f'{self.suitcase_id}, {self.issue_time}, {self.device_id}'
        
    
class Receipts():
    '''КЛАСС С ЧЕКАМИ'''
    receipts_id = int()
    receipts_timestamp = ''
    device_id = int()
    suitcases = list()
    
    def __init__(self, receipts_id, 
                       receipts_timestamp, 
                       device_id):
        self.receipts_id = receipts_id
        self.receipts_timestamp = receipts_timestamp
        self.device_id = device_id
        self.suitcases = list()
        
    def __str__(self):
        return f'{self.receipts_id}, {len(self.suitcases)}'
    
    def __repr__(self):
        return f'{self.receipts_id}, {len(self.suitcases)}'
        

class Suitcase():
    '''КЛАСС С УПАКОВКАМИ'''
    suitcase_id = int()
    suitcase_start = ''
    suitcase_finish = ''
    suitcase_issue = list()
    
    def __init__(self, suitcase_id, 
                       suitcase_start, 
                       suitcase_finish):
        self.suitcase_id = suitcase_id
        self.suitcase_start = suitcase_start
        self.suitcase_finish = suitcase_finish
        self.suitcase_issue = list()


    def __str__(self):
        return f'{self.suitcase_id}, {self.suitcase_start}'
    
    def __repr__(self):
        return f'{self.suitcase_id, self.suitcase_start}'


class Get_request():
    '''ЗАПРОСЫ К БАЗЕ'''
    def __init__(self, pg_user, 
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
        '''ПОДКЛЮЧЕНИЕ К БАЗЕ'''
        conn = await asyncpg.connect(
            f'postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}/{self.pg_db}')
        return conn
    
    
    async def get_devices(self):
        '''ПОЛУЧЕНИЕ СПИСКА ДЕВАЙСОВ'''
        conn = await self.connect()
        
        rows = await conn.fetch('\
            SELECT id, title FROM polycomm_device \
            inner join timestamps \
                on polycomm_device.code = CAST(timestamps.devicecode as int) \
                and timestamps.ready=True and timestamps.status_type_device=1 \
                ORDER BY id; \
            ')
        await conn.close()
        
        devices_list = list()
        for row in rows:
            devices_list.append(Device(device_id=row['id'], name=row['title']))
            
        return devices_list.copy()
    
    
    async def get_suitcases(self, device_id):
        '''ПОЛУЧЕНИЕ УПАКОВОК'''
        conn = await self.connect()
        
        rows = await conn.fetch(f"\
        SELECT id, dateini_local, local_date \
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
                                           suitcase_finish=row['local_date'])
                                  )
            
        return suitcases_list.copy()
    
    
    async def get_receipts(self, device_id):
        '''ПОЛУЧЕНИЕ ЧЕКОВ'''
        conn = await self.connect()

        rows = await conn.fetch(f"\
        SELECT DISTINCT receipts.receipt_id, dateopen, device_id\
        FROM receipts \
        LEFT JOIN polycomm_suitcase on \
            receipts.receipt_id = polycomm_suitcase.receipt_id \
        WHERE \
            dateopen > timestamp '{self.date_start}' and \
            dateopen < timestamp '{self.date_finish}' and \
            device_id = {device_id} \
        ORDER BY \
            dateopen \
        ")
        
        await conn.close()
        
        receipts_list = list()
        for row in rows:
            receipts_list.append(Receipts(receipts_id=row['receipt_id'], 
                                          receipts_timestamp=row['dateopen'], 
                                          device_id=row['device_id'])
                                  ) 
        return receipts_list.copy()
    
    
    async def get_issue(self, device_id):
        '''ПОЛУЧЕНИЕ ОПОВЕЩЕНИЙ(должны жить внутри упаковки)'''
        conn = await self.connect()
        
        rows = await conn.fetch(f"\
            SELECT suitcase, localdate, device \
            FROM polycommissue \
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
                                    device_id=row['device']))
        return issue_list.copy()
    
    async def get_alarm(self, device_id):
        '''ПОЛУЧЕНИЕ УВЕДОМЛЕНИЙ'''
        
        conn = await self.connect()

        alarm_list = list()
        rows = await conn.fetch(f"\
            SELECT id, localdate, device \
            FROM polycommalarm \
            WHERE localdate > timestamp '{self.date_start}' \
                and localdate < timestamp '{self.date_finish}' \
                and device = {device_id} \
            ORDER BY localdate \
            ")
        await conn.close()
        
        for row in rows:
            alarm_list.append(Alarm(alarm_id=row['id'],
                                    alarm_time=row['localdate'],
                                    alarm_device_id=row['device']
                                    ))
        print(*alarm_list)
        return alarm_list.copy()
        

async def run_app(request):
    devices = await request.get_devices()
    for device in devices:
        suitcases = await request.get_suitcases(device.device_id)
        device.receipts = await request.get_receipts(device.device_id)
        issue_list = await request.get_issue(device.device_id)
        alarm_list = await request.get_alarm(device.device_id)
        
        
        for receipt in device.receipts:
            
            while len(suitcases) > 0 and receipt.receipts_timestamp > suitcases[0].suitcase_start:
                try:
                    while len(issue_list) > 0 and issue_list[0].issue_time < suitcases[1].suitcase_start:
                        print(issue_list[0])
                        suitcases[0].suitcase_issue.append(issue_list.pop(0))
                except IndexError:
                    suitcases[0].suitcase_issue.append(issue_list.pop(0))
                receipt.suitcases.insert(0, suitcases.pop(0))
    
    
        print(suitcases)
    print()

    for device in devices:
        print(f'\t╠ НАЗВАНИЕ ДЕВАЙСА {device.name}')
        for receipt in device.receipts:
            print(f'\t╚═══╦ НАЧАЛО ЧЕКА {receipt.receipts_id}')
            for suitcase in receipt.suitcases:
                print(f'\t    ╚═══╦ НАЧАЛО УПАКОВКИ {suitcase.suitcase_start}')
                for issue in suitcase.suitcase_issue:
                    print(f'\t        ╠════ ОПОВЕЩЕНИЯ {issue.issue_time}')
                print(f'\t    ╔═══╩ КОНЕЦ УПАКОВКИ {suitcase.suitcase_finish}')
            print(f'\t╔═══╩ ЗАКРЫТИЕ ЧЕКА {receipt.receipts_timestamp} \n\t║\n\t║' )
        print('\t║\n\t║')
    
if __name__ == '__main__':
    request = Get_request(pg_user=PG_USER,
                          pg_password=PG_PASSWORD,
                          pg_host=PG_HOST,
                          pg_db=PG_DB,
                          date_start=DATE_START,
                          date_finish=DATE_FINISH
                          )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_app(request))
    # loop.run_until_complete(request.get_alarm(132))
    