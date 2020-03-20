import asyncio
import asyncpg

import csv
import logging
from datetime import datetime, timedelta


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - \
                    %(levelname)s - %(message)s')


PG_USER = 'postgres'
PG_PASSWORD = '1'
PG_HOST = 'localhost'
PG_DB = 'control_db'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DATE_START = '2020-01-01'
DATE_FINISH = '2020-01-02'
TIMEDELTA_CHECK = 420


class Alarm():
    '''КЛАСС С УВЕДОМЛЕНИЯМИ'''
    
    alarm_time = ''
    alarm_id = int()
    alarm_device_id = int()
    alarm_type = ''
    
    def __init__(self, alarm_id, 
                       alarm_time,
                       alarm_device_id,
                       alarm_type):
        self.alarm_id = alarm_id
        self.alarm_time = alarm_time
        self.alarm_device_id = alarm_device_id
        self.alarm_type = alarm_type
        
    def __str__(self):
        return f'{self.alarm_type}'
        # return f'{self.alarm_id}, {self.alarm_time}, {self.alarm_device_id}'
    
    def __repr__(self):
        return f'{self.alarm_type}'
        # return f'{self.alarm_id}, {self.alarm_time}, {self.alarm_device_id}'


class Device():
    '''КЛАСС ДЕВАЙСОВ'''
    name = str()
    device_id = int()
    alarm_list = list() # УВЕДОМЛЕНИЯ
    issue_list = list() # ОПОВЕЩЕНИЯ
    receipts_list = list() # ЧЕКИ
    suitcases_list = list() # УПАКОВКИ
    line_event = list()
    
    def __init__(self, device_id, 
                       name):
        self.name = name
        self.device_id = device_id
        self.alarm_list = list()
        self.issue_list = list()
        self.receipts_list = list()
        self.suitcases_list = list()
        self.line_event = list()
    
    
    def __repr__(self):
        return f'{self.device_id}, {self.name}, {self.receipts}'
    
    
    def __str__(self):
        return f'{self.device_id}, {self.name}, {self.receipts}'


class Issue():
    '''КЛАСС С ОПОВЕЩЕНИЯМИ'''
    suitcase_id = int()
    issue_time = ''
    issue_device_id = int()
    issue_type = ''

    def __init__(self, suitcase_id, 
                       issue_time, 
                       device_id,
                       issue_type):
        self.suitcase_id = suitcase_id
        self.issue_time = issue_time
        self.device_id = device_id
        self.issue_type = issue_type
    
    def __str__(self):
        return f'{self.issue_type}'
        # return f'{self.suitcase_id}, {self.issue_time}, {self.device_id}'
    
    def __repr__(self):
        return f'{self.issue_type}'
        # return f'{self.suitcase_id}, {self.issue_time}, {self.device_id}'
        
    
class Receipts():
    '''КЛАСС С ЧЕКАМИ'''
    receipts_id = int()
    receipts_timestamp = ''
    device_id = int()
    suitcases = list()
    quantitypackageone = ''
    quantitypackagedouble = ''
    
    def __init__(self, receipts_id,
                       receipts_timestamp,
                       device_id,
                       quantitypackageone,
                       quantitypackagedouble):
        self.receipts_id = receipts_id
        self.receipts_timestamp = receipts_timestamp
        self.device_id = device_id
        self.suitcases = list()
        self.quantitypackageone = quantitypackageone
        self.quantitypackagedouble = quantitypackagedouble
        
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
    suitcase_alarm = list()
    package_type = ''
    
    def __init__(self, suitcase_id, 
                       suitcase_start, 
                       suitcase_finish,
                       package_type):
        self.suitcase_id = suitcase_id
        self.suitcase_start = suitcase_start
        self.suitcase_finish = suitcase_finish
        self.suitcase_issue = list()
        self.suitcase_alarm = list()
        self.package_type = package_type


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
    
    
    async def get_alarm(self, device_id):
        '''ПОЛУЧЕНИЕ УВЕДОМЛЕНИЙ'''
        
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
        '''ПОЛУЧЕНИЕ ОПОВЕЩЕНИЙ(должны жить внутри упаковки)'''
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
        '''ПОЛУЧЕНИЕ ЧЕКОВ'''
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
    
    
    async def get_suitcases(self, device_id):
        '''ПОЛУЧЕНИЕ УПАКОВОК'''
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
    

async def run_app(request: Get_request):
    devices = await request.get_devices()
    
    for device in devices:
        device.alarm_list = await request.get_alarm(device.device_id)
        device.issue_list = await request.get_issue(device.device_id)
        device.receipts_list = await request.get_receipts(device.device_id)
        device.suitcases_list = await request.get_suitcases(device.device_id)
        
        for alarm in device.alarm_list:
            device.line_event.append(
                    {'time': alarm.alarm_time,
                     'object': alarm,
                     'type':'alarm'
                    }
                )
        for issue in device.issue_list:
            device.line_event.append(
                    {'time': issue.issue_time,
                     'object': issue,
                     'type':'issue'
                    }
                )
        for receipt in device.receipts_list:
            device.line_event.append(
                    {'time': receipt.receipts_timestamp,
                     'object': receipt,
                     'type':'receipt'
                    }
                )
        for suitcase in device.suitcases_list:
            device.line_event.append(
                    {'time': suitcase.suitcase_start,
                     'object': suitcase,
                     'type': 'suitcase_start'
                    }
                )
            device.line_event.append(
                    {'time': suitcase.suitcase_finish,
                     'object': suitcase,
                     'type': 'suitcase_finish'
                    }
                )
            
        device.line_event.sort(key=lambda d: d['time'])
        
        
        # NOTE: drobbing 
        last = None
        for i in range(len(device.line_event)):
            if device.line_event[i]['type'] in {'suitcase_start', 'suitcase_finish'}:
                last = device.line_event[i]['type']
            if device.line_event[i]['type'] == 'receipt':
                if last == None:
                    j = i
                    while j < len(device.line_event) and device.line_event[j]['type'] not in {'suitcase_start', 'suitcase_finish'}:
                        j += 1
                    if j < len (device.line_event) and device.line_event[j]['type'] == 'suitcase_finish':
                        t = device.line_event.pop(i)
                        device.line_event.insert(j, t)
                if last == 'suitcase_start':
                    j = i
                    while j < len(device.line_event) and device.line_event[j]['type'] != 'suitcase_finish':
                        j += 1
                    if j < len(device.line_event):
                        t = device.line_event.pop(i)
                        device.line_event.insert(j, t)
        del last
        
        
        #NOTE: groupping
        i = 0 #WORKED:
        while len(device.line_event) > i:                        
            if i != len(device.line_event)-1:
                if device.line_event[i]['type'] == 'receipt':
                    if device.line_event[i+1]['type'] in {'suitcase_start', 'issue', 'alarm'}:
                        device.line_event.insert(i+1, {"type": "none"})
                elif device.line_event[i]['type'] != 'none' and device.line_event[i+1]['time'] - device.line_event[i]['time'] > timedelta(seconds=TIMEDELTA_CHECK):
                    device.line_event.insert(i+1, {"type": "none", "ДРОБЛЕНИЕ ПО ПРИЗНАКУ": "ВРЕМЯ"})
            i += 1
        del i
        
        
        #NOTE: create [[s], [s], [s]]  
        new_list = [[]]
        j = 0
        while len(device.line_event) > 0:
            if device.line_event[0]['type'] != 'none':
                new_list[j].append(device.line_event.pop(0))
            else:
                if device.line_event[0].get('ДРОБЛЕНИЕ ПО ПРИЗНАКУ'):
                    new_list.append([{'ДРОБЛЕНИЕ ПО ПРИЗНАКУ': "ВРЕМЯ"}])
                else:
                    new_list.append([])
                j += 1
                device.line_event.pop(0)
                
        device.line_event = new_list
        del j, new_list
        
        
        for block in device.line_event:
            for event in block:
                print(event)
            print()
        
                
        
        # #NOTE: CSV
        with open('suitcases.csv', 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter=";")
            
            for block in device.line_event:
                writer.writerow((device.name,''))
                for i in block:
                    if i.get("ДРОБЛЕНИЕ ПО ПРИЗНАКУ"):
                        writer.writerow(('420 СЕКУНД', ''))
                        
                    elif i['type'] == 'suitcase_start':
                        writer.writerow(('', i['time'], 'НАЧАЛО УПАКОВКИ'))
                    elif i['type'] == 'suitcase_finish':
                        writer.writerow(('', i['time'], "КОНЕЦ УПАКОВКИ"))
                    
                    elif i['type'] == 'receipt' and i['object'].quantitypackageone > 0:
                        writer.writerow(('', i['time'], i['type'], f"quantitypackageone: {i['object'].quantitypackageone}"))
                    elif i['type'] == 'receipt' and i['object'].quantitypackagedouble > 0:
                        writer.writerow(('', i['time'], i['type'], f"quantitypackagedouble: {i['object'].quantitypackagedouble}"))
                    else:
                        writer.writerow(('', i['time'], i['type']))
                writer.writerow('')

  
    
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
