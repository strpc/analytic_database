import asyncio
import asyncpg

import csv
import logging
from datetime import datetime


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
        # self.line_event = {
        #     'alarm_list': [],
        #     'issue_list': [],
        #     'receipts_list': [],
        #     'suitcases_list': [],
        #     'unlinked_issue_list': [],
        #     'unlinked_alarm_list': []
        # }
            
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
    suitcase_alarm = list()
    
    def __init__(self, suitcase_id, 
                       suitcase_start, 
                       suitcase_finish):
        self.suitcase_id = suitcase_id
        self.suitcase_start = suitcase_start
        self.suitcase_finish = suitcase_finish
        self.suitcase_issue = list()
        self.suitcase_alarm = list()


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

        #TODO: old: dateopen
        rows = await conn.fetch(f"\
        SELECT DISTINCT receipts.receipt_id, dateclose, device_id\
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
                                          device_id=row['device_id'])
                                  ) 
        return receipts_list.copy()
    
    
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

        # for i in device.line_event:
            # if i['type'] == 'suitcase_start':
            #     print(f"{device.name} - {i['time']} - НАЧАЛО УПАКОВКИ")
            # elif i['type'] == 'suitcase_finish':
            #    print(f"{device.name} - {i['time']} - КОНЕЦ УПАКОВКИ")
            # else:
            #     print(f"{device.name} - {i['time']} - {i['type']} - {i['object']}")
    #     with open('suites.csv')
        with open('suitcases.csv', 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter=";")
            for i in device.line_event:
                if i['type'] == 'suitcase_start':
                    writer.writerow((device.name, i['time'], 'НАЧАЛО УПАКОВКИ'))
                elif i['type'] == 'suitcase_finish':
                    writer.writerow((device.name, i['time'], "КОНЕЦ УПАКОВКИ"))
                else:
                    writer.writerow((device.name, i['time'], i['type']))
                        
        # print(device.name)
        # for issue in device.issue_list:
        #     for suitcase in device.suitcases_list:
        #         if issue.issue_time > suitcase.suitcase_start and issue.issue_time < suitcase.suitcase_finish and len(device.issue_list) > 0:
        #             # print('done')
        #             device.line_event.append(f'{issue.issue_time} - issue')
        #             device.issue_list.pop(0)
        #             # print(device.line_event[0])
                    
        #             # suitcase.suitcase_issue.append(device.issue_list.pop(0))
        #         else:
        #             if len(device.issue_list) > 0:
        #                 device.line_event['unlinked_issue_li st'].append(device.issue_list.pop(0))
        #                 # issue_unlinked.append(device.issue_list.pop(0))
                        
        #         for alarm in device.alarm_list:
        #             if alarm.alarm_time > suitcase.suitcase_start and alarm.alarm_time < suitcase.suitcase_finish and len(device.alarm_list) > 0:
        #                 # device.line_event['alarm_list'].append(device.alarm_list.pop(0))
        #                 # print('done')
        #                 pass
        #                 # print(alarm)
        #                 # suitcase.suitcase_alarm.append(device.alarm_list.pop(0))
        #             else:
        #                 if len(device.alarm_list) > 0:
        #                     # device.line_event['unlinked_alarm_list'].append(device.alarm_list.pop(0))
        #                     pass
        #                     # alarm_unlinked.append(device.alarm_list.pop(0))
                
        #         for receipt in device.receipts_list:
        #             pass
                    # device.line_event['receipts_list'].append(device.receipts_list[0].receipts_timestamp)
                    # device.receipts_list.pop(0)
                                                              #.pop(0))
        # for i in device.line_event['receipts_list']:
        #     print(datetime.strftime(i, DATE_FORMAT)
                
                
        # for receipt in device.receipts_list: # NOTE: v1
            
        #     # while len(device.suitcases_list) > 0 and receipt.receipts_timestamp > device.suitcases_list[0].suitcase_start:
        #     #     try:
        #     #         while len(device.issue_list) > 0 and device.issue_list[0].issue_time < device.suitcases_list[1].suitcase_start:
        #     #             print(device.issue_list[0])
        #     #             device.suitcases_list[0].suitcase_issue.append(device.issue_list.pop(0))
        #     #     except IndexError:
        #     #         device.suitcases_list[0].suitcase_issue.append(device.issue_list.pop(0))
        #     #     receipt.suitcases.insert(0, device.suitcases_list.pop(0))
    
        #     # for suitcase in device.suitcases_list:
        #     #     print(suitcase)
        #     #     print()

    # for device in devices:
    #     print(f'\t╠ НАЗВАНИЕ ДЕВАЙСА {device.name}')
    #     for receipt in device.receipts:
    #         print(f'\t╚═══╦ НАЧАЛО ЧЕКА {receipt.receipts_id}')
    #         for suitcase in receipt.suitcases:
    #             print(f'\t    ╚═══╦ НАЧАЛО УПАКОВКИ {suitcase.suitcase_start}')
    #             for issue in suitcase.suitcase_issue:
    #                 print(f'\t        ╠════ ОПОВЕЩЕНИЯ {issue.issue_time}')
    #             print(f'\t    ╔═══╩ КОНЕЦ УПАКОВКИ {suitcase.suitcase_finish}')
    #         print(f'\t╔═══╩ ЗАКРЫТИЕ ЧЕКА {receipt.receipts_timestamp} \n\t║\n\t║' )
    #     print('\t║\n\t║')
    
if __name__ == '__main__':
    request = Get_request(pg_user=PG_USER,
                          pg_password=PG_PASSWORD,
                          pg_host=PG_HOST,
                          pg_db=PG_DB,
                          date_start=DATE_START,
                          date_finish=DATE_FINISH
                          )
    # print(type(request))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_app(request))
    # loop.run_until_complete(request.get_alarm(132))
    