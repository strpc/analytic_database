# -*- coding: utf-8 -*-
"""

"""

import asyncpg

import asyncio
import csv
import logging
from datetime import datetime, timedelta

from config import (PG_USER,
                    PG_PASSWORD,
                    PG_HOST,
                    PG_DB,
                    DATE_FORMAT,
                    DATE_START,
                    DATE_FINISH,
                    TIMEDELTA_CHECK)


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - \
                    %(levelname)s - %(message)s')


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
        # return f'{self.alarm_id}, {self.alarm_time}, {self.alarm_device_id}'

    def __repr__(self):
        return f'{self.alarm_type}'
        # return f'{self.alarm_id}, {self.alarm_time}, {self.alarm_device_id}'


class Device():
    '''Класс, содержащий информацию о устройствах для упаковки.'''

    name = str()
    device_id = int()
    alarm_list = list()  # УВЕДОМЛЕНИЯ
    issue_list = list()  # ОПОВЕЩЕНИЯ
    receipts_list = list()  # ЧЕКИ
    suitcases_list = list()  # УПАКОВКИ
    line_event = list()
    broken_line_event = list()

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
        # return f'{self.suitcase_id}, {self.issue_time}, {self.device_id}'

    def __repr__(self):
        return f'{self.issue_type}'
        # return f'{self.suitcase_id}, {self.issue_time}, {self.device_id}'


class Receipts():
    '''Класс, содержащий информацию о чеках.'''

    receipts_id = int()
    receipts_timestamp = ''
    device_id = int()
    suitcases = list()
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
        self.suitcases = list()
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
    suitcase_issue = list()
    suitcase_alarm = list()
    package_type = ''

    def __init__(self,
                 suitcase_id,
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


async def run_app(request: Get_request):
    '''
    Получение списка устройств для упаковок, событий к ним,
    добавление всех событий в список словарей. Сортировка словаря.
    
    :param request: экземпляр класса Get_request().
    '''

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
                 'type': 'alarm'
                 }
            )
        for issue in device.issue_list:
            device.line_event.append(
                {'time': issue.issue_time,
                 'object': issue,
                 'type': 'issue'
                 }
            )
        for receipt in device.receipts_list:
            device.line_event.append(
                {'time': receipt.receipts_timestamp,
                 'object': receipt,
                 'type': 'receipt'
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
        sort_receipts(device)


def sort_receipts(device: Device):
    '''
    Правка событий по признаку "чек после упаковки".
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''


    last = None
    for i in range(len(device.line_event)):
        if device.line_event[i]['type'] in {'suitcase_start', 'suitcase_finish'}:
            last = device.line_event[i]['type']
        if device.line_event[i]['type'] == 'receipt':
            if last == None:
                j = i
                while j < len(device.line_event) and device.line_event[j]['type'] not in {'suitcase_start', 'suitcase_finish'}:
                    j += 1
                if j < len(device.line_event) and device.line_event[j]['type'] == 'suitcase_finish':
                    t = device.line_event.pop(i)
                    device.line_event.insert(j, t)
            if last == 'suitcase_start':
                j = i
                while j < len(device.line_event) and device.line_event[j]['type'] != 'suitcase_finish':
                    j += 1
                if j < len(device.line_event):
                    t = device.line_event.pop(i)
                    device.line_event.insert(j, t)
    grouping_events(device)


def grouping_events(device: Device):
    '''
    Группировка событий по признаку "полный цикл". Добавление события "none"
    после каждого полного цикла упаковки. Проверка на время между упаковками
    (не более TIMEDELTA_CHECK). 
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''

    i = 0
    while len(device.line_event) > i:
        if i != len(device.line_event)-1:
            if device.line_event[i]['type'] == 'receipt':
                if device.line_event[i+1]['type'] in {'suitcase_start', 'issue', 'alarm'}:
                    device.line_event.insert(i+1, {"type": "none"})
            elif device.line_event[i]['type'] != 'none' and device.line_event[i+1]['time'] - device.line_event[i]['time'] > timedelta(seconds=TIMEDELTA_CHECK):
                device.line_event.insert(i+1, {"type": "none", "ДРОБЛЕНИЕ ПО ПРИЗНАКУ": "ВРЕМЯ"})
        i += 1
    listing_events(device)


def listing_events(device: Device):
    '''
    Создание списка списков словарей. Каждый элемент главного списка - это
    список, который содержит в себе n-элементов словарей событий полного
    цикла упаковки.
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''

    new_list = [[]]
    j = 0
    while len(device.line_event) > 0:
        if device.line_event[0]['type'] != 'none':
            new_list[j].append(device.line_event.pop(0))
        else:
            if device.line_event[0].get('ДРОБЛЕНИЕ ПО ПРИЗНАКУ'):
                new_list.append([{'type': 'broken',
                                  'ДРОБЛЕНИЕ ПО ПРИЗНАКУ': 'ВРЕМЯ'}])
            else:
                new_list.append([])
            j += 1
            device.line_event.pop(0)
    device.line_event = new_list
    check_broked_events(device)


def check_broked_events(device: Device):
    '''
    Проверка упаковок на наличие чеков к ним
    device.broken_line_event - список бракованных упаковок.
    device.line_event - список успешных упаковок.
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''

    package_type_one = package_type_two = quantitypackageone = quantitypackagedouble = 0

    for block in device.line_event:
        for event in block:
            if event['type'] == 'suitcase_start' and event['object'].package_type == 1:
                package_type_one += 1
            elif event['type'] == 'suitcase_start' and event['object'].package_type == 2:
                package_type_two += 1

            elif event['type'] == 'receipt' and event['object'].quantitypackageone > 0:
                quantitypackageone += event['object'].quantitypackageone
            elif event['type'] == 'receipt' and event['object'].quantitypackagedouble > 0:
                quantitypackagedouble += event['object'].quantitypackagedouble
        if package_type_one != quantitypackageone or package_type_two != quantitypackagedouble:
            device.broken_line_event.append(block)
            device.line_event.remove(block)
        package_type_one = package_type_two = quantitypackageone = quantitypackagedouble = 0

    add_task(device)
    
    
def add_task(device: Device):
    '''
    Добавления типа задачи 
    {'task_type': '', 'type': 'service'}
    для каждого блока с событиями
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    count_alarm = count_issue = count_receipt = count_suitcase_start = 0
    
    for block in device.broken_line_event:
        for event in block:
            if event['type'] == 'alarm':
                count_alarm += 1
            elif event['type'] == 'issue':
                count_issue += 1
            elif event['type'] == 'receipt':
                count_receipt += 1
            elif event['type'] == 'suitcase_start':
                count_suitcase_start += 1
        
        if count_issue != 0 and count_receipt == count_suitcase_start == 0:
            block.insert(0, {'task_type': 'оповещение', 'type': 'service'})
        
        elif (count_receipt != 0 and count_issue == count_suitcase_start == 0) or (count_suitcase_start != 0 and count_receipt != 0 and count_suitcase_start < count_receipt):
            block.insert(0, {'task_type': 'чек без упаковок', 'type': 'service'})

        elif count_receipt != 0 and count_issue != 0 and count_suitcase_start == 0:
            block.insert(0, {'task_type': 'чеки без упаковок и уведомление', 'type': 'service'}) 
        
        elif count_suitcase_start != 0 and count_receipt != 0 and count_suitcase_start > count_receipt:
            block.insert(0, {'task_type': 'КПУ/КнПУ', 'type': 'service'}) 
        
        else:
            block.insert(0, {'task_type': 'смешанные', 'type': 'service'}) 
        count_alarm = count_issue = count_receipt = count_suitcase_start = 0
    
    
    
    create_csv(device)

    

def view_console(device: Device):
    '''
    Визуализация в консоль
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''

    for block in device.line_event:
        # print(block)
        for event in block:
            print(event)


def create_csv(device: Device):
    '''
    Cоздание CSV-файла с данными.
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''

    with open('suitcases_broken.csv', 'a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=";")

        for block in device.broken_line_event:
            writer.writerow((device.name, ''))
            for i in block:
                if i['type'] == 'service':
                    writer.writerow(('','task_type:', i['task_type']))
                elif i['type'] == 'broken':
                    writer.writerow((i['type'], f"ПРЕВЫШЕНИЕ ПО ВРЕМЕНИ МЕЖДУ УПАКОВКАМИ {TIMEDELTA_CHECK}"))

                elif i['type'] == 'suitcase_start':
                    writer.writerow(('', i['time'], 'НАЧАЛО УПАКОВКИ', f"ТИП УПАКОВКИ: {i['object'].package_type}"))
                elif i['type'] == 'suitcase_finish':
                    writer.writerow(('', i['time'], "КОНЕЦ УПАКОВКИ"))

                elif i['type'] == 'receipt' and i['object'].quantitypackageone > 0:
                    writer.writerow(('', i['time'], i['type'], f"ЧИСЛО ОДИНАРНЫХ УПАКОВОК В ЧЕКЕ: {i['object'].quantitypackageone}"))
                elif i['type'] == 'receipt' and i['object'].quantitypackagedouble > 0:
                    writer.writerow(('', i['time'], i['type'], f"ЧИСЛО ДВОЙНЫХ УПАКОВОК В ЧЕКЕ: {i['object'].quantitypackagedouble}"))
                # NOTE: уведомления/оповещения
                elif i['type'] == 'issue':
                    writer.writerow(('', i['time'], i['type'], i['object'].issue_type))
                elif i['type'] == 'alarm':
                    writer.writerow(('', i['time'], i['type'], i['object'].alarm_type))
                    
                # elif i['task_type'] == '':
                #     writer.writerow(('', 'task_type', i['task_type'], ))
            writer.writerow('')


if __name__ == '__main__':
    request = Get_request(pg_user=PG_USER,
                          pg_password=PG_PASSWORD,
                          pg_host=PG_HOST,
                          pg_db=PG_DB,
                          date_start=DATE_START,
                          date_finish=DATE_FINISH
                          )

    from time import time
    t1 = time()
    asyncio.run(run_app(request))
    print(f"Passed: {round(time() - t1, 2)}")
