import asyncio
from datetime import timedelta, datetime
import csv

from request_database import Request, Device
from config import TIMEDELTA_CHECK, LAST_EVENT_TIME


async def run_app(request:Request):
    '''
    Получение списка устройств для упаковок, типов оповещений, событий к ним,
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
        del device.alarm_list, device.issue_list, device.receipts_list, device.suitcases_list #NOTE: optimization memory
        device.line_event.sort(key=lambda d: d['time'])
        sort_receipts(device)


def sort_receipts(device:Device):
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
    

def grouping_events(device:Device):
    '''
    Группировка событий по признаку "полный цикл". Добавление события "none"
    после каждого полного цикла упаковки. Проверка на время между упаковками
    (не более TIMEDELTA_CHECK). 
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    
    new_list = []
    if len(device.line_event) != 0: last_time = device.line_event[0]['time'] 
    for event in device.line_event:
        if event['type'] != 'suitcase_finish':
            if event['time'] - last_time > timedelta(seconds=TIMEDELTA_CHECK):
                new_list.append({"type": "none", "ДРОБЛЕНИЕ ПО ПРИЗНАКУ": "ВРЕМЯ"})
        last_time = event['time']
        new_list.append(event)
    device.line_event = new_list
    listing_events(device)
    

def listing_events(device:Device):
    '''
    Создание списка списков словарей. Каждый элемент главного списка - это
    список, который содержит в себе n-элементов словарей событий полного
    цикла упаковки.
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    new_list = [[]]
    j = 0
    for event in device.line_event:
        if event['type'] == 'none':
            new_list.append([])
            j += 1
        else:
            new_list[j].append(event)
    
    device.line_event = new_list
    check_last_event_time(device)


def check_last_event_time(device:Device):
    '''
    Проверка последнего события на предмет того, что оно произошло раньше, чем
    LAST_EVENT_TIME(900 сек или 15 минут).
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    line_event = []
    for block in device.line_event:
        if len(block) != 0:
            if block[-1]['type'] in {'suitcase_start', 'suitcase_finish', 
                                     'issue', 'alarm', 'receipt'}:
                if datetime.now() - block[-1]['time'] > timedelta(
                                                        seconds=LAST_EVENT_TIME):
                    line_event.append(block)
                    
    device.line_event = line_event
    check_broked_events(device)


def check_broked_events(device:Device):
    '''
    Проверка групп на аномалии. Если в группе событий есть хотя бы одно 
    аномальное событие, то она(группа) попадает в список 
    device.broken_line_event. В противном случае, она(группа) остается в 
    device.line_event.
    
    device.broken_line_event - список бракованных упаковок.
    device.line_event - список успешных упаковок.
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    broken_line_event = []
    line_event = []
    for block in device.line_event:
        issues = alarms = 0
        for event in block:
            if event['type'] == 'issue':
                issues += 1
            if event['type'] == 'alarm':
                alarms += 1
                
        if issues != 0 or alarms != 0:
            broken_line_event.append(block)
        else:
            line_event.append(block)
    device.line_event = line_event
    device.broken_line_event = broken_line_event

    add_task(device)


def add_task(device:Device):
    '''
    Добавления типа задачи:
    оповещение, чек без упаковок, чеки без упаковок и уведомление, 
    КПУ/КнПУ, смешанные
    в ключ словаря task_type
    {'task_type': '% %', 'type': 'service'}
    для каждого блока с событиями
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    #broken_line_event
    for block in device.broken_line_event:
        count_alarm = count_issue = count_suitcase_start = 0
        for event in block:
            if event['type'] == 'alarm':
                count_alarm += 1
            elif event['type'] == 'issue':
                count_issue += 1
            
            elif event['type'] == 'suitcase_start':
                count_suitcase_start += 1
        
        if (count_alarm != 0 and count_suitcase_start == 0) or \
        (count_issue != 0 and count_suitcase_start == 0):
            block.insert(0, {'task_type': 'уведомление', 'type': 'service'})
        
        elif count_suitcase_start != 0 and count_alarm == 0 and count_issue == 0:
            block.insert(0, {'task_type': 'КПУ/КнПУ', 'type': 'service'})
            
        else:
            block.insert(0, {'task_type': 'смешанные', 'type': 'service'}) 

