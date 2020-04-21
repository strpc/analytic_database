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
    devices = await request.get_devices(status_type_device=2)
    for device in devices:
        device.request = request
        device.alarm_list = await request.get_alarm(device.device_id)
        device.issue_list = await request.get_issue(device.device_id)
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
        del device.alarm_list, device.issue_list, device.suitcases_list #NOTE: optimization memory
        device.line_event.sort(key=lambda d: d['time'])
        grouping_events(device)


def grouping_events(device:Device):
    '''
    Группировка событий по признаку "полный цикл". После каждого полного
    цикла упаковки. Если между событиями прошло больше, чем TIMEDELTA_CHECK
    секунд, то между ними добавляется словарь с ключами:
    'type': 'none', 'ДРОБЛЕНИЕ ПО ПРИЗНАКУ': 'ВРЕМЯ'

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
                                     'issue', 'alarm'}:
                if datetime.now() - block[-1]['time'] > timedelta(
                                                        seconds=LAST_EVENT_TIME):
                    line_event.append(block)

    device.line_event = line_event
    check_broked_events(device)


def check_broked_events(device:Device):
    '''
    Проверка групп на аномалии. Если в группе событий есть хотя бы одно
    аномальное событие(оповещение, уведомление), то она(группа) попадает в
    список device.broken_line_event. В противном случае, она(группа) остается в
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
                event['object'].status = 1
            elif event['type'] == 'issue':
                count_issue += 1
                event['object'].status = 1
            elif event['type'] == 'suitcase_start':
                count_suitcase_start += 1
                event['object'].status = 1

        if (count_alarm != 0 and count_suitcase_start == 0) or \
        (count_issue != 0 and count_suitcase_start == 0):
            block.insert(0, {'task_type': 'уведомление', 'type': 'service'})

        elif count_suitcase_start != 0 and count_alarm == 0 and count_issue == 0:
            block.insert(0, {'task_type': 'КПУ/КнПУ', 'type': 'service'})

        else:
            block.insert(0, {'task_type': 'смешанные', 'type': 'service'})

    adding_attributes(device)

def adding_attributes(device):
    '''
    Добавление аттрибутов для упаковок: csp, unpaid, to_account


    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    for block in device.broken_line_event:
        i = 0
        while len(block) > i:
            if len(block) > i+1 and block[i]['type'] == 'suitcase_start' and \
            block[i+1]['type'] == 'suitcase_finish':
                block[i]['object'].csp = True
                block[i]['object'].unpaid = False
                block[i]['object'].to_account = True
            elif len(block) > i+1 and block[i]['type'] == 'suitcase_start' and \
            block[i+1]['type'] != 'suitcase_finish':
                block[i]['object'].csp = False
                block[i]['object'].unpaid = True
                block[i]['object'].to_account = False
            i += 1
    asyncio.gather(update_database(device))


async def update_database(device:Device):
    '''
    Генерирование информации для будущих записей в бд, создание записей в бд,
    обновление записей в бд.

    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    # broken_line_event
    for block in device.broken_line_event:
        count = 1
        to_task_event = {
            'event_id': '',
            'table_name': '',
            'ord': 0,
            'parent_id': None,
            'created_date': '',
            'task_id': None
        }
        to_task = {
            'date': '',
            'local_date': '',
            'device_id': '',
            'type': ''
        }

        # task data:
        if block[-1]['type'] != 'none':
            to_task['date'] = block[-1]['time']
            to_task['local_date'] = block[-1]['time']
        else:
            to_task['date'] = datetime.now()
            to_task['local_date'] = datetime.now()
        to_task['device_id'] = device.device_id
        to_task['type'] = device.task_type.get(block[0]['task_type'])
        to_task_event['task_id'] = await device.request.create_task(to_task)


        #task_to_event_data:
        for event in block:
            if event['type'] == 'suitcase_start':
                to_task_event['event_id'] = event['object'].polycom_id
                to_task_event['parent_id'] = None
                to_task_event['table_name'] = 'polycomm_suitcase'
                to_task_event['ord'] += 1
                to_task_event['parent_id'] = await device.request.create_task_to_event(to_task_event)

            elif event['type'] == 'issue':
                to_task_event['event_id'] = event['object'].polycommissue_id
                to_task_event['table_name'] = 'polycommissue'
                to_task_event['ord'] += 1

            elif event['type'] == 'alarm':
                to_task_event['event_id'] = event['object'].polycommalarm_id
                to_task_event['table_name'] = 'polycommalarm'
                to_task_event['ord'] += 1

            if event['type'] in {'issue', 'alarm'}:
                await device.request.create_task_to_event(to_task_event)
            await device.request.update_status(event=event)
        if to_task_event['task_id']:
            await device.request.update_status(task_id=to_task_event['task_id'])