# -*- coding: utf-8 -*-
"""
Запуск программы
"""

import asyncio
from datetime import timedelta, datetime
import csv
from time import sleep

import logger
import dwr_service
from request_database import Request, Device
from config import (PG_USER,
                    PG_PASSWORD,
                    PG_HOST,
                    PG_DB,
                    TIMEDELTA_CHECK,
                    LAST_EVENT_TIME,
                    DEVICE_WITHOUT_RECEIPTS,
                    TIME_SLEEP
                    )

# dev settings:
DATE_START = '2020-01-02'
DATE_FINISH = '2020-01-03'


async def run_app(request:Request):
    '''
    Получение списка устройств для упаковок, типов оповещений, событий к ним,
    добавление всех событий в список словарей. Сортировка словаря.

    :param request: экземпляр класса Get_request().
    '''
    devices = await request.get_devices(status_type_device=1)
    for device in devices:
        device.alarm_list = await request.get_alarm(device.device_id)
        device.issue_list = await request.get_issue(device.device_id)
        device.receipts_list = await request.get_receipts(device.device_id)
        device.suitcases_list = await request.get_suitcases(device.device_id)

        alarm_list_temp = []
        if device.alarm_list and device.suitcases_list:
            for alarm in device.alarm_list:
                for suitcase in device.suitcases_list:
                    if alarm.alarm_time > suitcase.suitcase_start and \
                    alarm.alarm_time < suitcase.suitcase_finish:
                        suitcase.alarm_list.append(alarm)
                    else:
                        alarm_list_temp.append(alarm)
            device.alarm_list = set(alarm_list_temp)
            del alarm_list_temp

        issue_list_temp = []
        if device.issue_list and device.suitcases_list:
            for issue in device.issue_list:
                for suitcase in device.suitcases_list:
                    if issue.issue_time > suitcase.suitcase_start and \
                    issue.issue_time < suitcase.suitcase_finish:
                        suitcase.issue_list.append(issue)
                    else:
                        issue_list_temp.append(issue)
            device.issue_list = set(issue_list_temp)
            del issue_list_temp

            for issue in device.issue_list:
                logger.create('Уведомлению не нашлось упаковки id: '
                f'{issue.polycommissue_id}, device_id: {device.device_id}')

        if device.alarm_list:
            for alarm in device.alarm_list:
                device.line_event.append(
                    {'time': alarm.alarm_time,
                    'object': alarm,
                    'type': 'alarm'
                    }
                )
        if device.issue_list:
            for issue in device.issue_list:
                device.line_event.append(
                    {'time': issue.issue_time,
                    'object': issue,
                    'type': 'issue'
                    }
                )
        if device.receipts_list:
            for receipt in device.receipts_list:
                device.line_event.append(
                    {'time': receipt.receipts_timestamp,
                    'object': receipt,
                    'type': 'receipt'
                    }
                )
        if device.suitcases_list:
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

        del (device.alarm_list, device.issue_list,
             device.receipts_list, device.suitcases_list)
        device.line_event.sort(key=lambda d: d['time'])
        if device.line_event:
            sort_receipts(device)
        else:
            logger.create('Цикл аналитики не был запущен, так как данные для обработки'
                          ' не были загружены. Метод run_app. Возможно устройство'
                          ' не работает. id устройства: {0}, название: {1}'.format(
                                                device.device_id, device.name))


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
    i = 0
    while len(device.line_event) > i:
        if i != len(device.line_event)-1:
            if device.line_event[i]['type'] == 'receipt':
                if len(device.line_event) > i+1 and device.line_event[i+1]['type'] in {'suitcase_start', 'issue', 'alarm'}:
                    device.line_event.insert(i+1, {"type": "none"})
            elif len(device.line_event) > i+1 and device.line_event[i]['type'] != 'none' and device.line_event[i+1]['time'] - device.line_event[i]['time'] > timedelta(seconds=TIMEDELTA_CHECK):
                device.line_event.insert(i+1, {"type": "none", "ДРОБЛЕНИЕ ПО ПРИЗНАКУ": "ВРЕМЯ"})
        i += 1
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
    Проверка упаковок на наличие чеков к ним
    device.broken_line_event - список бракованных упаковок.
    device.line_event - список успешных упаковок.

    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    broken_line_event = []
    line_event = []
    for block in device.line_event:
        packages_one = packages_double = 0
        quantitypackageone = quantitypackagedouble = 0
        for event in block:
            if event['type'] == 'suitcase_start' and \
            event['object'].package_type == 1:
                packages_one += 1
            if event['type'] == 'suitcase_start' and \
            event['object'].package_type == 2:
                packages_double += 1

            if event['type'] == 'receipt':
                quantitypackageone += event['object'].quantitypackageone
                quantitypackagedouble += event['object'].quantitypackagedouble

        if packages_one != quantitypackageone or \
        packages_double != quantitypackagedouble:
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
        count_alarm = count_issue = count_receipt = count_suitcase_start = 0
        package_type_one = package_type_two = quantitypackageone = quantitypackagedouble = 0
        for event in block:
            if event['type'] == 'alarm':
                count_alarm += 1
            elif event['type'] == 'issue':
                count_issue += 1

            elif event['type'] == 'receipt':
                count_receipt += 1
                if event['object'].quantitypackageone > 0:
                    quantitypackageone += event['object'].quantitypackageone
                if event['object'].quantitypackagedouble > 0:
                    quantitypackagedouble += event['object'].quantitypackagedouble
            elif event['type'] == 'suitcase_start':
                count_suitcase_start += 1
                if event['object'].package_type == 1:
                    package_type_one += 1
                if event['object'].package_type == 2:
                    package_type_two += 1

        if count_alarm != 0 and count_receipt == count_suitcase_start == 0:
            block.insert(0, {'task_type': 'уведомление', 'type': 'service'})

        elif count_receipt != 0 and count_suitcase_start == 0:
            block.insert(0, {'task_type': 'чек без упаковки', 'type': 'service'})

        elif count_suitcase_start != 0 and count_receipt != 0 and quantitypackageone >= package_type_one and quantitypackageone != 0 and quantitypackagedouble >= package_type_two and quantitypackagedouble != 0:
            block.insert(0, {'task_type': 'чек без упаковки', 'type': 'service'})

        elif count_receipt != 0 and count_alarm != 0 and count_suitcase_start == 0:
            block.insert(0, {'task_type': 'чеки без упаковок и уведомления', 'type': 'service'})

        elif count_suitcase_start != 0 and count_receipt == 0 or count_suitcase_start != 0 and count_receipt != 0 and quantitypackageone <= package_type_one and quantitypackagedouble <= package_type_two:
            block.insert(0, {'task_type': 'КПУ/КнПУ', 'type': 'service'})

        else:
            block.insert(0, {'task_type': 'смешанные', 'type': 'service'})


    # line_event
    for block in device.line_event:
        count_alarm = count_issue = count_receipt = count_suitcase_start = 0
        package_type_one = package_type_two = quantitypackageone = quantitypackagedouble = 0
        for event in block:
            if event['type'] == 'alarm':
                count_alarm += 1
            elif event['type'] == 'issue':
                count_issue += 1

            elif event['type'] == 'receipt':
                count_receipt += 1
                if event['object'].quantitypackageone > 0:
                    quantitypackageone += event['object'].quantitypackageone
                if event['object'].quantitypackagedouble > 0:
                    quantitypackagedouble += event['object'].quantitypackagedouble
            elif event['type'] == 'suitcase_start':
                count_suitcase_start += 1
                if event['object'].package_type == 1:
                    package_type_one += 1
                if event['object'].package_type == 2:
                    package_type_two += 1

        if count_alarm != 0 and count_receipt == count_suitcase_start == 0:
            block.insert(0, {'task_type': 'уведоление', 'type': 'service'})

        elif count_receipt != 0 and count_suitcase_start == 0:
            block.insert(0, {'task_type': 'чек без упаковки', 'type': 'service'})

        elif count_suitcase_start != 0 and count_receipt != 0 and quantitypackageone >= package_type_one and quantitypackageone != 0 and quantitypackagedouble >= package_type_two and quantitypackagedouble != 0:
            block.insert(0, {'task_type': 'чек без упаковки', 'type': 'service'})

        elif count_receipt != 0 and count_alarm != 0 and count_suitcase_start == 0:
            block.insert(0, {'task_type': 'чеки без упаковок и уведомления', 'type': 'service'})

        elif count_suitcase_start != 0 and count_receipt == 0 or count_suitcase_start != 0 and count_receipt != 0 and quantitypackageone <= package_type_one and quantitypackagedouble <= package_type_two:
            block.insert(0, {'task_type': 'КПУ/КнПУ', 'type': 'service'})

        else:
            block.insert(0, {'task_type': 'смешанные', 'type': 'service'})



    receipt_broken_line_event_sync(device)


def receipt_broken_line_event_sync(device:Device):
    '''
    Присваивание чека каждой упаковке событий списка broken_line_event

    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    for block in device.broken_line_event:
        count_packageone = count_packagedouble = 0
        if block[0]['task_type'] in {'КПУ/КнПУ', 'чек без упаковок', 'смешанные'}:
            for event in block[::-1]:

                if event['type'] == 'receipt':
                    count_packageone = event['object'].quantitypackageone
                    count_packagedouble = event['object'].quantitypackagedouble

                    i = len(block) - 1
                    while i >= 0:
                        if block[i]['type'] == 'suitcase_finish' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type == 1 and count_packageone != 0 and block[i-1]['type'] == 'suitcase_start':
                            block[i]['object'].receipt_id = event['object'].receipt_id
                            block[i]['object'].package_type_by_receipt = 1
                            count_packageone -= 1
                            event['object'].count_packageone -= 1
                            if count_packageone == count_packagedouble == 0:
                                break

                        elif block[i]['type'] == 'suitcase_finish' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type == 2 and count_packagedouble != 0 and block[i-1]['type']  == 'suitcase_start':
                            block[i]['object'].receipt_id = event['object'].receipt_id
                            block[i]['object'].package_type_by_receipt = 2
                            count_packagedouble -= 1
                            event['object'].count_packagedouble -= 1
                            if count_packageone == count_packagedouble == 0:
                                break

                        elif block[i]['type'] == 'suitcase_finish' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type in {1, 2, None, 'None'} and block[i-1]['type'] == 'suitcase_start' and count_packagedouble == 0 and count_packageone == 0:
                            block[i]['object'].receipt_id = -1
                            if count_packageone == count_packagedouble == 0:
                                break
                        i -= 1

                    i = len(block) - 1
                    while i >= 0:
                        if block[i]['type'] == 'suitcase_finish' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type == 1 and count_packageone != 0 and block[i-1]['type'] != 'suitcase_start':
                            block[i]['object'].receipt_id = event['object'].receipt_id
                            block[i]['object'].package_type_by_receipt = 1
                            count_packageone -= 1
                            event['object'].count_packageone -= 1
                            if count_packageone == count_packagedouble == 0:
                                break

                        elif block[i]['type'] == 'suitcase_finish' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type == 2 and count_packagedouble != 0 and block[i-1]['type']  != 'suitcase_start':
                            block[i]['object'].receipt_id = event['object'].receipt_id
                            block[i]['object'].package_type_by_receipt = 2
                            count_packagedouble -= 1
                            event['object'].count_packagedouble -= 1
                            if count_packageone == count_packagedouble == 0:
                                break

                        elif block[i]['type'] == 'suitcase_finish' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type in {1, 2, None, 'None'} and block[i-1]['type']  != 'suitcase_start' and (count_packagedouble == 0 and count_packageone == 0):
                            block[i]['object'].receipt_id = -1
                            if count_packageone == count_packagedouble == 0:
                                break
                        i -= 1

                    if count_packageone != 0 or count_packagedouble != 0:
                        i = len(block) - 1
                        while i >= 0 and (count_packageone > 0 or count_packagedouble > 0):
                            if count_packageone != 0 and block[i]['type'] == 'suitcase_finish' and block[i]['object'].receipt_id == '':
                                block[i]['object'].receipt_id = event['object'].receipt_id
                                block[i]['object'].package_type_by_receipt = 1
                                count_packageone -= 1
                                event['object'].count_packageone -= 1

                            elif count_packagedouble != 0 and block[i]['type'] == 'suitcase_finish' and block[i]['object'].receipt_id == '':
                                block[i]['object'].receipt_id = event['object'].receipt_id
                                block[i]['object'].package_type_by_receipt = 2
                                count_packagedouble -= 1
                                event['object'].count_packagedouble -= 1
                            i -= 1

        for event in block:
            if event['type'] == 'suitcase_finish' and event['object'].receipt_id == '':
                event['object'].receipt_id = -1
    receipt_line_event_sync(device)


def receipt_line_event_sync(device:Device):
    '''
    Присваивание чека каждой упаковке событий списка line_event(события без "претензий").

    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    for block in device.line_event:
        count_packageone = count_packagedouble = 0
        for event in block:

            if event['type'] == 'receipt':
                count_packageone = event['object'].quantitypackageone
                count_packagedouble = event['object'].quantitypackagedouble

                i = 0
                while len(block) > i:
                    if len(block) > i+1 and block[i]['type'] == 'suitcase_start' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type == 1 and count_packageone != 0 and block[i+1]['type'] == 'suitcase_finish':
                        block[i]['object'].receipt_id = event['object'].receipt_id
                        block[i]['object'].package_type_by_receipt = 1
                        count_packageone -= 1
                        if count_packageone == count_packagedouble == 0:
                            break

                    elif len(block) > i+1 and block[i]['type'] == 'suitcase_start' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type == 2 and count_packagedouble != 0 and block[i+1]['type']  == 'suitcase_finish':
                        block[i]['object'].receipt_id = event['object'].receipt_id
                        block[i]['object'].package_type_by_receipt = 2
                        count_packagedouble -= 1
                        if count_packageone == count_packagedouble == 0:
                            break

                    elif len(block) > i+1 and block[i]['type'] == 'suitcase_start' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type in {1, 2, None, 'None'} and block[i+1]['type'] == 'suitcase_finish' and count_packagedouble == 0 and count_packageone == 0:
                        block[i]['object'].receipt_id = -1
                        if count_packageone == count_packagedouble == 0:
                            break
                    i += 1

                i = 0
                while len(block) > i:
                    if len(block) > i+1 and block[i]['type'] == 'suitcase_start' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type == 1 and count_packageone != 0 and block[i+1]['type'] != 'suitcase_finish':
                        block[i]['object'].receipt_id = event['object'].receipt_id
                        block[i]['object'].package_type_by_receipt = 1
                        count_packageone -= 1
                        if count_packageone == count_packagedouble == 0:
                            break

                    elif len(block) > i+1 and block[i]['type'] == 'suitcase_start' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type == 2 and count_packagedouble != 0 and block[i+1]['type']  != 'suitcase_finish':
                        block[i]['object'].receipt_id = event['object'].receipt_id
                        block[i]['object'].package_type_by_receipt = 2
                        count_packagedouble -= 1
                        if count_packageone == count_packagedouble == 0:
                            break

                    elif len(block) > i+1 and block[i]['type'] == 'suitcase_start' and block[i]['object'].receipt_id == '' and block[i]['object'].package_type in {1, 2, None, 'None'} and block[i+1]['type']  != 'suitcase_finish' and (count_packagedouble == 0 and count_packageone == 0):
                        block[i]['object'].receipt_id = -1
                        if count_packageone == count_packagedouble == 0:
                            break
                    i += 1

                if count_packageone != 0 or count_packagedouble != 0:
                    i = 0
                    while len(block) > i and (count_packageone > 0 or count_packagedouble > 0):
                        if count_packageone != 0 and block[i]['type'] == 'suitcase_start' and block[i]['object'].receipt_id == '':
                            block[i]['object'].receipt_id = event['object'].receipt_id
                            block[i]['object'].package_type_by_receipt = 1
                            count_packageone -= 1
                        elif count_packagedouble != 0 and block[i]['type'] == 'suitcase_start' and block[i]['object'].receipt_id == '':
                            block[i]['object'].receipt_id = event['object'].receipt_id
                            block[i]['object'].package_type_by_receipt = 2
                            count_packagedouble -= 1
                        i += 1

        for event in block:
            if event['type'] == 'suitcase_start' and event['object'].receipt_id == '':
                event['object'].receipt_id = -1
    adding_attributes(device)


def adding_attributes(device:Device):
    '''
    Добавление аттрибутов для будущих уведомлений

    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    def add_template(event):
        event['object'].issue_attrib['id'] = 22222222
        event['object'].issue_attrib['total'] = event['object'].totalid
        event['object'].issue_attrib['suitcase'] = event['object'].polycom_id
        event['object'].issue_attrib['date'] = event['object'].suitcase_finish
        event['object'].issue_attrib['localdate'] = event['object'].suitcase_finish


    for block in device.broken_line_event:

        for event in block:
            if event['type'] == 'suitcase_start':

                 # КПУ неоплаченная:
                if event['object'].receipt_id == -1 and not event['object'].issue_list and not event['object'].alarm_list:
                    event['object'].csp = True
                    event['object'].unpaid = True
                    event['object'].to_account = True
                    event['object'].issue_attrib['type'] = 7
                    add_template(event)

                #КПУ оплаченная
                elif event['object'].receipt_id not in {-1, None} and not event['object'].issue_list and not event['object'].alarm_list:
                    event['object'].csp = True
                    event['object'].unpaid = False
                    event['object'].to_account = True

                    if event['object'].package_type_by_receipt == 1 and event['object'].package_type == 2:
                        event['object'].issue_attrib['type'] = 8
                        add_template(event)

                    elif event['object'].package_type_by_receipt == 2 and event['object'].package_type == 1:
                        event['object'].issue_attrib['type'] = 9
                        add_template(event)

                elif event['object'].issue_list or event['object'].alarm_list:
                    #КнПУ и неоплаченная
                    if event['object'].receipt_id == -1:
                        event['object'].csp = False
                        event['object'].unpaid = True
                        event['object'].to_account = False

                    #КПУ оплаченная
                    elif event['object'].receipt_id != -1:
                        event['object'].csp = True
                        event['object'].unpaid = False
                        event['object'].to_account = True

                        if event['object'].package_type == 2 and event['object'].package_type_by_receipt == 1:
                            event['object'].issue_attrib['type'] = 8
                            add_template(event)

                        elif event['object'].package_type == 1 and event['object'].package_type_by_receipt == 2:
                            event['object'].issue_attrib['type'] = 9
                            add_template(event)

                else: # подстраховка
                    event['object'].csp = True
                    event['object'].unpaid = True
                    event['object'].to_account = True
                    event['object'].issue_attrib['type'] = 7
                    add_template(event)

    # create_csv(device)
    asyncio.gather(update_database(device))


async def update_database(device:Device):
    '''
    Генерирование информации для будущих записей в бд, создание записей в бд.

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
        i = 0
        while len(block) > i:
            if block[i]['type'] == 'suitcase_start' and \
            block[i]['object'].issue_attrib.get('type') in {7, 8, 9}:
                await request.create_polycommissue_event(block[i]['object'])
            if block[i]['type'] == 'suitcase_start':
                block[i]['object'].status = 1

            elif block[i]['type'] in {'alarm', 'issue'}:
                block[i]['object'].status = 1
            elif block[i]['type'] == 'receipt':
                if block[i]['object'].count_packageone != 0 or \
                block[i]['object'].count_packagedouble != 0:
                    block[i]['object'].status = 2
                else:
                    block[i]['object'].status = 1
            i += 1


        # task data:
        if block[-1]['type'] == 'alarm':
            to_task['date'] = block[-1]['object'].moscow_date
            to_task['local_date'] = block[-1]['object'].alarm_time

        elif block[-1]['type'] == 'issue':
            to_task['date'] = block[-1]['object'].moscow_date
            to_task['local_date'] = block[-1]['object'].issue_time

        elif block[-1]['type'] == 'suitcase_start' or \
        block[-1]['type'] == 'suitcase_finish':
            to_task['date'] = block[-1]['object'].moscow_date
            to_task['local_date'] = block[-1]['object'].suitcase_finish

        elif block[-1]['type'] == 'receipt':
            to_task['date'] = block[-1]['object'].dateclosemoscow
            to_task['local_date'] = block[-1]['object'].receipts_timestamp

        to_task['device_id'] = device.device_id
        to_task['type'] = device.task_type.get(block[0]['task_type'])
        to_task_event['task_id'] = await request.create_task(to_task)

        #task_to_event_data:
        for event in block:
            if event['type'] == 'suitcase_start':
                to_task_event['event_id'] = event['object'].polycom_id
                to_task_event['parent_id'] = None
                to_task_event['table_name'] = 'polycomm_suitcase'
                to_task_event['ord'] += 1
                to_task_event['parent_id'] = await request.create_task_to_event(to_task_event)

            elif event['type'] == 'issue':
                to_task_event['event_id'] = event['object'].polycommissue_id
                to_task_event['table_name'] = 'polycommissue'
                to_task_event['ord'] += 1

            elif event['type'] == 'alarm':
                to_task_event['event_id'] = event['object'].polycommalarm_id
                to_task_event['table_name'] = 'polycommalarm'
                to_task_event['ord'] += 1

            elif event['type'] == 'receipt':
                to_task_event['event_id'] = event['object'].receipt_id
                to_task_event['table_name'] = 'receipts'
                to_task_event['ord'] += 1
                to_task_event['parent_id'] = None

            if event['type'] in {'issue', 'alarm', 'receipt'}:
                await request.create_task_to_event(to_task_event)
            await request.update_status(event=event)
        if to_task_event['task_id']:
            await request.update_status(task_id=to_task_event['task_id'])


    # line event:
    for block in device.line_event:
        count = 1
        to_task = {
            'date': '',
            'local_date': '',
            'device_id': '',
            'type': ''
        }
        to_task_event = {
            'event_id': '',
            'table_name': '',
            'ord': 0,
            'parent_id': None,
            'created_date': '',
            'task_id': None
        }

        for event in block:
            if event['type'] in {'suitcase_start', 'issue', 'alarm', 'receipt'}:
                event['object'].status = 1

        # task data:
        if block[-1]['type'] == 'receipt':
            to_task['date'] = block[-1]['object'].dateclosemoscow
            to_task['local_date'] = block[-1]['object'].receipts_timestamp
        else:
            to_task['date'] = datetime.now()
            to_task['local_date'] = datetime.now()
        to_task['device_id'] = device.device_id
        to_task['type'] = device.task_type.get(block[0]['task_type'])
        to_task_event['task_id'] = await request.create_task(to_task)

        #task_to_event_data:
        for event in block:
            if event['type'] == 'suitcase_start':
                to_task_event['event_id'] = event['object'].polycom_id
                to_task_event['parent_id'] = None
                to_task_event['table_name'] = 'polycomm_suitcase'
                to_task_event['ord'] += 1
                to_task_event['parent_id'] = await request.create_task_to_event(to_task_event)
                event['object'].csp = True
                event['object'].unpaid = False
                event['object'].in_task = False
                event['object'].to_account = True

            elif event['type'] == 'issue':
                to_task_event['event_id'] = event['object'].polycommissue_id
                to_task_event['table_name'] = 'polycommissue'
                to_task_event['ord'] += 1

            elif event['type'] == 'alarm':
                to_task_event['event_id'] = event['object'].polycommalarm_id
                to_task_event['table_name'] = 'polycommalarm'
                to_task_event['ord'] += 1

            elif event['type'] == 'receipt':
                to_task_event['event_id'] = event['object'].receipt_id
                to_task_event['table_name'] = 'receipts'
                to_task_event['ord'] += 1

            if event['type'] in {'issue', 'alarm', 'receipt'}:
                await request.create_task_to_event(to_task_event)
            await request.update_status(event=event)
        if to_task_event['task_id']:
            await request.update_status_and_resolved(
                                            task_id=to_task_event['task_id'])


def create_csv(device:Device):
    '''
    Cоздание CSV-файла с данными.

    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    with open('suitcases_broken.csv', 'a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=";")
        fieldnames = ['status', 'id', 'type', 'date', 'total', 'suitcase', 'localdate']

        for block in device.broken_line_event:
            writer.writerow((device.name, ''))
            for i in block:

                if i['type'] == 'service':
                    writer.writerow(('','task_type:', i['task_type']))
                elif i['type'] == 'broken':
                    writer.writerow((i['type'], f"ПРЕВЫШЕНИЕ ПО ВРЕМЕНИ МЕЖДУ УПАКОВКАМИ {TIMEDELTA_CHECK}"))

                elif i['type'] == 'suitcase_start':
                    writer.writerow(('', i['time'], 'НАЧАЛО УПАКОВКИ', f"ТИП УПАКОВКИ: {i['object'].package_type}", f"номер чека: {i['object'].receipt_id}", f"тип чека: {i['object'].package_type_by_receipt}", f"polycom_id = {i['object'].polycom_id}"))
                    if i['object'].issue_attrib.get('id'):
                        writer.writerow((f"id: {i['object'].issue_attrib['id']}",
                                        f"total: {i['object'].issue_attrib['total']}",
                                        f"suitcase: {i['object'].issue_attrib['suitcase']}",f"date: {i['object'].issue_attrib['date']}",
                                        f"localdate: {i['object'].issue_attrib['localdate']}"))
                        writer.writerow((f"type: {i['object'].issue_attrib['type']}",))

                    writer.writerow((f"csp: {i['object'].csp}",
                                    f"unpaid: {i['object'].unpaid}",
                                    f"to_account: {i['object'].to_account}"))


                elif i['type'] == 'suitcase_finish':
                    writer.writerow(('', i['time'], "КОНЕЦ УПАКОВКИ"))
                    writer.writerow('')

                elif i['type'] == 'receipt':
                    writer.writerow(('', i['time'], i['type'], f"ЧИСЛО ОДИНАРНЫХ УПАКОВОК В ЧЕКЕ: {i['object'].quantitypackageone}", f"ЧИСЛО ДВОЙНЫХ УПАКОВОК В ЧЕКЕ: {i['object'].quantitypackagedouble}", f"номер чека {i['object'].receipt_id}"))

                # NOTE: уведомления/оповещения
                elif i['type'] == 'issue':
                    writer.writerow(('', i['time'], i['type'], i['object'].issue_type))

                elif i['type'] == 'alarm':
                    writer.writerow(('', i['time'], i['type'], i['object'].alarm_type))
            writer.writerow('')


if __name__ == '__main__':
    request = Request(pg_user=PG_USER,
                      pg_password=PG_PASSWORD,
                      pg_host=PG_HOST,
                      pg_db=PG_DB,
                      date_start=DATE_START, #dev
                      date_finish=DATE_FINISH
                      )


    while True:
        # from time import time #dev
        # t1 = time()

        asyncio.run(run_app(request))
        if DEVICE_WITHOUT_RECEIPTS:
            asyncio.run(dwr_service.run_app(request))
        sleep(TIME_SLEEP)

        # print(f"Passed: {round(time() - t1, 2)}")
