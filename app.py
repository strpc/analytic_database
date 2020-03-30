# -*- coding: utf-8 -*-
"""

"""

import asyncpg

import asyncio
import csv
import logging
from datetime import timedelta

from request_database import Get_request, Device
from config import (PG_USER,
                    PG_PASSWORD,
                    PG_HOST,
                    PG_DB,
                    DATE_FORMAT,
                    DATE_START,
                    DATE_FINISH,
                    TIMEDELTA_CHECK)


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


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
    package_type_one = package_type_two = quantitypackageone = quantitypackagedouble = 0
    for block in device.broken_line_event:
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
        
        if count_issue != 0 and count_receipt == count_suitcase_start == 0:
            block.insert(0, {'task_type': 'оповещение', 'type': 'service'})
        
        elif count_receipt != 0 and count_issue == count_suitcase_start == 0:
            block.insert(0, {'task_type': 'чек без упаковок', 'type': 'service'})
        
        elif count_suitcase_start != 0 and count_receipt != 0 and count_issue == 0 and (quantitypackageone >= package_type_one or quantitypackagedouble >= package_type_two): #FIXME:
            block.insert(0, {'task_type': 'чек без упаковок', 'type': 'service'})

        elif count_receipt != 0 and count_issue != 0 and count_suitcase_start == 0:
            block.insert(0, {'task_type': 'чеки без упаковок и уведомление', 'type': 'service'}) 
        
        elif count_suitcase_start != 0 and count_receipt != 0 and count_issue == 0 and (quantitypackageone <= package_type_one or quantitypackagedouble <= package_type_two):
            block.insert(0, {'task_type': 'КПУ/КнПУ', 'type': 'service'})
        
        else:
            block.insert(0, {'task_type': 'смешанные', 'type': 'service'}) 

        count_alarm = count_issue = count_receipt = count_suitcase_start = 0
        package_type_one = package_type_two = quantitypackageone = quantitypackagedouble = 0
        
    
    
    
    view_console(device)
    
    
# list = [[ [] ], [ [] ], [ [] ]]
def add_receipt_to_suitcase_broken(device: Device):
    '''
    Присваивание чека каждой упаковке внутри списка "бракованных" упаковок.
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    
    
    package_type_one = package_type_two = quantitypackageone = quantitypackagedouble = 0
    
    for block in device.broken_line_event:
        if block[0]['task_type'] in {'КПУ/КнПУ', 'чек без упаковок', 'смешанные'}:
            device.list_receipt_with_suitcase.append([])
        
            for event in block:
                if event['type'] == 'receipt':
                    if event['object'].quantitypackageone > 0:
                        quantitypackageone += event['object'].quantitypackageone
                    elif event['object'].quantitypackagedouble > 0:
                        quantitypackagedouble += event['object'].quantitypackagedouble
                elif event['type'] == 'suitcase_start':
                    if event['object'].package_type == 1:
                        package_type_one += 1
                    elif event['object'].package_type == 2:
                        package_type_two += 1
            
            # quantitypackageone - количество одинарных упаковок в чеке
            # quantitypackagedouble - количество двойных упаковок в чеке
            # package_type_one - количество одинарных упаковок
            # package_type_two - количество двойных упаковок
            
            # device.list_receipt_with_suitcase = [ [ [события], [чеки без событий] ], [ [], [] ] ]
            
            for event in block:
                if event['type'] == 'service':
                    device.list_receipt_with_suitcase[0].append(event)
                    
                elif event['type'] == 'suitcase_start' and event['object'].package_type == 1 and quantitypackageone != 0:
                    device.list_receipt_with_suitcase[0].append(event)
                    package_type_one -= 1
                    for i in block:
                        if i['type'] == 'issue':
                            device.list_receipt_with_suitcase[0].append(i)
                            block.remove(i)
                        if i['type'] == 'suitcase_finish':
                            break
                        
                    for i in block:
                        if i['type'] == 'suitcase_finish' and i['object'].package_type == 1:
                            device.list_receipt_with_suitcase[0].append(i)
                            block.remove(i)
                            break
                    for i in block:
                        if i['type'] == 'receipt' and i['object'].quantitypackageone != 0:
                            device.list_receipt_with_suitcase[0].append(i)
                            quantitypackageone -= 1
                            break
                    
                elif event['type'] == 'suitcase_start' and event['object'].package_type == 2 and quantitypackagedouble != 0:
                    device.list_receipt_with_suitcase[0].append(event)
                    package_type_two -= 1
                    for i in block:
                        if i['type'] == 'issue':
                            device.list_receipt_with_suitcase[0].append(i)
                            block.remove(i)
                        if i['type'] == 'suitcase_finish':
                            break
                        
                    for i in block:
                        if i['type'] == 'suitcase_finish' and i['object'].package_type == 2:
                            device.list_receipt_with_suitcase[0].append(i)
                            block.remove(i)
                            break
                        
                    for i in block:
                        if i['type'] == 'receipt' and i['object'].quantitypackagedouble != 0:
                            device.list_receipt_with_suitcase[0].append(i)
                            quantitypackagedouble -= 1
                            break

                else:
                    device.list_receipt_without_suitcase.append(event)

        print('hello')



                
        package_type_one = package_type_two = quantitypackageone = quantitypackagedouble = 0
    
    create_csv(device)
            

                 
def write_csv_temp(device: Device):
    
    with open('list_receipt_without_suitcase.csv', 'a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=";")
        writer.writerow((device.name, ''))
        for i in device.list_receipt_without_suitcase:
            if i['type'] == 'suitcase_start':
                writer.writerow(('', i['time'], 'НАЧАЛО УПАКОВКИ', f"ТИП УПАКОВКИ: {i['object'].package_type}"))
            elif i['type'] == 'suitcase_finish':
                writer.writerow(('', i['time'], "КОНЕЦ УПАКОВКИ"))

            elif i['type'] == 'receipt' and i['object'].quantitypackageone > 0:
                writer.writerow(('', i['time'], i['type'], f"ЧИСЛО ОДИНАРНЫХ УПАКОВОК В ЧЕКЕ: {i['object'].quantitypackageone}"))
            elif i['type'] == 'receipt' and i['object'].quantitypackagedouble > 0:
                writer.writerow(('', i['time'], i['type'], f"ЧИСЛО ДВОЙНЫХ УПАКОВОК В ЧЕКЕ: {i['object'].quantitypackagedouble}"))
                
        writer.writerow(('',''))
    
    

def view_console(device: Device):
    '''
    Визуализация в консоль
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''
    #WARNING: broken_line_event 
    for block in device.broken_line_event:
        for event in block:
            print(block)
            


def create_csv(device: Device):
    '''
    Cоздание CSV-файла с данными.
    
    :param device: элемент списка экземпляров активных устройств класса Device.
    '''

    with open('list_receipt_with_suitcase.csv', 'a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=";")

        for block in device.list_receipt_with_suitcase:
            writer.writerow((device.name, ''))
            for i in block:
                if i['type'] == 'service':
                    writer.writerow("")
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
                # elif i['type'] == 'alarm':
                #     writer.writerow(('', i['time'], i['type'], i['object'].alarm_type))
                    
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