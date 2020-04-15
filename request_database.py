import asyncio
from datetime import datetime

import asyncpg

import logger


class Alarm():
    '''Класс, содержащий информацию о уведомлениях'''
    def __init__(self, polycommalarm_id, alarm_time, 
                 alarm_device_id, alarm_type, status):
        self.polycommalarm_id = polycommalarm_id
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
        self.task_type = {
            'уведомление': 1,
            'чек без упаковки': 2,
            'смешанные': 5,
            'КПУ/КнПУ': 4,
            'чеки без упаковок и уведомления': 3,
            'Работа двигателя без упаковок Polycomm': 6,
            }

    def __repr__(self):
        return f'{self.device_id}, {self.name}, {self.receipts}'

    def __str__(self):
        return f'{self.device_id}, {self.name}, {self.receipts}'


class Issue():
    '''Класс, содержащий информацию о оповещениях.'''
    def __init__(self, suitcase_id, issue_time, device_id, issue_type, status,
                 polycommissue_id):
        self.suitcase_id = suitcase_id
        self.issue_time = issue_time
        self.device_id = device_id
        self.issue_type = issue_type
        self.status = status
        self.polycommissue_id = polycommissue_id

    def __str__(self):
        return f'{self.issue_type}'

    def __repr__(self):
        return f'{self.issue_type}'
    

class Receipts():
    '''Класс, содержащий информацию о чеках.'''
    def __init__(self, receipt_id, receipts_timestamp, device_id, 
                 quantitypackageone, quantitypackagedouble, status,
                 dateclosemoscow):
        self.receipt_id = receipt_id
        self.receipts_timestamp = receipts_timestamp
        self.device_id = device_id
        self.quantitypackageone = quantitypackageone
        self.quantitypackagedouble = quantitypackagedouble
        self.count_packageone = quantitypackageone
        self.count_packagedouble = quantitypackagedouble
        self.status = status
        self.dateclosemoscow = dateclosemoscow

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
                 date_start, date_finish): #FIXME: убрать время
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_host = pg_host
        self.pg_db = pg_db
        self.date_start = date_start #FIXME: убрать время
        self.date_finish = date_finish
    
    
    async def _connect_database(self):
        '''Создание подключения к базе.'''
        try:
            conn = await asyncpg.connect(
                f'postgresql://{self.pg_user}:{self.pg_password}'
                            f'@{self.pg_host}/{self.pg_db}')
            return conn
        
        except Exception as e:
            logger.create('Произошла ошибка при попытке подключения к'
                          ' базе данных. Метод _connect_database', e)
            return False


    async def get_devices(self):
        '''Получение списка активных устройств упаковки из базы. 
        Создание списка экземпляров классов Device'''
        conn = await self._connect_database()
        if conn:
            try:
                rows = await conn.fetch('\
                    SELECT id, title FROM polycomm_device \
                    ORDER BY id; \
                    ')
                        #            dev: INNER JOIN timestamps \
                        # on polycomm_device.code = CAST(timestamps.devicecode as int) \
                        # and timestamps.ready=True and timestamps.status_type_device=1 \
            except Exception as e:
                logger.create('Произошла ошибка при получении списка активных'
                              ' устройств из базы. Метод get_devices', e)
                return False
            finally:
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
        if conn:
            try:
                rows = await conn.fetch(f"\
                    SELECT polycommalarm_id, localdate, device, \
                        polycomm_alarm_type.title, polycommalarm.status \
                    FROM polycommalarm \
                    INNER JOIN polycomm_alarm_type ON \
                        polycomm_alarm_type.id = polycommalarm.alarmtype \
                    WHERE localdate > timestamp '{self.date_start}' \
                        and localdate < timestamp '{self.date_finish}' \
                        and device = {device_id} \
                        and status = 0 \
                    ORDER BY localdate \
                    ") #FIXME: убрать время
            except Exception as e:
                logger.create('Произошла ошибка при получении списка '
                              'уведомлений из базы. Метод get_alarm', e)
                return False
            finally:
                await conn.close()

        alarm_list = list()
        for row in rows:
            alarm_list.append(Alarm(
                polycommalarm_id=row['polycommalarm_id'],
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
        if conn:
            try:
                rows = await conn.fetch(f"\
                    SELECT suitcase, localdate, device, polycomm_issue_type.title, \
                        status, polycommissue_id \
                    FROM polycommissue \
                    INNER JOIN polycomm_issue_type ON \
                        polycommissue.type = polycomm_issue_type.id \
                    WHERE localdate > timestamp '{self.date_start}' \
                        and localdate < timestamp '{self.date_finish}' \
                        and device = {device_id} \
                        and status = 0 \
                    ORDER BY localdate \
                    ") #FIXME: убрать время
            except Exception as e:
                logger.create('Произошла ошибка при получении списка '
                              'оповещений из базы. Метод get_issue', e)
                return False
            finally:
                await conn.close()

        issue_list = list()
        for row in rows:
            issue_list.append(Issue(suitcase_id=row['suitcase'],
                                issue_time=row['localdate'],
                                device_id=row['device'],
                                issue_type=row['title'],
                                status=row['status'],
                                polycommissue_id=row['polycommissue_id']
                                ))
        return issue_list.copy()


    async def get_receipts(self, device_id):
        '''
        Получение чеков из базы. 
        Создание списка экземпляров классов Receipts
        
        :param device_id: - id активного устройства, полученный
        методом get_devices()
        '''
        conn = await self._connect_database()
        if conn:
            try:
                rows = await conn.fetch(f"\
                SELECT DISTINCT receipts.receipt_id, dateclose, polycomm_device.id, \
                    receipts.quantitypackageone, receipts.quantitypackagedouble, \
                    receipts.status, receipts.dateclosemoscow \
                FROM receipts \
                LEFT JOIN polycomm_device on \
                    CAST(receipts.devicecode as int) = polycomm_device.code \
                WHERE dateclose > timestamp '{self.date_start}' \
                    and dateclose < timestamp '{self.date_finish}' \
                    and polycomm_device.id = {device_id} \
                    and status = 0 \
                ORDER BY dateclose") #FIXME: убрать время
            except Exception as e:
                logger.create('Произошла ошибка при получении списка '
                              'чеков из базы. Метод get_receipts', e)
                return False
            finally:
                await conn.close()

        receipts_list = list()
        for row in rows:
            receipts_list.append(Receipts(receipt_id=row['receipt_id'],
                            receipts_timestamp=row['dateclose'],
                            device_id=row['id'],
                            quantitypackageone=row['quantitypackageone'],
                            quantitypackagedouble=row['quantitypackagedouble'],
                            status=row['status'],
                            dateclosemoscow=row['dateclosemoscow']))
        return receipts_list.copy()
    

    async def get_suitcases(self, device_id):
        '''
        Получение упаковок из базы. 
        Создание списка экземпляров классов Suitcase
        
        :param device_id: - id активного устройства, полученный
        методом get_devices()
        '''
        conn = await self._connect_database()
        if conn:
            try:
                rows = await conn.fetch(f"\
                SELECT id, dateini_local, local_date, package_type, \
                    polycom_id, totalid, status, duration \
                FROM polycomm_suitcase \
                WHERE \
                    dateini_local > timestamp '{self.date_start}' \
                    and dateini_local < timestamp '{self.date_finish}' \
                    and device_id = {device_id} \
                    and status = 0 \
                ORDER BY dateini_local;") #FIXME: убрать время
            except Exception as e:
                logger.create('Произошла ошибка при получении списка '
                              'упаковок из базы. Метод get_suitcases', e)
                return False
            finally:
                await conn.close()

        suitcases_list = list()
        for row in rows:
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
        conn = await self._connect_database() #control_db
        # conn = await asyncpg.connect('postgresql://postgres:1@localhost/test')
        if conn:
            try:
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
                VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9);",
                    event.issue_list['id'],
                    event.issue_list['localdate'],
                    event.device_id,
                    event.issue_list['total'],
                    event.issue_list['suitcase'],
                    event.duration,
                    event.issue_list['type'],
                    event.issue_list['date'],
                    datetime.now())
                await conn.close()
            except Exception as e:
                logger.create('Произошла ошибка при создании записи '
                              'в сущности polycommissue. '
                              'Метод create_polycommissue_event', e)
                await conn.close()
                return False
    
    
    async def create_task(self, to_task):
        '''
        Создание записи в таблице task.
        
        :param to_task: - словарь с нужными для записи в таблицу данными.
        '''
        conn = await self._connect_database() #control_db
        # conn = await asyncpg.connect('postgresql://postgres:1@localhost/test')
        if conn:
            try:
                task_id = await conn.fetchval("\
                INSERT INTO task(date, \
                                local_date, \
                                device_id, \
                                type) \
                VALUES($1, $2, $3, $4)\
                RETURNING id;",
                    to_task['date'],
                    to_task['local_date'],
                    to_task['device_id'],
                    to_task['type']
                    )
                await conn.close()
                print('task\'s created')
                return task_id
            except Exception as e:
                logger.create('Произошла ошибка при создании записи '
                              'в сущности task. Метод create_task', e)
                await conn.close()
                return False
        
        
    async def create_task_to_event(self, to_task_event):
        '''
        Создание записи в таблице task_to_event.
        
        :param to_task_event: - словарь с нужными для записи в таблицу данными.
        '''
        conn = await self._connect_database() #control_db
        # conn = await asyncpg.connect('postgresql://postgres:1@localhost/test')
        if conn:
            try:
                parent_id = await conn.fetchval("\
                INSERT INTO task_to_event(event_id, \
                                        table_name, \
                                        ord, \
                                        parent_id, \
                                        created_date, \
                                        task_id) \
                VALUES($1, $2, $3, $4, $5, $6)\
                RETURNING id;",
                    to_task_event['event_id'],
                    to_task_event['table_name'],
                    to_task_event['ord'],
                    to_task_event['parent_id'],
                    datetime.now(),
                    to_task_event['task_id']
                    )
                await conn.close()
                return parent_id
            except Exception as e:
                logger.create('Произошла ошибка при создании записи '
                              'в сущности task_to_event. Метод create_task_to_event', 
                                                                            e)
                await conn.close()
                return False
    
        
    async def update_status(self, event=None, task_id=None):
        '''
        Обновляем исходные записи в своих таблицах.
        
        :param event: - эклемпряр класса события.
        :param task_id: - id события в сущности task.
        '''
        conn = await self._connect_database() #control_db
        # conn = await asyncpg.connect('postgresql://postgres:1@localhost/test')
        
        if task_id == None and event['type'] == 'suitcase_start' and conn:
            # conn = await self._connect_database()
            try:
                print(event['object'].status)
                await conn.execute("\
                UPDATE polycomm_suitcase \
                SET status = $1, csp = $2, unpaid = $3, in_task = True, \
                    to_account = $4 \
                WHERE polycom_id = $5;",
                event['object'].status, 
                event['object'].csp, 
                event['object'].unpaid, 
                event['object'].to_account,
                event['object'].polycom_id
                )
                print('suitcase') #FIXME:
            except Exception as e:
                logger.create('Произошла ошибка при обновлении записи в '
                              'сущности polycomm_suitcase. Метод update_status', 
                                                                            e)
            finally:
                await conn.close()
                
        
        elif task_id == None and event['type'] == 'alarm' and conn:
            # conn = await self._connect_database()
            try:
                print(event['object'].status)
                await conn.execute("\
                UPDATE polycommalarm \
                SET status = $1 \
                WHERE polycommalarm_id = $2;",
                event['object'].status, 
                event['object'].polycommalarm_id
                )
                print('alarm') #FIXME:
            except Exception as e:
                logger.create('Произошла ошибка при обновлении записи в '
                              'сущности polycommalarm. Метод update_status', 
                                                                            e)
            finally:
                await conn.close()
            
        elif task_id == None and event['type'] == 'issue' and conn:
            # conn = await self._connect_database()
            try:
                print(event['object'].status)
                await conn.execute("\
                UPDATE polycommissue \
                SET status = $1 \
                WHERE polycommissue_id = $2;",
                event['object'].status, 
                event['object'].polycommissue_id
                )
                print('issue') #FIXME:
            except Exception as e:
                logger.create('Произошла ошибка при обновлении записи в '
                              'сущности polycommissue. Метод update_status', 
                                                                            e)
            finally:
                await conn.close()
            
        elif task_id == None and event['type'] == 'receipt' and conn:
            # conn = await self._connect_database()
            try:
                print(event['object'].status)
                await conn.execute("\
                UPDATE receipts \
                SET status = $1 \
                WHERE receipt_id = $2;",
                event['object'].status, 
                event['object'].receipt_id
                )
                print('receipt') #FIXME:
            except Exception as e:
                logger.create('Произошла ошибка при обновлении записи в '
                              'сущности receipts. Метод update_status', e)
            finally:
                await conn.close()
            
        elif task_id != None and conn:
            try:
                await conn.execute("\
                UPDATE task \
                SET status = 1 \
                WHERE id = $1;", task_id
                )
                print('task_id') #FIXME:
            except Exception as e:
                logger.create('Произошла ошибка при обновлении записи в '
                              'сущности task. Метод update_status', e)
            finally:
                await conn.close()
        
        
    async def update_status_and_resolved(self, task_id):
        '''
        Обновление status и параметра resolved в сущности task для событий, из
        списка 'line_event'(к которым нет претензий).
        
        :param task_id: - id события в сущности task.
        '''
        conn = await self._connect_database() #control_db
        # conn = await asyncpg.connect('postgresql://postgres:1@localhost/test')
        if conn:
            try:
                await conn.execute("\
                UPDATE task \
                SET status = 1, resolved = True \
                WHERE id = $1;", task_id
                )
                print('task_id') #FIXME:
            except Exception as e:
                logger.create('Произошла ошибка при обновлении записи в '
                              'сущности task. Метод update_status_and_resolved', 
                                                                            e)
            finally:
                await conn.close()