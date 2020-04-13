# Analytic database
[FlyData wiki](https://fly-d.ru/wiki/doku.php?id=analytic_database_on_python_3)

Программа в асинхронном режиме извлекает информацию из базы данных и анализирует её, составляет хронологический событийный ряд, правит его согласно заданным условиям, выявляет аномалии, присваивает им типы задач, записывает финальные данные в базу данных.

------
`config.py`:<br>
Конфигурационный файл, который содержит в себе константы, необходимые для работы программы:<br>
`PG_USER`(str) - логин польователя базы данных.<br>
`PG_PASSWORD`(str) - пароль пользователя базы данных.<br>
`PG_HOST`(str) - адрес сервера базы данных.<br>
`PG_DB`(str) - имя базы данных.<br>
`LAST_EVENT_TIME`(int) - временной интервал, свыше которого будут обрабатываться события(рекомендуемое значение - 900/или 15 минут: будут обрабатываться события, которые были созданы раньше, чем за последние 15 минут).<br>
`TIMEDELTA_CHECK`(int) - допустимое время между упаковками в секундах(рекомендуемое значение - 420).<br>
`DIR_NAME_LOG`(str) - имя папки для логов программы.<br>
`FILE_NAME_LOG`(str) - название файлов логов программы.<br>
`FILESIZE_LOG`(int) - максимальный размер одного экземпляра логов.<br>
`COUNT_BACKUP_LOG`(int) - кол-во последних копий логов.<br>

------
`request_database.py`:<br>
Модуль состоит из 6 классов: 5 - это структуры данных, 1 - запросы к базе данных. Структуры данных - это данные, которые используются внутри программы: устройства, оповещения, уведомления, упаковки, чеки. У каждого экземпляра есть свои свойства(атрибуты), которые заполняются при создании экземпляров после запросов из базы, а так же в процессе выполнения программы.<br>
Класс `Request` осуществляет запросы к базе данных. Извлекает информацию из сущностей, а так же обновляет ее. Методы:<br>
**_\_init__** - конструктор класса, в котором определяются логин, пароль, адрес и имя базы данных. <br>
**_connect_database** - создает подключение к базе данных(которое после нужно закрыть методом .close())<br>
**get_devices** - получает список активных устройств из базы данных, создает из них экземпляры класса Device, добавляет их в список и возвращает копию этого списка.<br>
**get_alarm** - полуает список необработанных уведомлений из базы данных(с условием status = 0), создает из них экземпляры класса Alarm(заполняя все свойства, извлеченные из базы данных), добавляет их в список и возвращает копию этого списка.<br>
**get_issue** - получает список необработанных оповещений из базы данных(с условием status = 0), создает из них экземпляры класса Issue(заполняя все свойства, извлеченные из базы данных), добавляет их в список и возвращает копию этого списка.<br>
**get_receipts** - получает список необработанных чеков из базы данных(с условием status = 0), создает из них экземпляры класса Receipts(заполняя все свойства, извлеченные из базы данных), добавляет их в список и возвращает копию этого списка.<br>
**get_suitcases** - получает список необработанных упаковок из базы данных(с условием status = 0), создает из них экземпляры класса Suitcase(заполняя все свойства, извлеченные из базы данных), добавляет их в список и возвращает копию этого списка.<br>
**create_polycommissue_event** - создает запись в сущности polycommissue. На вход принимает экземпляр класса Suitcase. Данные берутся из атрибутов экземпряла класса. Заполняются поля id, localdate, device, total, suitcase, duration, type, date, createtime, которые были присвоены в процессе логики программы.<br>
**create_task** - создание записи в сущности task. На вход принимает словарь с данными, которые необходимо заполнить в сущности: date, local_date, device_id, type.<br>
**create_task_to_event** - создание записи в сущности task_to_event. На вход принимает словарь с данными, которые необходимы для заполнения в сущности: event_id, table_name, ord, parent_id, created_date, task_id, которые были присвоены в процессе логики программы.<br>
**update_status** - обновляет исходные записи в исходных сущностях события. На вход принимает один из двух параметров: event - это экземпляр класса события, из которых берутся данные для заполнения или task_id - это id группы событий в сущности task, которой нужно присвоить status = 1.<br>
**update_status_and_resolved** - обновляет атрибуты status и resolved в сущности task для групп событий, к которым "нет претензий". На вход принимает id события в сущности task, которые нужно обновить.<br>

------
`app.py`:<br>
Модуль-точка запуска программы. В самом начале создается экземпляр класса Request с константами из конфигурационного config<span></span>.py. Далее выполняется асинхронная функция run_app, которая принимает экземпляр класса Request.<br>
В функции `run_app` в первую очередь извлекается список всех готовых устройств из метода get_devices() экземпляра класса Request.<br>
Далее в итерациях по списку готовых устройств devices извлекаются все типы событий для каждого устройства(по его id), и заполняются в списки атрибутов класса Device(alarm_list, issue_list, receipts_list, suitcases_list).<br>
Далее в итерациях по каждому из списков событий каждого устройства создаются словари, ключи которых содержат time - время события, object - экземпляр класса самого события(Alarm, Issue, Receipt, Suitcase), type - тип события(строковая запись события: 'alarm', 'issue', 'receipt', 'suitcase_start', 'suitcase_finish'). Причем, что для одной упаковки Suitcase создаются два словаря с типами 'suitcase_start' и 'suitcase_finish' и их временем time начала и конца соответственно(но одним экземпляром класса Suitcase). <br>
Все эти словари добавляются в список live_event атрибута класса Device. Далее все эти словари сортируются по ключу time. <br>
На этом этапе мы имеем список всех устройств devices, с экземплярами классов Device, у которого в атрибуте live_event список словарей всех событий(начало упаковки, конец упаковки, уведомления, оповещения, чеки), отсортированные по времени, с которыми мы будем дальше работать.

Далее вызывается функция `sort_receipts`, которая принимает на вход экземпляр класса Device. Внутри функции происходит правка событийного ряда line_event каждого устройства по признаку "чек после упаковки". Если вдруг оказалось, что чек(по времени) стоит перед началом упаковки, то он перемещался после этой упаковки.

Далее вызывается функция `grouping_events`, которая на вход принимает экземпляр класса Device. Внутри функции происходит правка событийного ряда line_event каждого устройства по признаку "полный цикл упаковки". Это означает, что если после события типа "чек" было событие типа {"начало упаковки", "оповещение", "уведомление"}, то после чека вставлялся словарь с ключом "type" со значением "none". Так же осуществляется проверка на время между событиями внутри одного цикла упаковки. Если между событиями прошло больше, чем TIMEDELTA_CHECK(константа из config<span></span>.py, по умолчанию 420 секунд), то добавляется словарь с ключами 'type': 'none', 'ДРОБЛЕНИЕ ПО ПРИЗНАКУ': 'ВРЕМЯ'

Далее вызывается функция `listing_events`, которая на вход принимает экземпляр класса Device. Внутри функции создается список списков словарей событий. Каждый элемент главного списка - это список, который содержит список словарей полного цикла упаковки.<br>
Пример:<br>

| Элемент главного списка(группа) | Элемент второстепенного списка(словари-события внутри группы)
-------------|------------- 
 Группа  событий        |               
                        |{Начало упаковки}
                        |{Конец упаковки}
                        |{Начало упаковки}
                        |{Конец упаковки}
                        |{Чек на 2 упаковки}
 Группа событий         |               
                        |{Начало упаковки}
                        |{Конец упаковки}
                        |{Чек на 1 упаковку}

Далее вызывается функция `check_last_event_time`, которая на вход принимает экземпляр класса Device. Внутри функции осуществляется проверка на предмет того, что события, с которыми будет продолжена дальше работа произошли раньше, чем последние LAST_EVENT_TIME(константа из config.<span></span>py. по умолчанию 900 сек или 15 минут). Если событие произошло меньше, чем последние LAST_EVENT_TIME(константа из config<span></span>.py), то оно исключается из списка line_event.

Далее вызывается функция `check_broked_events`, которая на вход принимает экземпляр класса Device. Внутри функции происходит проверка упаковок на наличие чеков к ним. Если количество одинарных упаковок(Suitcase.package_type == 1) в группе равно количеству одинарных чеков(Receipt.quantitypackageone) в группе и количество двойных упаковок(Suitcase.package_type == 2) в группе равно количеству одинарных чеков(Receipt.quantitypackagedouble) в группе, то эта группа остается в списке line_event атрибута класса Device. В противном случае, если что-то из них не равно, то эта группа попадает в список broken_line_event атрибута класса Device, с которой будет проводится основная работа в последующем.

Далее вызывается функция `add_task`, которая на вход принимает экземпляр класса Device. Внутри функции каждому блоку событий списков line_event и broken_line_event добавляется словарь с ключами 'type': 'service', 'task_type' и значением одной из задач: оповещение, чек без упаковок, чеки без упаковок и уведомление, КПУ/КнПУ, смешанные. Логика присвоения следующая:<br>
***Уведомление***
1. группа событий состоит только из событий типа «уведомление»

***Чек без упаковки и уведомление***
1. группа событий состоит из событий типа «чек» и «уведомление»

***Чек без упаковки***
1. группа событий состоит только из событий типа «чек» ИЛИ 
2. группа событий состоит только из событий типа "упаковка" и "чек" при этом:
    * совокупное количество одинарных упаковок в чеках группы больше или равно числу событий типа "упаковка" с типом упаковки 1 (одинарная/стандартная)
    * И
    * совокупное количество двойных упаковок в чеках группы больше или равно числу событий типа "упаковка" с типом упаковки 2 (двойная)

***КПУ/КнПУ***
1. группа событий состоит только из событий типа «упаковка» ИЛИ
2. группа событий состоит только из событий типа "упаковка" и "чек" при этом:
    * количество одинарных упаковок в чеках группы меньше или равно числу событий типа "упаковка" с типом упаковки 1 (одинарная/стандартная)
    * И
    * количество двойных упаковок в чеках группы меньше или равно числу событий типа "упаковка" с типом упаковки 2 (двойная)

***Смешанный***
1. это все остальные группы событий, которым не присвоил ни один из вышеуказанных типов

Далее вызывается функция `receipt_broken_line_event_sync`, которая на вход принимает экземпляр класса Device. Внутри функции каждой упаковке в группах списка broken_line_event в атрибут Suitcase.receipt_id записывается номер чека Receipts.receipt_id, в Receipts.count_packageone минусуется одна единица(так как чек присвоен, то он вычитается. чтобы не присвоить лишних). Если упаковке не нашелся чек, то атрибуту Suitcase.receipt_id присваивается -1. Если после первой итерации остались свободные чеки, то по остаточному принципу привязываем их к упаковкам. Так же во время этого в атрибут Suitcase.package_type_by_receipt записывается тип оплачиваемой упаковки(1 или 2), которое берется из типа чека, который привязывается к ней. Важный момент, что перебор упаковок(для broken_line_event) идет задом-наперед. Те же действия происходят в функции `receipt_line_event_sync`, только для списка line_event атрибута класса Device.

Далее вызывается функция `adding_attributes`, которая на вход принимает экземпляр класса Device. Внутри функции в атрибут класса Suitcase.issue_list(словарь) добавляются атрибуты для будущих уведомлений(для каждого события упаковка всех блоков событий broken_line_event). 
1. если у упаковки package нет вложенных оповещений и уведомлений:
    1. если для упаковки package не нашлось чека (receipt_id=-1), тогда package - коммерчески пригодная (КПУ) и неоплаченная:
        1. package.csp = true
        2. package.unpaid = true
        3. package.to_account = true
        4. дополнительно:
            * создаем у package вложенный массив оповещений
            * создаем оповещение 7-ого типа ISSUE и заполняем данными(id, total, suitcase, date, localdate)
            * укладываем ISSUE во вложенный массив оповещений
    2. если для упаковки package нашелся чек (receipt_id != -1) , тогда package - коммерчески пригодная (КПУ) и оплаченная:
        1. package.csp = true
        2. package.unpaid = false
        3. package.to_account = true
        4. если тип package не совпадает по типу оплаченной упаковки чека, тогда дополнительно:
            1. случай, когда package_type=2 и тип оплаченной упаковки чека = одинарная:
                1. создаем у package вложенный массив оповещений
                2. создаем оповещение 8-ого типа ISSUE и заполняем данными(id, total, suitcase, date, localdate)
                3. укладываем ISSUE во вложенный массив оповещений
            2. случай, когда package_type=1 и тип оплаченной упаковки чека = двойная
                1. создаем у package вложенный массив оповещений
                2. создаем оповещение 9-ого типа ISSUE и заполняем данными(id, total, suitcase, date, localdate)
                3. укладываем ISSUE во вложенный массив оповещений
2. если у упаковки package есть вложенные оповещения и уведомления:
    1. если для упаковки package не нашлось чека (receipt_id=-1) , тогда package - коммерчески НЕ пригодная (КнПУ) и неоплаченная:
        1. package.csp = false
        2. package.unpaid = true
        3. package.to_account = false
    2. если для упаковки package нашелся чек (receipt_id != -1) , тогда package - коммерчески пригодная (КПУ) и оплаченная:
        1. package.csp = true
        2. package.unpaid = false
        3. package.to_account = true
        4. если тип package не совпадает по типу оплаченной упаковки чека, тогда дополнительно:
            1. случай, когда package_type=2 и тип оплаченной упаковки чека = одинарная:
                1. создаем у package вложенный массив оповещений
                2. создаем оповещение 8-ого типа ISSUE и заполняем данными(id, total, suitcase, date, localdate)
                3. укладываем ISSUE во вложенный массив оповещений
            2. случай, когда package_type=1 и тип оплаченной упаковки чека = двойная:
                1. создаем у package вложенный массив оповещений
                2. создаем оповещение 9-ого типа ISSUE и заполняем данными (какими? - сам разберешься)
                3. укладываем ISSUE во вложенный массив оповещений

Далее вызывается асинхронная функция `update_database`, которая на вход принимает экземпляр класса Device. Внутри функции генерируется информация для осуществления обновлений в базе данных. Если у атрибута Suitcase.issue_list['type'] 7, 8 или 9, то вызывается функция request.create_polycommissue_event, для создания записи в сущности polycommissue(с ранее подготовленными данными в функции adding_attributes). Далее объектам событиям alarm и issue присвается в атрибут status = 1. Далее, если у события чек нет "свободных позиций"(все чеки присвоены упаковкам), то атрибуту status объекта чека присваивается значение 1, иначе, если у чека есть свободные позиации, то атрибуту чека status присваивается значение 2.

Затем подготавливается словарь с данными to_task для последующей записи в сущность task. Ключи - значения словаря:
 * date - московское время окончания последнего (по хронологии) события группы(атрибут Receipts.dateclosemoscow для события типа чек и datetime.now() на случай, если вдруг тип события != чек).
 * local_date - локальное время окончания последнего (по хронологии) события группы.(datetime.now())
 * device_id - id устройства, для которог производится аналитика.
 * type - номер типа группы(оповещение, чек без упаковок, чеки без упаковок и уведомление, КПУ/КнПУ, смешанные). Берется исходя из записи нулевого элементы группы из device.task_type. Для КПУ/КнПУ - 4 и тд.
 * createtime - время, генерируемое базой автоматически.
После того, как данные готовы происходит запись методом request.create_task, с возвращением id созданной записи. id записывается в значение ключа 'task_id' словаря to_task_event, для последующей записи в task_to_event

Далее для каждого события каждой группы создаются записи в сущности task_to_event, со следующими полями:
 * event_id - id события(polycom_id для упаковки, polycommissue_id для оповещения, polycommalarm_id для уведомления, receipt_id для чека)
 * table_name - 'polycomm_suitcase' для упаковки, 'polycommissue' для оповещения, 'polycommalarm' для уведомления, 'receipts' для чеков
 * ord - порядковый номер события в группе.
 * parent_id - id упаковки в сущности task_to_event, к которой относится уведомление или оповещение.
 * task_id - id группы событий в сущности task.
 * created_date - время, генерируемое базой автоматически.

Затем происходит обновление поля status у каждого из событий в исходных сущностях в соотстветствии со значением, которое было присвоено в процессе выполнения программы(для события упаковки в сущности polycomm_suitcase и тд) методом request.update_status с параметром=объект события.
После происходит обновление поля status = 1 в сущности task методом request.update_status с параметром task_id=to_task_event['task_id']
Затем все те же действия производятся для списка блоков событий line_event атрибута Device, за единственным исключением: 
в сущности task выполняется обновление поля status = 1 и поля resolved = True методом request.update_status_and_resolved с параметром task_id=to_task_event['task_id'].
