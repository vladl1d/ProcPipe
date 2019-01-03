# -*- coding: utf-8 -*-
"""
Содержит среду запуска и исполнения расчетов (shell)
Created on Tue Nov 13 06:29:07 2018
@author: V-Liderman
"""
#from __future__ import unicode_literals
#from __future__ import print_function as _print
import os
import re
import threading
import importlib
import logging
import time
import json as jsonlib
import jsonpath
#import uuid
#import pandas as pd
#from six.moves import queue
from configparser import ConfigParser
from ..json.stream import JsonStream
#from ..json.parser import JsonParser
from ..adapter.qbuilder import QueryBuilderNestedFrom, QueryBuilderNestedSelect
from ..adapter.adapter import DataAdapter
from .poller import Poller
from .results import ProcResult
#from .types import t_dict
#from .log import ShellLogHandler

__RUN_DIR__ = os.path.dirname(__file__)

class ProcShell():
    ''' Класс, котором осуществляется выполнение экземпляров расчета, который содержит контекст
    и глобальные переменные расчета'''
    def __init__(self, ini_file=None, dba_cfg_dict=None, base_dir=None, debug=False):
        #конфигурация
        self._config = None
        # рабочая директория
        self.base_dir = base_dir if base_dir else __RUN_DIR__

        #признак того, что мы обрабатываем задание
        self._ready_event = threading.Event()
        #демон очереди запросов
#        self._db_worker = None
        #адаптер к данным
        self._data_adapter = None

        #очередь заданий на расчет с подписчиками в виде классов расчета
        self._proc_queue = None
        # глобальные результаты расчета
        self.result = None
        #логгер
        self.log = None
#        self._log_queue = queue.Queue()
        self._log_handler = None
        #режим отладки. Не запускает мультипоточность
        self.debug = debug
        # текущий контекст
        self.context = {
            #id ноды
            'node_id': None,
            #id сессии (расчета)
            'session_id': None,
            #id пакета
            'batch_id': None,
            #счетчик запусков по подписке
            'part_id': 0,
            # глобальный кеш объектов (используется если источник данных выдает плоскую таблицу)
            'cache': dict(),
            # кеш путей в файле с данными построенный по схеме
            'schema_map': dict()
            }

        # кеш к данным
        self.dict_cache = None

        #инициализация среды
        self._read_ini_file(ini_file if ini_file else 'DataAdapter.ini')
        self._init_log_system()
        self._init_db_adapter(dba_cfg_dict)
        self._init_proc_queue()
        self._init_db_dicts()

############### Properties #######################
    @property
    def _ready(self):
        '''Признак готовности к выполнению задания'''
        return self._ready_event.is_set()

    #список переменных задающих контекст. ОТправляются с каждым запросом на сервер
    _context_var_names = {'node_id': '@Node_id', 'session_id': '@Session_id', \
                          'batch_id': '@Batch_id'}
    def context_param_values(self):
        '''Возвращает значения переменных состояния'''

        param_values = dict()
        if self._context_var_names and self.context:
            for prop in self._context_var_names:
                if prop in self.context:
                    param_values[self._context_var_names[prop]] = self.context[prop]

        return param_values

############### Ini-file #######################
    def _cfg_get(self, section, key, default=None, init=None, assert_key=False):
        '''получение настройки из файла'''
        assert self._config, 'Не инициированы настройки'
        assert section and key, 'Неверные параметры'
        #assert cfg_section, 'Конфигурационный файл не содержит секции' + section
        value = self._config.get(section, key, fallback=default)
        assert not assert_key or value is not None, 'В ini-файле в секции %s на найден ключ %s' % \
                                            (section, key)
        if init:
            value = init(value)
        return value

    def _read_ini_file(self, ini_file):
        ''' Инициализация среды исполнения программы'''
        config = ConfigParser()
        path = os.path.join(self.base_dir, ini_file)
        try:
            config.read(path)
        except:
            return
        else:
            self._config = config

        self.context['node_id'] = self._cfg_get('Node', 'NodeID', init=int, assert_key=True)

        if 'GLOBALS' in config.sections():
            for key in config['GLOBALS']:
                globals()[key] = config['GLOBALS'][key]
                setattr(self, key, config['GLOBALS'][key])

############### Logger #######################
    def _init_log_system(self):
        '''Инициализация среды исполнения'''
        log_name = self._cfg_get('Logging', 'LogName', assert_key=True)
        log_path = self._cfg_get('Logging', 'LogPath', default='./__Log__', assert_key=False)
        log_level = self._cfg_get('Logging', 'LogLevel', default=0, assert_key=False)
        log_path = os.path.join(self.base_dir, log_path, log_name+'.log')
        self.log = logging.getLogger(log_name)
#        rec_log = self.log.getChild('data')
        #настройка системы логирования
        _format = '%(asctime)s;%(levelname)s;%(message)s;%(filename)s;%(funcName)s'
#        file_handler = logging.handlers.TimedRotatingFileHandler('OmniX_CR', when='midnight', \
#                                                                 backupCount=365, encoding='utf-8')
        logging.basicConfig(filename=log_path, datefmt='%Y.%m.%d %H:%M:%S', style='%', \
                            format=_format, level=log_level)
        #настройка системы ассинхронного логирования на сервер
        self.log_data_id = self._cfg_get('Logging', 'LogDataID', assert_key=False)
#        if self.log_data_id:
#            queue_handler = logging.handlers.QueueHandler(self._log_queue)
#            self.log.addHandler(queue_handler)
#            log_handler = ShellLogHandler(self._data_adapter.push_record, \
#                                          data_id=self.log_data_id, \
#                                          param_values=self.context_param_values)
#            self._log_handler = logging.handlers.QueueListener(self._log_queue, log_handler)

        self.log.info('===========================================================')
        self.log.info('Старт системы логирования. Уровень логирования: %s', log_level)

    def log_tree_node_error(self, msg, *arg, **kwarg):
        '''Логирует ошибку в данных расчета'''
        assert self.log, 'Система логирования не проинициализирована'
        log = self.log#.getChild('data')
        #выделяем контекст исполнения операции
        #TODO: Переделать по нормальному. Пока просто заглушка
        log.warning(msg, *arg, **kwarg)

############### Data Adapter #######################
    def _init_db_adapter(self, dba_cfg_dict=None):
        '''Инициализация адаптера к БД и потокового чтения json-файлов'''
        def _get_dba_config(name=None):
            '''Загрузка файла конфигурации адаптера'''
            assert name, 'Не задан файл конфигурации адаптера данных'
            proc_module = importlib.import_module(name)
            return getattr(proc_module, 'Node')

        #основной метод
        #драйвер
        driver = self._cfg_get('DataAdapter', 'Driver', default='pyodbc', assert_key=False)
        module = importlib.import_module(driver)
        # источник данных
        dsn = self._cfg_get('DataAdapter', 'DSN', assert_key=True)
        # Диалект SQL
        sql_lang = self._cfg_get('DataAdapter', 'SqlLang', assert_key=False)
        # краткие имена столбцов
        shrink_names = self._cfg_get('DataAdapter', 'ShrinkNames', default=False, \
                                     init=(lambda x: int(x) != 0 if x.isnumeric() else bool(x)), \
                                     assert_key=False)
        #Построитель запросов
        qbuilder = self._cfg_get('DataAdapter', 'QueryBuilder', assert_key=False)
        if qbuilder == 'NestedSelect':
            qbuilder = QueryBuilderNestedSelect(shrink_names=shrink_names, sql_lang=sql_lang)
        else:
            qbuilder = QueryBuilderNestedFrom(shrink_names=shrink_names, sql_lang=sql_lang)
        #получение файла конфигурации адаптера
        config = self._cfg_get('DataAdapter', 'Config', assert_key=False)
        if not config:
            config = self._cfg_get('DataAdapter', 'ConfigDir', assert_key=False)
        else:
            config = dba_cfg_dict if dba_cfg_dict else _get_dba_config(config)
        #инициализация адаптера
        self._data_adapter = DataAdapter(dsn, config, module, query_builder=qbuilder, \
                                         base_dir=self.base_dir, log=self.log, debug=self.debug)
        #кодировка
        self.encoding = self._cfg_get('DataAdapter', 'Encoding', default='utf-8', assert_key=True)
        self.log.info('Адаптер успешно проиницализирован')

    def _json_reader(self, typed_cursor):
        '''Обертка. Сконфигурированный ридер для чтения данных из БД и преобразования его в json'''
        return self._data_adapter.fetch_to_json(typed_cursor, encoding=self.encoding)

    def _init_db_dicts(self):
        '''Кеширует записи широко используемых справочников'''
        data_id = self._cfg_get('Cache', 'CacheDataID', assert_key=False)
        if data_id:
            self.dict_cache = self._data_adapter.get_record(data_id, reader=self._json_reader)
############### Workers #######################
    def _init_proc_queue(self):
        '''Инициализация очереди исполнения расчета'''
        queue_size = self._cfg_get('Node', 'QueueSize', default=0, init=int, assert_key=False)
        workers_count = self._cfg_get('Node', 'ThreadCount', default=1, init=int, assert_key=False)
        keep_objects = self._cfg_get('Node', 'Persist', \
                                     init=(lambda x: int(x) != 0 if x.isnumeric() else bool(x)),\
                                     assert_key=False)
        #Единый массив результатов. Использовать в случае единой отправки данных на сервер
        global_results = self._cfg_get('Node', 'GlobalResults', \
                                     init=(lambda x: int(x) != 0 if x.isnumeric() else bool(x)),\
                                     default=False, assert_key=False)
        proc_module = self._cfg_get('Proc', 'ProcModule', assert_key=False)
        proc_class = self._cfg_get('Proc', 'ProcClass', assert_key=True)

        if keep_objects is None:
            keep_objects = workers_count > 0
        try:
            if proc_module:
                proc_module = importlib.import_module(proc_module)
                proc_class = getattr(proc_module, proc_class)
            else:
                proc_class = globals()[proc_class]
        except:
            self.log.critical('Не найден класс для исполнения запроса')
            return

        proc_params = {'shell': self}
        self._proc_queue = Poller(proc_class, self.log, workers_count, keep_objects, queue_size, \
                                  **proc_params)
        # Настройка парсера
        # id адаптера данных по которому получать данные для расчета
        self.proc_data_id = self._cfg_get('Proc', 'ProcDataID', assert_key=True)
        # id адаптера данных для коммита результатов
        self.commit_data_id = self._cfg_get('Proc', 'CommitDataID', assert_key=True)

#        ### json-парсер
#        # Подписка на путь в файле относительно которого запускать расчет
#        parser_cb_path = self._cfg_get('JSONParser', 'ProcCallbackPath', assert_key=True)
#        # Путь, который по подписке удаляет ненужные узлы из памяти. Так она не растет
#        parser_top_path = self._cfg_get('JSONParser', 'WithdrawalPath', assert_key=False)

#        #настройка подписки по пути в json
#        callbacks = dict()
#        if parser_cb_path:
#            callbacks[parser_cb_path] = self.__proc_cb
#        #Подписка для удаления ненужных путей
#        if parser_top_path:
#            callbacks[parser_cb_path] = (lambda *x, **y: True)
#        #настройка потокового парсера данных
#        self.parser = JsonParser(encoding=self.encoding, callbacks=callbacks)

        #результаты расчета
        # Размер пакета для отправки данных на сервер
        result_batch_size = self._cfg_get('Results', 'ResultBatchSize', assert_key=False, init=int, default=0)
        # id адаптера данных по которому получать данные
        result_data_id = self._cfg_get('Results', 'ResultsDataID', assert_key=False)
        self.result = ProcResult(proc_class.details_schema, self._data_adapter.push_record, \
                                 self.log, workers_count, batch_size=result_batch_size, \
                                 #параметры которые передадутся callback(push_record)
                                 data_id=result_data_id, \
                                 param_values=self.context_param_values) \
                        if global_results else None

        # Логирование
        self.log.info('Настроен обработчик расчета %s.%s', proc_module.__name__, proc_class.__name__)
        self.log.info('Максимальная длина очереди = %s', queue_size)

    def _run_poll(self, max_count=None):
        '''Метод исполнения потока опроса БД'''
        poll_data_id = self._cfg_get('Poll', 'PollDataID', assert_key=True)
        poll_timeout = self._cfg_get('Poll', 'PollTimeout', init=float, default=0, assert_key=False)
        poll_root = self._cfg_get('Poll', 'PollRoot', assert_key=True)
        self.log.info('Начало опроса БД')
        cnt = 0
        _stopping = False
        while not _stopping:
            job_context = self._data_adapter.get_record(poll_data_id, self.context_param_values, \
                                                        reader=self._json_reader)
            if job_context:
                job_context = jsonpath.jsonpath(job_context, poll_root)
            if job_context:
                # убираем список из корня
                if isinstance(job_context, list):
                    job_context = job_context[0]
                self._ready_event.clear()
                if not self.new_job(job_context):
                    self.job_finished(False)

                #ждем окончания обработки задания
                self._ready_event.wait()
            #замираем на время
            time.sleep(poll_timeout)
            cnt += 1

            if max_count and cnt >= int(max_count):
                break

    def new_job(self, job_context):
        '''Получено новое задание'''
        def _get_session_vars(job_context):
            '''Генерирует session id Для расчета'''
            assert isinstance(job_context, dict)
            assert job_context.get('session_id', None), 'Не задан session_id'
            return job_context['session_id'], job_context.get('batch_id', None)

        ### Тело
        assert self._proc_queue, 'Поток исполнения не проинициализирован'
        # Контекст задания
        session_id, batch_id = _get_session_vars(job_context)
        self.context.update({'session_id': session_id, 'batch_id': batch_id, 'part_id': 0})

        if self.proc_data_id:
            try:
                #Параметры запроса к БД
                param_values = {'@id': session_id, '@batch':batch_id}
                self.log.debug('Новый job для исполнения с параметрами: %s', str(job_context))
                self.job_starting(param_values)
                context = job_context.copy()
                #подготовка потока данных запроса
                res = self._data_adapter.get_record(self.proc_data_id,
                                                    param_values=param_values,
                                                    reader=self._parse_job_record,
                                                    #kwargs
                                                    job_context=context)
                #ожидание завершения всех потоков
                self._wait_finished()
                #завершение задания
                self.log.debug('Завершили job для исполнения с параметрами: %s', str(job_context))
                self.job_finished(res)

                #обработка результатов
                if res:
                    #смотрим - смогли ли отправить результаты
                    which = []
                    if not self.result.success.is_set():
                        which.append('отправки')
                    if not (self.debug or self._proc_queue.success.is_set()):
                        which.append('обработки')
                    if which:
                        self.log.debug('В процессе обработки job возникли ошибки %s: %s', \
                                       ', '.join(which), str(job_context))

                return True
            except:
                self.log.exception('Ошибка выполнения запроса к БД %s', self.proc_data_id)
                return False
        else:
            self.log.critical('Не найден источник данных с id = %s', self.proc_data_id)
            return False

    def job_starting(self, param_values=None):
        '''Событие начала обработки задания'''
        self.result.success.set()
        self._proc_queue.success.set()

    def _parse_job_record(self, typed_cursor, job_context):
        '''Обработка данных для расчета'''
        ##### Тело
        cursor, schema = next(typed_cursor, (None, None))

        #цикл исполнения всех датасетов
        while cursor is not None:
            #обертка потока для буферезированного чтения
            fin = JsonStream(cursor, encoding=self.encoding, \
                             get_value_cb=lambda x: str(x[0]) if x else None)

            #потоковая обработка json
            if self._data_adapter.query_builder.shrink_names:
                alias_map = self._data_adapter.query_builder.alias_map
            else:
                alias_map = None
            #кешируем пути в данных для быстрой обработке
            if schema:
                self.context['schema'] = schema
                self.context['schema_map'] = schema.traverse_query_paths()

            # готовим контекст для расчета
            job_context.update({
                    'path': '$',
                    'context_cache': self.context['cache'],
                    'schema_map': self.context['schema_map'],
                    'schema': self.context['schema']
                    })

            #map-функция для перевода в мелкий регистр и с учетом словоря синонимов запроса
            def _pairs_hook(node):
                if alias_map:
                    return {alias_map.get(nam.lower(), nam.lower()): val \
                            for nam, val in node}
                return {nam.lower(): val for nam, val in node}
            #парсинг
            json = jsonlib.load(fin, object_pairs_hook=_pairs_hook)
            if json:
                # выбираем правильный корень для расчета
                start_path = self._cfg_get('Proc', 'ProcRoot', assert_key=True)
                json = jsonpath.jsonpath(json, start_path)
                if start_path:
                    job_context['path'] = re.sub(r'(\[\d+\])', '',  start_path)
                if not json:
                    break
                self._proc_job(json, job_context=job_context)
                cursor, schema = next(typed_cursor, (None, None))
            else:
                break
        return not not json

    def _proc_job(self, node, job_context):
        '''Callback потокового обрабочика json Для запуска расчета'''
        # метод выполнения расчета класса расчета
        proc_data_entry = self._cfg_get('Proc', 'ProcDataEntry', assert_key=False, default='run')

#        assert isinstance(parser, JsonParser), 'Неверные атрибуты запуска расчета'
        self.context['part_id'] += 1
        _id = ''
#        _id = node.get('link', self.context['part_id']) if hasattr(node, 'get') \
#                                                        else self.context['part_id']
        self.log.info('Новая подписка узла(%d:%s): %s', self.context['part_id'], _id, \
                      str(self.context_param_values()))
        #запуск обработчика
        if self.debug:
            proc = self._proc_queue.object_constructor()
            run = getattr(proc, proc_data_entry)
            res = run(tree_node=node, context=job_context)
            self._proc_finished(res)
            return res

        else:
            self._proc_queue.create_job(proc_data_entry, callback=self._proc_finished, \
                                        #параметры run
                                        tree_node=node, context=job_context, hold_node=node)
            return False

    def _proc_finished(self, result, **kwargs):
        '''Событие окончания обработки порции файла по подписке.
        Использовать его для отправки данных на сервер если данные отправляются кусками'''
        self.log.info('Закончена обработка узла(%d) по подписке: %s', \
                      self.context['part_id'], str(self.context_param_values()))
#        self.result.flush()

    def _wait_finished(self):
        '''Ожидание завершения расчета'''
        if not self.debug:
            self._proc_queue.wait_finished()
        self.result.wait_finished()

    def job_finished(self, success):
        '''Обработка результатов расчета. Глобальный конец расчета'''
        # отправка результатов на сервер
        self.result.flush()
        #закрытие обработки в БД
        self._data_adapter.commit_db_job(self.proc_data_id)
        # подтверждение окончания задания
        param_values = self.context_param_values()
        param_values['@B_Success'] = success
        self._data_adapter.push_record(None, self.commit_data_id, param_values, \
                                       data_field='@C_Result')
        #сброс параметров текущего задания
        self.context.update({'session_id': None, 'batch_id': None, 'part_id': 0,
                             'cache': dict(), 'schema_map': dict()})

        # готовы для следующей обработки
        self._ready_event.set()
#        with open(r'C:\Users\v-liderman\Desktop\result2.json', 'w', encoding='utf-8') as fout:
#            json.dump(self.result, fout, cls=CustomEncoder)
#            if not self.result.empty:
#                self.result.to_json(fout, orient='records')

############### Публичные методы #######################
    def run(self, max_count=None):
        '''Запуск оболочки'''
        if hasattr(self, '_log_handler') and  self._log_handler:
            self._log_handler.start()
        # запуск демона чтения заданий из БД
#        thread = threading.Thread(target=self._run_poll)
#        thread.daemon = True
#        self._db_worker = thread
        self._ready_event.set()
#        thread.start()
        self.log.info('Запуск ноды')
        #отказался от потока, потому что это безсмысленно
        self._run_poll(max_count)

    def stop(self):
        '''Остановка оболочки'''
        if hasattr(self, 'log') and self.log:
            self.log.info('Остановка ноды')
        #остановка демона чтения БД
#        self._stopping = True
#        if hasattr(self, '_db_worker') and self._db_worker:
#            self._db_worker.join()
#            del self._db_worker

        if hasattr(self, '_proc_queue') and self._proc_queue:
            self._proc_queue.stop_poll()


        if hasattr(self, '_log_handler') and  self._log_handler:
            self._log_handler.stop()

        if hasattr(self, 'result') and self.result:
            self.result.stop_poll()

    def __del__(self):
        self.stop()
