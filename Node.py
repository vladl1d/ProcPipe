# -*- coding: utf-8 -*-
"""
Содержит классы с результатом расчета и среду запуска и исполнения расчетов (shell)
Created on Tue Nov 13 06:29:07 2018
@author: V-Liderman
"""

#from __future__ import unicode_literals
#from __future__ import print_function as _print
import sys
import os
import threading
import importlib
import logging
#import uuid
#import pandas as pd
__RUN_DIR__ = os.path.dirname(__file__)
sys.path.append(__RUN_DIR__)
#from uuid import UUID
#from datetime import datetime
from six.moves import queue
from configparser import ConfigParser
from Query import Node
from DataAdapter import DataAdapter, QueryBuilderNestedFrom, QueryBuilderNestedSelect
from JsonStream import JsonStream, JsonParser, JsonTree
from Poller import Poll
from Util import CustomEncoder
from Log import ShellLogHandler
from pprint import pprint
import json
#from Proc import ProcCR

class ProcResult(Poll):
    '''
    Класс инкапсулирующий результат расчета.
    Нужен для ассинхронного добавление результата через инфраструктуру Poller
    Класс копит batch И отправляет его на СУБД
    '''
#    @staticmethod
#    def _create_pd_dataframe(schema):
#        _details = pd.DataFrame(columns=schema.keys())
#        # коверсия в pandas Типы
#        to_dtype = lambda x: \
#            'int64' if x == int else \
#            'float64' if x == float else \
#            'datetime64' if x==datetime  else \
#            'object'
#
#        types = {key:to_dtype(schema[key][0]) for key in schema.keys()}
#        _details.astype(types)
#        return _details
#    def _append2pd_dataframe(self, new):
#        to_jstype = lambda value, _type: \
#            value.isoformat() if _type == datetime else \
#            str(value) if _type == UUID else \
#            value
#
#        new = {key:to_jstype(new[key], self.__schema[key][0]) \
#                                for key in new}
#
#        self.__result = self.__result.append(new, ignore_index=True)

    def __init__(self, columns, flush_func, log, workers_count=None, batch_size=100, **kwarg):
        '''Создание класса по колонкам'''
        #self._result = self._create_pd_dataframe(columns)
        self._schema = columns
        self._result = []
        self._lock = threading.Semaphore(1)
        #размер пакета для потоковой записи в БД
        self._batch_size = int(batch_size)
        #адаптер доступа к данным
        self._flush_func = flush_func
        self._kwarg = kwarg
        # классы не отличимы от функций. Поэтому эмулируем внешний класс с помощью lambda
        super().__init__(lambda: self, log, workers_count=workers_count, verbose=False, \
                          keep_objects=False)

    @property
    def result(self):
        '''Получение результата из вне'''
        return self._result
    @property
    def result_count(self):
        '''счетчик кол-во записей'''
        return len(self._result)
    def __call__(self, **args):
        return self._result
    def __str__(self):
        return str(self())
    def _async_append(self, new):
        #Создаем критическую секцию to be threadsafe by all means
        with self._lock:
#            self._lock.acquire(True)
            self._result.append(new)
            #self._append2pd_dataframe(new)
            self._check_batch()
#            self._lock.release()
    def append(self, new):
        '''Добавление новой строки к результату'''
        self.create_job('_async_append', callback=None, new=new)
    def _check_batch(self):
        '''Проверка наполнения batch'''
        if self._batch_size>0 and self.result_count > self._batch_size:
            self._flush()
    def _flush(self, outside=False):
        '''Сбрасывает результаты расчета на СУБД'''
        if not self._flush_func:
            return
        assert callable(self._flush_func)
        #вставляем результаты в БД в критической секции
        if self.result_count:
            if outside:
                self._lock.acquire(True)
            if not self._flush_func(self._result, **self._kwarg):
                self.success.clear()
            self._result.clear()
            if outside:
                self._lock.release()
                
    def flush(self):
        '''Сбрасывает результаты расчета на СУБД'''
        self._flush(False)

###############################################################################
class ProcShell():
    ''' Класс, котором осуществляется выполнение экземпляров расчета, который содержит контекст
    и глобальные переменные расчета'''
    def __init__(self, ini_file=None, dba_cfg_dict=None, debug=False):
        #конфигурация
        self._config = None
        #идентификатор ноды
        self.node_id = None
        #признак остановки для workerа
        self._stop_event = threading.Event()
        #признак того, что мы обрабатываем задание
        self._ready_event = threading.Event()
        #демон очереди запросов
        self._db_worker = None
        #адаптер к данным
        self._data_adapter = None
        #подписки данных
        self._data_callbacks = None
        # кеш к данным
        self.dict_cache = None
        #точка входа расчета
        self._proc_queue = None
        self._last_job = None
        # глобальные результаты расчета
        self.result = None
        #логгер
        self.log = None
        self._log_queue = queue.Queue()
        self._log_handler = None
        #режим отладки. Не запускает мультипоточность
        self.debug = debug
        # текущий контекст
        self.session_id = None
        self.batch_id = None
        self.part_id = 0
        #список переменных задающих контекст. ОТправляются с каждым запросом на сервер
        self._context_var_list = lambda: {'node_id' : '@Node_id', 'session_id' : '@Session_id', \
                                          'batch_id' : '@Batch_id'}

        #инициализация среды
        self._read_ini_file(ini_file if ini_file else 'DataAdapter.ini')
        self._init_log_system()
        self._init_db_adapter(dba_cfg_dict)
        self._init_proc_queue()
#        self._init_db_dicts()

############### Properties #######################
    @property
    def _stopping(self):
        '''Признак окончания деятельности shell'''
        return self._stop_event.is_set()

    @_stopping.setter
    def _stopping(self, value):
        if value:
            self._stop_event.set()
        else:
            self._stop_event.clear()

    @property
    def _ready(self):
        '''Признак готовности к выполнению задания'''
        return self._ready_event.is_set()

    def context_param_values(self):
        '''Возвращает значения переменных состояния'''
        if isinstance(self._context_var_list, dict):
            return self._context_var_list
        if callable(self._context_var_list):
            fields = self._context_var_list()
            param_values = JsonTree.t_dict()
            for prop in fields.keys():
                if hasattr(self, prop):
                    param_values[fields[prop]] = getattr(self, prop)
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
        path = os.path.join(__RUN_DIR__, ini_file)
        try:
            config.read(path)
        except:
            return
        else:
            self._config = config

        self.node_id = self._cfg_get('Node', 'NodeID', init=int, assert_key=True)

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
        log_path = os.path.join(os.path.dirname(__file__), log_path, log_name+'.log')
        self.log = logging.getLogger(log_name)
#        rec_log = self.log.getChild('data')
        #настройка системы логирования
        FORMAT = '%(asctime)s;%(levelname)s;%(message)s;%(filename)s;%(funcName)s'
#        file_handler = logging.handlers.TimedRotatingFileHandler('OmniX_CR', when='midnight', \
#                                                                 backupCount=365, encoding='utf-8')
        logging.basicConfig(filename=log_path, datefmt='%Y.%m.%d %H:%M:%S', style='%', \
                            format=FORMAT, level=log_level)
        #настройка системы ассинхронного логирования на сервер
        self.log_data_id = self._cfg_get('DataAdapter', 'LogDataID', assert_key=False)
#        if self.log_data_id:
#            queue_handler = logging.handlers.QueueHandler(self._log_queue)
#            self.log.addHandler(queue_handler)
#            log_handler = ShellLogHandler(self._push_record, data_id=self.log_data_id, \
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
        config = dba_cfg_dict if dba_cfg_dict else _get_dba_config(config)
        #инициализация адаптера
        self._data_adapter = DataAdapter(dsn, config, module, query_builder=qbuilder)
        self.log.info('Адаптер успешно проиницализирован')

    def _push_record(self, record, data_id, param_values=None, data_field='@C_Data'):
        '''Отправляет результат на сервер.
        3 способа передать параметры: не передавать, словарь со значениями, функция с словарем
        с названиями локальных полей -> названия параметров'''
        if data_id:
            assert self._data_adapter, 'Адаптер к БД не проиницирован'
            try:
                if not param_values:
                    param_values = {data_field: str(record)}
                else:
                    if isinstance(param_values, dict):
                        param_values[data_field] = str(record)
                    elif callable(param_values):
                        param_values = param_values()
                        param_values[data_field] = str(record)
                    else:
                        raise ValueError('Неверно задан тип параметра param_values')

                next(self._data_adapter.prepare(data_id, param_values=param_values, \
                                                verbose=self.debug), None)
                self._data_adapter.commit_db_job(data_id)
                #не надо логировать одно и тоже 2 раза (record)
                param_values.pop(data_field, None)
                self.log.debug('Отправка данных на сервер, %s->%s(%s)', \
                               str(record)[:500], data_id, str(param_values))
                return True
            except:
                self.log.exception('Ошибка выполнения запроса к БД %s', data_id)
                return False
        else:
            self.log.error('Не найден источник данных для записи результатов с id = %s', data_id)
            return False

    def _get_record(self, data_id, param_values=None, reader=None, **kwargs):
        '''Получение данных с сервера БД. Возвращает json. Для чтения данных используется reader:
            self._data_adapter::fetch_to_dict или ::fetch_to_json. Первый для чтения и обработки
            плоского датасета, второй для чтения json в первом слобце'''
        #encoding = self._cfg_get('JSONParser', 'Encoding', default='utf-8', assert_key=False)
        if data_id:
            if callable(param_values):
                param_values = param_values()
            assert not param_values or isinstance(param_values, dict), \
                    'Неверный тип параметра param_values'

            try:
                #если не задан reader используем json
                if reader is None:
                    reader = self._data_adapter.fetch_to_json
                    kwargs['encoding'] = self.encoding
                fin = self._data_adapter.prepare(data_id, param_values=param_values, \
                                                 verbose=self.debug)
                data = reader(fin, **kwargs)
                self._data_adapter.commit_db_job(data_id)
                self.log.debug('Получение данных с сервера, %s(%s)', data_id, str(param_values))
                return data
            except:
                self.log.exception('Ошибка выполнения запроса к БД')
        else:
            self.log.error('Не найден источник данных с id = %s', data_id)
        return None

    def _init_db_dicts(self):
        '''Кеширует записи широко используемых справочников'''
        data_id = self._cfg_get('Proc', 'CacheDataID', assert_key=False)
        if data_id:
            self.dict_cache = self._get_record(data_id)
############### Workers #######################
    def _init_proc_queue(self):
        '''Инициализация очереди исполнения расчета'''
        queue_size = self._cfg_get('Node', 'QueueSize', default=0, init=int, assert_key=False)
        workers_count = self._cfg_get('Node', 'ThreadCount', default=1, init=int, assert_key=False)
        proc_module = self._cfg_get('Node', 'ProcModule', assert_key=False)
        proc_class = self._cfg_get('Node', 'ProcClass', assert_key=True)
        keep_objects = self._cfg_get('Node', 'Persist', \
                                     init=(lambda x: int(x) != 0 if x.isnumeric() else bool(x)),\
                                     assert_key=False)
        #Единый массив результатов. Использовать в случае единой отправки данных на сервер
        global_results = self._cfg_get('Node', 'GlobalResults', \
                                     init=(lambda x: int(x) != 0 if x.isnumeric() else bool(x)),\
                                     default=False, assert_key=False)

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
        self._proc_queue = Poll(proc_class, self.log, workers_count, keep_objects, queue_size, **proc_params)
        # Настройка парсера
        self.encoding = self._cfg_get('JSONParser', 'Encoding', default='utf-8', assert_key=True)
        # Подписка на путь в файле относительно которого запускать расчет
        self.parser_cb_path = self._cfg_get('JSONParser', 'ProcCallbackPath', assert_key=True)
        # Путь, который по подписке удаляет ненужные узлы из памяти. Так она не растет
        self.parser_top_path = self._cfg_get('JSONParser', 'WithdrawalPath', assert_key=False)
        # id адаптера данных по которому получать данные
        self.proc_data_id = self._cfg_get('Proc', 'ProcDataID', assert_key=True)
        # id адаптера данных по которому получать данные
        self.result_data_id = self._cfg_get('Proc', 'ResultsDataID', assert_key=False)
        # Размер пакета для отправки данных на сервер
        result_batch_size = self._cfg_get('Proc', 'ResultBatchSize', assert_key=False, init=int, default=0)
        # id адаптера данных для коммита результатов
        self.commit_data_id = self._cfg_get('DataAdapter', 'CommitDataID', assert_key=True)
        # метод выполнения расчета класса расчета
        self.proc_data_entry = self._cfg_get('Proc', 'ProcDataEntry', assert_key=False, default='run')
        # глобальный кеш объектов (используется если источник данных выдает плоскую таблицу)
        self.context_cache = JsonTree.t_dict()

        #результаты расчета
        self.result = ProcResult(proc_class.details_schema, self._push_record, \
                                 self.log, workers_count, batch_size=result_batch_size, \
                                 data_id=self.result_data_id, \
                                 param_values=self.context_param_values) \
                        if global_results else None

        # Логирование
        self.log.info('Настроен обработчик расчета %s.%s', proc_module.__name__, proc_class.__name__)
        self.log.info('Максимальная длина очереди = %s', queue_size)

    def _run_poll(self, max_count=None):
        '''Метод исполнения потока опроса БД'''
        poll_data_id = self._cfg_get('DataAdapter', 'PollDataID', assert_key=True)
        poll_timeout = self._cfg_get('DataAdapter', 'PollTimeout', init=float, default=0, assert_key=False)
        self.log.info('Начало опроса БД')
        cnt = 0
        while not self._stopping:
            job_context = self._get_record(poll_data_id, self.context_param_values)
            if job_context:
                # убираем список из корня
                if isinstance(job_context, list):
                    job_context = job_context[0]
                self._ready_event.clear()
                if not self._new_job(job_context):
                    self._flush_job()

                #ждем окончания обработки задания
                self._ready_event.wait()
            #замираем на время
            self._stop_event.wait(poll_timeout)
            cnt += 1
            
            if max_count and cnt>int(max_count):
                break

    def _cb_proc_finished(self, result, **kwargs):
        '''Событие окончания обработки порции файла по подписке.
        Использовать его для отправки данных на сервер если данные отправляются кусками'''
        self.log.info('Закончена обработка узла(%d) по подписке: %s', \
                      self.part_id, str(self.context_param_values()))
#        self.result.flush()

    def __proc_cb(self, node, parser, job_context):
        '''Callback потокового обрабочика json Для запуска расчета'''
        assert isinstance(parser, JsonParser), 'Неверные атрибуты запуска расчета'
        self.part_id += 1
        _id = node.get('LINK', self.part_id) if hasattr(node, 'get') else self.part_id
        self.log.info('Новая подписка узла(%d:%s): %s', self.part_id,  _id, \
                      str(self.context_param_values()))
        #запуск обработчика
        if self.debug:
            proc = self._proc_queue.object_constructor()
            run = getattr(proc, self.proc_data_entry)
            run(tree_node=parser.rebuild_root(node), context=job_context)

        else:
            self._last_job = self._proc_queue.create_job(self.proc_data_entry, \
                                                         callback=self._cb_proc_finished, \
                                                         #параметры run
                                                         tree_node=parser.json, \
                                                         context=job_context)
    #TODO: переделать. Это параша
    def _prepare_job_context(self, job_context):
        '''Внешний контекст который будет передан в расчет'''
        context = JsonTree.t_dict()
        context['D_Date0'] = job_context.get('D_Date1', None)
        context['D_Date1'] = job_context.get('D_Date2', None)
        context['context_cache'] = self.context_cache
        context['job_context'] = job_context

        return context

    def _new_job(self, job_context):
        '''Получено новое задание'''
        def _get_param_from_job_context(job_context):
            '''Выделяет параметры из возвращенного DBPoller Ответа'''
            assert isinstance(job_context, dict)
            return {'@id':job_context.get('Session_Id', None), \
                    '@batch':job_context.get('Batch_Id', None)}

        def _get_session_id(job_context):
            '''Генерирует session id Для расчета'''
            assert isinstance(job_context, dict)
            assert job_context.get('Session_Id', None), 'Не задан session_id'
            return job_context['Session_Id']
        def _get_batch_id(job_context):
            '''Генерирует id пакета Для расчета'''
            assert isinstance(job_context, dict)
            return job_context.get('Batch_Id', 0)

        assert self._proc_queue, 'Поток исполнения не проинициализирован'
        # Контекст задания
        self.session_id = _get_session_id(job_context)
        self.batch_id = _get_batch_id(job_context)
        self.part_id = 0

        if self.proc_data_id:
            try:
                #Параметры запроса к БД
                param_values = _get_param_from_job_context(job_context)
                self.log.debug('Новый job для исполнения с параметрами: %s', str(job_context))
                self.result.success.set()
                self._proc_queue.success.set()
                #подготовка потока данных запроса
                fin = self._data_adapter.prepare(self.proc_data_id, param_values=param_values, \
                                                 verbose=self.debug)
                #обертка потока для буферезированного чтения
                fin = JsonStream(fin, encoding=self.encoding, \
                              get_value_cb=lambda x: str(x[0]).encode(self.encoding) if x else None)
                #настройка подписки по пути в json
                cb = dict()
                if self.parser_cb_path:
                    cb[self.parser_cb_path] = self.__proc_cb
                #Подписка для удаления ненужных путей
                if self.parser_top_path:
                    cb[self.parser_cb_path] = (lambda *x, **y: True)
                #настройка потокового парсера данных
                parser = JsonParser(encoding=self.encoding, \
                                    callbacks=cb, \
                                    #параметр который будет передан в обработчик по подписке
                                    job_context=self._prepare_job_context(job_context))
                #потоковая обработка json
                if self._data_adapter.query_builder.shrink_names:
                    alias_map = self._data_adapter.query_builder.alias_map
                else:
                    alias_map = None
                #парсим файл с ключами в мелком регистре
                res = parser.parse(fin, alias_map=alias_map)
                #выполнить расчет для того что осталось в json. Вдруг не было ни одной подписки
                if res and not self.parser_cb_path:
                    self.__proc_cb(parser.json, parser, **parser.kwargs)
                #ожидание завершения всех потоков
                self._wait_finished()
                #завершение задания
                self.log.debug('Завершили job для исполнения с параметрами: %s', str(job_context))
                self._job_finished()

                #обработка результатов
                if not res:
                    self.log.error('Ошибка обработки данных в job %s', parser.error)
                    print(parser.error)
                else:
                    #смотрим - смогли ли отправить результаты
                    which = []
                    if not self.result.success.is_set():
                        which.append('отправки' )
                    if not (self.debug or self._proc_queue.success.is_set()):
                        which.append('обработки')
                    if which:
                        self.log.debug('В процессе обработки job возникли ошибки %s: %s', \
                                       ', '.join(which), str(job_context))
                        return False
                    return True
            except:
                self.log.exception('Ошибка выполнения запроса к БД %s', self.proc_data_id)
                return False
        else:
            self.log.critical('Не найден источник данных с id = %s', self.proc_data_id)
            return False

    def _wait_finished(self):
        '''Ожидание завершения расчета'''
        if not self.debug:
            self._proc_queue.wait_finished()
        self.result.wait_finished()
    def _flush_job(self):
        '''Сброс текущего задания'''
        #сброс параметров текущего задания
        self.session_id = None
        self.batch_id = None
        self.part_id = 0
        #закрытие обработки в БД
        self._data_adapter.commit_db_job(self.proc_data_id)
        # готовы для следующей обработки
        self._ready_event.set()
    def _job_finished(self):
        '''Обработка результатов расчета. Глобальный конец расчета'''
        # отправка результатов на сервер
        self.result.flush()
        # сброс кеша
        self.context_cache.clear()
        # подтверждение окончания задания
        param_values = self.context_param_values
        self._push_record(None, self.commit_data_id, param_values, data_field='@C_Result')
        self._flush_job()
#        with open(r'C:\Users\v-liderman\Desktop\result2.json', 'w', encoding='utf-8') as fout:
#            json.dump(self.result, fout, cls=CustomEncoder)
#            if not self.result.empty:
#                self.result.to_json(fout, orient='records')

############### Публичные методы #######################
    def run(self):
        '''Запуск оболочки'''
        if hasattr(self, '_log_handler') and  self._log_handler:
            self._log_handler.start()
        # запуск демона чтения заданий из БД
        thread = threading.Thread(target=self._run_poll)
        thread.daemon = True
        self._db_worker = thread
        self._ready_event.set()
        thread.start()
        self.log.info('Запуск ноды')

    def stop(self):
        '''Остановка оболочки'''
        if hasattr(self, 'log') and self.log:
            self.log.info('Остановка ноды')
        #остановка демона чтения БД
        self._stopping = True
        if hasattr(self, '_db_worker') and self._db_worker:
            self._db_worker.join()
            del self._db_worker

        if hasattr(self, '_proc_queue') and self._proc_queue:
            self._proc_queue.stop_poll()


        if hasattr(self, '_log_handler') and  self._log_handler:
            self._log_handler.stop()

        if hasattr(self, 'result') and self.result:
            self.result.stop_poll()

    def __del__(self):
        self.stop()

def __test__(config=None):
    shell = ProcShell(debug=False)#dba_cfg_dict=config)
#    reader = None
#    reader = shell._data_adapter.fetch_to_dict
    #rec = shell._get_record('APP_Fetch_Next_Batch', {'@Node_id':1})#, reader)
#    rec = shell._get_record('APP_Calc_Subscr', {'@id': 389572, '@batch': 0}, reader)
#    if isinstance(rec, JsonTree):
#        rec = rec.json()
#    with open(r'C:\Users\v-liderman\Desktop\result2.json', 'w', encoding='utf-8') as fout:
#        json.dump(rec, fout, cls=CustomEncoder)
#        shell._data_adapter.execute('APP_Calc_Subscr', fout,  \
#                     param_values = {'@id':389572, '@batch':0}, verbose=True)

    #param_values=shell.context_param_values
    #shell._push_record('Text', 'APP_Append_Log', param_values)
    shell._run_poll(10)
    #shell.run()
#    param_values = {'@id':389572, '@batch':0}
#    shell._new_job(param_values)
    shell.stop()

__test__(Node)
