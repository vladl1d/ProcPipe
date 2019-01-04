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
#import threading
import importlib
import time
import json as jsonlib
import jsonpath
#import uuid
#import pandas as pd
#from six.moves import queue
from ..json.stream import JsonStream
from .results import ProcResult
from .ishell import IShell
#from .types import t_dict
#from .log import ShellLogHandler

__RUN_DIR__ = os.path.dirname(__file__)

class ProcShell(IShell):
    ''' Класс, котором осуществляется выполнение экземпляров расчета, который содержит контекст
    и глобальные переменные расчета'''
    def __init__(self, ini_file=None, dba_cfg_dict=None, base_dir=None, debug=False):
        #Класс, исполняющий расчет
        self._proc = None
        # глобальные результаты расчета
        self.result = None
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
            'schema_map': dict(),
            # кеш справочников в виде (Название справочника#ключ)->запись
            'dict_map': dict()
            }

        # кеш к данным
#        self.dict_cache = None

        #инициализация среды
        super().__init__(ini_file if ini_file else 'DataAdapter.ini', dba_cfg_dict, base_dir)
        # инициализации рабочего класа
        self._init_proc()
        # инициализация результатов расчета
        self._init_result()
        # кешироваие данных для работы
        self._init_db_dicts()

############### Properties #######################
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

    def log_tree_node_error(self, msg, *arg, **kwarg):
        '''Логирует ошибку в данных расчета'''
        assert self.log, 'Система логирования не проинициализирована'
        log = self.log#.getChild('data')
        #выделяем контекст исполнения операции
        #TODO: Переделать по нормальному. Пока просто заглушка
        log.warning(msg, *arg, **kwarg)


################ кеш справочников ##############
    def dict_hook(self, js, schema):
        '''Индексирует записи справочников для быстрого доступа'''
        pk = schema.PK
        if not pk:
            return
        if isinstance(pk, list):
            pk = pk[0] 
        assert isinstance(js, dict)
        for key in js:
            for record in js[key]:
                pk_val = record.get(pk.lower(), None)
                if pk_val:
                    self.context['dict_map'].setdefault('%s#%s' % (str(key), str(pk_val)), record)

    def _init_db_dicts(self):
        '''Кеширует записи широко используемых справочников'''
        data_id = self.cfg_get('Cache', 'CacheDataID', assert_key=False)
        if data_id:
            self._data_adapter.get_record(data_id, reader=self._json_reader, hook=self.dict_hook)
    
############### Workers #######################
    def _init_proc(self):
        '''Инициализация очереди исполнения расчета'''
        self.context['node_id'] = self.cfg_get('Node', 'NodeID', init=int, assert_key=True)
        proc_module = self.cfg_get('Proc', 'ProcModule', assert_key=False)
        proc_class = self.cfg_get('Proc', 'ProcClass', assert_key=True)

        try:
            if proc_module:
                proc_module = importlib.import_module(proc_module)
                proc_class = getattr(proc_module, proc_class)
            else:
                proc_class = globals()[proc_class]
        except Exception as excpt:
            self.log.critical('Не найден класс для исполнения запроса')
            raise excpt

        self._proc = proc_class(shell=self)
        # Настройка парсера
        # id адаптера данных по которому получать данные для расчета
        self.proc_data_id = self.cfg_get('Proc', 'ProcDataID', assert_key=True)
        # id адаптера данных для коммита результатов
        self.commit_data_id = self.cfg_get('Proc', 'CommitDataID', assert_key=True)

        # Логирование
        self.log.info('Настроен обработчик расчета %s.%s', proc_module.__name__, proc_class.__name__)

    def _init_result(self):
        '''Инициализация результатов исполнения расчета'''
        #Единый массив результатов. Использовать в случае единой отправки данных на сервер
        global_results = self.cfg_get('Node', 'GlobalResults', init=bool, default=False, assert_key=False)
        #результаты расчета
        # Размер пакета для отправки данных на сервер
        result_batch_size = self.cfg_get('Results', 'ResultBatchSize', assert_key=False, init=int, default=0)
        # id адаптера данных по которому получать данные
        result_data_id = self.cfg_get('Results', 'ResultsDataID', assert_key=False)
        self.result = ProcResult(self._data_adapter.push_record, \
                                 batch_size=result_batch_size, \
                                 #параметры которые передадутся callback(push_record)
                                 data_id=result_data_id, \
                                 param_values=self.context_param_values) \
                        if global_results else None

 
    def _run_poll(self, max_count=None):
        '''Метод исполнения потока опроса БД'''
        poll_data_id = self.cfg_get('Poll', 'PollDataID', assert_key=True)
        poll_timeout = self.cfg_get('Poll', 'PollTimeout', init=float, default=0, assert_key=False)
        poll_root = self.cfg_get('Poll', 'PollRoot', assert_key=True)
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
                # заняты
                self._ready = False
                # запуск обработчика
                self.new_job(job_context)
                # готовы для следующей обработки
                self._ready = True

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
        assert self._proc, 'Класс исполнения не проинициализирован'
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
                #подготовка потока данных запроса. вызов обработчика
                res = self._data_adapter.get_record(self.proc_data_id,
                                                    param_values=param_values,
                                                    reader=self._parse_job_record,
                                                    #kwargs
                                                    job_context=context)
                #завершение задания
                self.log.debug('Завершили job для исполнения с параметрами: %s', str(job_context))
                self.job_finished(res)

                #обработка результатов
                if res and not self.result.success:
                    self.log.debug('В процессе обработки job возникли ошибки отправки: %s', \
                                   str(job_context))

                return True
            except:
                self.log.exception('Ошибка выполнения запроса к БД %s', self.proc_data_id)
                self.job_finished(False)
                return False
        else:
            self.log.critical('Не найден источник данных с id = %s', self.proc_data_id)
            return False

    def job_starting(self, param_values=None):
        '''Событие начала обработки задания'''
        self.result.success = True
#        self._proc.success = True

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
                    'schema': self.context['schema'],
                    'dict_map': self.context['dict_map']
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
                start_path = self.cfg_get('Proc', 'ProcRoot', assert_key=True)
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
        proc_data_entry = self.cfg_get('Proc', 'ProcDataEntry', assert_key=False, default='run')

#        assert isinstance(parser, JsonParser), 'Неверные атрибуты запуска расчета'
        self.context['part_id'] += 1
        #запуск обработчика
        run = getattr(self._proc, proc_data_entry)
        res = run(tree_node=node, context=job_context)
        return res

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
                             'cache': dict(), 'schema_map': dict(), 'dict_map': dict()})

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
        self._ready = True
#        thread.start()
        self.log.info('Запуск ноды')
        #отказался от потока, потому что это безсмысленно
        self._run_poll(max_count)

    def stop(self):
        '''Остановка оболочки'''
        if hasattr(self, 'log') and self.log:
            self.log.info('Остановка ноды')

        if hasattr(self, '_log_handler') and  self._log_handler:
            self._log_handler.stop()

    def __del__(self):
        self.stop()
