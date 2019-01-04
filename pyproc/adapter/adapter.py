# -*- coding: utf-8 -*-
'''
Содержит инфраструктуру доступа к данным. Выполняется только под Python 3.5+
Created on Sun Oct 28 06:25:49 2018
@author: V-Liderman
'''
#from __future__ import unicode_literals
#from __future__ import print_function as _print
#import sys
import os
import logging
#import pyodbc
import threading
import json as jsonlib
from collections import namedtuple
from .qgraph import QGraph
from .qbuilder import IQueryBuilder, QueryBuilderNestedSelect
from ..core.types import t_dict, t_list
from ..json.stream import JsonStream
#from ..json.parser import JsonParser
from ..util.cache import CacheHelper


#import collections as c

class DataAdapter:
    '''
    Интерфейс для выполнения запросов к БД. Адаптер - как словарь, запросы помечаются метками
    (data_id). для выполнения запроса необходимо указать data_id и передать словарь с параметрами
    (name: value). Значение параметров может отличаться от указанного в запросе. При этом в запрос
    передается пересечение значений параметров и множества параметров в запросе
    '''
    _DATA_TAGS = {'id', 'params'}
    _DEPENDS = {
            'QGraph[]' : ['../pyproc/adapter/qgrapth.py1', lambda s, d='.': '%s/%s.py' %(d, s)]
            }
    def __init__(self, dsn, config, module, base_dir=None, log=None, debug=False,
                 query_builder=QueryBuilderNestedSelect(shrink_names=False, sql_lang=None)):
        '''
        Стандартный конструктор
        '''
        assert dsn, 'Не задана строка соединения'
        assert isinstance(query_builder, IQueryBuilder), 'Не задан построитель запроса'
        assert module and hasattr(module, 'connect'), 'Не передана библиотека работы с БД'

        #строка соединения
        self._dsn = dsn
        #ODBC driver (специфичен для сервера)
        self._module = module
        #helper для построения запросов
        self.query_builder = query_builder
        #конфигурация адаптера
        self.data = dict()
        # подставляем директорию для запросов. Пересобираем _DEPENDS
        config_dir = config if isinstance(config, str) else 'queries'
        depends = {tag: [(lambda x: file(x, config_dir)) if callable(file) else file \
                         for file in self._DEPENDS[tag]] \
                   for tag in self._DEPENDS}
        # хелпер для кеширования запросов
        self.cache = CacheHelper(depends=depends, base_dir=base_dir)
        # кеш открытых соединений с БД
        self.connections = t_dict()
        #логгер для обработки ошибок
        if not log:
            log = logging.getLogger(os.path.split(__file__)[-1])
            _format = '%(asctime)s;%(levelname)s;%(message)s;%(filename)s;%(funcName)s'
            logging.basicConfig(datefmt='%Y.%m.%d %H:%M:%S', style='%', format=_format)

        # лог для логирования ощибок
        self.log = log
        self.debug = debug
        #адаптер проинициализирован
        self.__initialized = False

        #инициализация адаптера
        self._parse_config(config)

    def _parse_config(self, config):
        '''
        Разбор файла с конфигурацией
        '''
        if not config or isinstance(config, str):
            #пробуем прочитать конфигурацию из директории
            config = self.cache.pick_dir(config if config else 'queries')
            if config:
                config = {"config": {"version": "1.0", "data":config}}

        assert isinstance(config, dict), 'Не задана конфигурация адаптера'
        assert 'config' in config, 'Файл имеет не верную структуру'

        config = config['config']
        assert 'data' in config and isinstance(config['data'], list), 'Не верная конфигурация'

        for data in config['data']:
            assert isinstance(data, dict) and \
                   'id' in data and isinstance(data['id'], str) and \
                   'param' not in config or isinstance(data['params'], dict), 'Неверная конфигурация'
            queries = self.cache.pick_data('QGraph[]', data['id'])
            if not queries:
                #разбор запросов
                job_id = self.new_db_job('QGraph[]')
                queries = []
                for key in (data.keys() - DataAdapter._DATA_TAGS):
                    if key == 'query':
                        entity = QGraph.parse_conf_node(data[key])
                        entity.validate(self.connections[job_id])
                    else:
                        entity = QGraph.parse_conf_node(data[key], name=key)
                        entity.validate(self.connections[job_id])

                    queries.append(entity)

                self.commit_db_job('QGraph[]')
                assert queries, 'Для адаптера не задано ни одного запроса'
                self.cache.dump_data(queries, 'QGraph[]', data['id'])

            self.data[data['id']] = {'params': data['params'], 'queries': queries}

        self.__initialized = True

    ############################## Работа с соедиенениями. Каждый data_id в своем соединении
    def new_db_job(self, job_id=None):
        '''Открывает соединение с БД для выполнения операций'''
        if not job_id:
            job_id = len(self.connections.keys()) + 1
        thrd_id = threading.current_thread().ident
        job_id += '_' + str(thrd_id)
        conn = self.connections.get(job_id, None)
        if not conn:
            conn = self._module.connect(self._dsn)
#            conn.autocommit = True
            self.connections[job_id] = conn
        else:
            #сбор предыдущей транзакции
            self.log.warning('Зашли снова в: %s', job_id)
            print('Зашли снова в:', job_id)
            conn.commit()
        print('Новый запрос:', job_id)
        return job_id
    def _finish_db_job(self, tran_func='commit', job_id=None):
        '''Закрывает операцию. Если не задан id операции закрывает все'''
        def _close_conn(conn, tran_func):
            try:
                getattr(conn, tran_func, lambda: True)()
                conn.close()
            except Exception as error:
                self.log.exception('Не смогли закрыть запрос: %s', job_id)
                print('Не смогли закрыть:', str(error))

        if job_id:
            thrd_id = threading.current_thread().ident
            job_id += '_' + str(thrd_id)
            if job_id in self.connections:
                _close_conn(self.connections[job_id], tran_func)
                self.connections.pop(job_id)
                print('Закрыли запрос:', job_id)
        else:
            for job_id in self.connections:
                _close_conn(self.connections[job_id], tran_func)

    def commit_db_job(self, job_id=None):
        '''Закрывает операцию с применением результата'''
        self._finish_db_job(tran_func='commit', job_id=job_id)
    def rollback_db_job(self, job_id=None):
        '''Закрывает операцию с отменой результата'''
        self._finish_db_job(tran_func='rollback', job_id=job_id)

    def __del__(self):
        '''
        Стандартный деструктор
        '''
        if hasattr(self, 'connections') and self.connections:
            try:
                self._finish_db_job()
            except:
                pass
    ##################################### Публичные методы выполнения запросов
    def prepare(self, data_id, param_values=None, debug=False):
        '''Подготавливает курсор для последовательного получения данных из БД
        '''
        assert self.__initialized, 'Адаптер не проинициализирован'
        assert data_id in self.data, 'Неверный идентификатор запроса %s' % data_id
        assert 'queries' in self.data[data_id], 'Не задан ни один запрос для %s' % data_id

        conn_id = self.new_db_job(data_id)
        for query in self.data[data_id]['queries']:
            params = self.data[data_id]['params']
            if params:
                assert isinstance(params, dict), 'Не верно заданы параметры запроса'
                if param_values:
                    param_values = [param_values.get(p_name, None) \
                                    for p_name in sorted(params.keys()) if p_name != '()']
                params = [(p_name, params[p_name]) for p_name in params.keys()]
#                params = sorted(params, key=lambda x: x[0])

            if query.type.upper() in 'TV':
                sql = self.query_builder.get_query_sql(query, params)
            else:
                sql = self.query_builder.get_proc_sql(query, params)
            if debug:
                print(sql)
                print(str(param_values)[:500])
            # Не может odbc работать с пустыми параметрами
            exec_params = [sql]
            if param_values:
                exec_params.append(param_values)
            yield (self.connections[conn_id].cursor().execute(*exec_params), query)

    def push_record(self, record, data_id, param_values=None, data_field='@C_Data'):
        '''Отправляет результат на сервер.
        3 способа передать параметры: не передавать, словарь со значениями, функция с словарем
        с названиями локальных полей -> названия параметров'''
        if data_id:
            assert self.__initialized, 'Адаптер к БД не проиницирован'
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

                next(self.prepare(data_id, param_values=param_values, debug=self.debug), None)
                self.commit_db_job(data_id)
                #не надо логировать одно и тоже 2 раза (record)
                param_values.pop(data_field, None)
                self.log.debug('Отправка данных на сервер, %s->%s(%s)', \
                               str(record)[:500], data_id, str(param_values))
                return True
            except:
                #убеждаемся что запрос все же закрыли
                self.commit_db_job(data_id)
                self.log.exception('Ошибка выполнения запроса к БД %s', data_id)
                return False
        else:
            self.log.error('Не найден источник данных для записи результатов с id = %s', data_id)
            return False

    def get_record(self, data_id, param_values=None, reader=None, **kwargs):
        '''Получение данных с сервера БД. Возвращает json. Для чтения данных используется reader:
            self._data_adapter::fetch_to_dict или ::fetch_to_json. Первый для чтения и обработки
            плоского датасета, второй для чтения json в первом слобце'''
        #encoding = self._cfg_get('JSONParser', 'Encoding', default='utf-8', assert_key=False)
        if data_id:
            assert self.__initialized, 'Адаптер к БД не проиницирован'

            if callable(param_values):
                param_values = param_values()
            assert not param_values or isinstance(param_values, dict), \
                    'Неверный тип параметра param_values'

            try:
                #если не задан reader используем json
                if reader is None:
                    reader = self.fetch_to_dict
                typed_cursor = self.prepare(data_id, param_values=param_values, debug=self.debug)
                data = reader(typed_cursor, **kwargs)
                self.commit_db_job(data_id)
                self.log.debug('Получение данных с сервера, %s(%s)', data_id, str(param_values))
                return data
            except:
                #убеждаемся что запрос все же закрыли
                self.commit_db_job(data_id)
                self.log.exception('Ошибка выполнения запроса к БД')

        else:
            self.log.error('Не найден источник данных с id = %s', data_id)
        return None

    def execute(self, data_id, output, headers=False, delim=';', param_values=None, debug=False):
        '''Получение результата запроса и сохранение их в файл
        '''
        # Готовим итератор запроса
        for cursor, _ in self.prepare(data_id, param_values, debug=debug):
            self.write_output(cursor, output, headers, delim)
        self.commit_db_job(data_id)

    ####################################### Сервисные методы упаковки результатов запроса
    def fetch_to_dict(self, typed_cursor, result=None):
        '''записывает результаты в структуру list of maps(key, value)'''
        if result is None:
            result = t_dict()
        cursor, schema = next(typed_cursor, (None, None))
        while cursor is not None:
            entity = getattr(schema, 'name', '_')
            pk = getattr(schema, 'PK', [None])[0]
            result[entity] = node_list = t_list(pk=pk)
            # названия колонок
            info = [node_list[0] for field in cursor.description]
            # переводим названия в полные
            if self.query_builder.alias_map:
                info = [self.query_builder.alias_map.get(fld, fld) for fld in info]
            obj = namedtuple(entity, info)
            #data = cursor.fetchall()
            for data in cursor:
                result.append(obj._make(data))
            cursor, schema = next(typed_cursor, (None, None))
        return result

    def fetch_to_json(self, typed_cursor, encoding='utf-8', hook=None):
        '''получает данные в json-формате'''
        def _pairs_hook(node):
            if self.query_builder.alias_map:
                return {self.query_builder.alias_map.get(nam.lower(), nam.lower()): val \
                        for nam, val in node}
            return {nam.lower(): val for nam, val in node}

        cursor, schema = next(typed_cursor, (None, None))
        json = dict()
        while cursor is not None:
            fin = JsonStream(cursor, encoding=encoding, \
                        get_value_cb=(lambda x: str(x[0]) if x else None))

            js = jsonlib.load(fin, object_pairs_hook=_pairs_hook)
            if js:
                if isinstance(js, list):
                    js = {getattr(schema, 'name', '_').replace('.', '_'): js}
                if callable(hook):
                    hook(js, schema)
                json.update(js)
                cursor, schema = next(typed_cursor, (None, None))
            else:
                break
        #не пропускаем гавно
        return json

    @staticmethod
    def write_output(cursor, output, headers=False, delim=';'):
        '''
        Запись результата запроса в файл. На входе итератор запроса
        '''
        assert output, 'Выходной поток не проинициализирован'
        if not cursor:
            print('Пустой курсор')
            return
        if headers:
            output.write(delim.join([cname[0] for cname in cursor.description]))
            output.write('\n')
        for row in cursor:
            output.write(delim.join(row))
                #output.write('\n')
