# -*- coding: utf-8 -*-
"""
Результаты выполнения с ассинхронной записью и отправкой данных на сервер. Для этого используется
очередь, которая контролирует свой batch_size и при привышении которого инициирует запись данных
на сервер. Для этого используется callback, который и должен отправить данные на сервер.
@author: V-Liderman
"""
#from __future__ import unicode_literals
#from __future__ import print_function as _print
import threading
import json
#import uuid
#import pandas as pd
#from uuid import UUID
#from datetime import datetime
from .poller import Poller
from ..util.util import CustomEncoder

#from Proc import ProcCR

class ProcResult(Poller):
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

    def __init__(self, columns, flush_func, log, workers_count=None, batch_size=100, **kwargs):
        '''Создание класса по колонкам'''
        #self._result = self._create_pd_dataframe(columns)
        self._schema = columns
        self._result = []
        self._lock = threading.Semaphore(1)
        #размер пакета для потоковой записи в БД
        self._batch_size = int(batch_size)
        #адаптер доступа к данным
        self._flush_func = flush_func
        self._kwargs = kwargs
#        self.encoder = CustomEncoder().default
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
#            self.logger.info('Push result^ %d', self.result_count)
            # превращаем в текст кастомные типы
            self._result.append(new)
            #self._append2pd_dataframe(new)
            self._check_batch()
#            self._lock.release()
    def append(self, new):
        '''Добавление новой строки к результату'''
        self.create_job('_async_append', callback=None, new=new)
    def _check_batch(self):
        '''Проверка наполнения batch'''
        if self._batch_size > 0 and self.result_count > self._batch_size:
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
#            self.logger.info('Сброс результатов: %d', len(self._result))
            #получаем json для отправки
            js_data = json.dumps(self._result, cls=CustomEncoder)
            #отправка
            if not self._flush_func(js_data, **self._kwargs):
                self.success.clear()
            #сбрасываем результаты для исключения повторной отправки
            self._result.clear()
            if outside:
                self._lock.release()

    def flush(self):
        '''Сбрасывает результаты расчета на СУБД'''
        self._flush(False)
