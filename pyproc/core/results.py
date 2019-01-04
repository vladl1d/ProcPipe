# -*- coding: utf-8 -*-
"""
Результаты выполнения с ассинхронной записью и отправкой данных на сервер. Для этого используется
очередь, которая контролирует свой batch_size и при привышении которого инициирует запись данных
на сервер. Для этого используется callback, который и должен отправить данные на сервер.
@author: V-Liderman
"""
#from __future__ import unicode_literals
#from __future__ import print_function as _print
import json
#import uuid
#import pandas as pd
#from uuid import UUID
#from datetime import datetime
from ..util.util import CustomEncoder

#from Proc import ProcCR

class ProcResult():
    '''
    Класс инкапсулирующий результат расчета.
    Класс копит batch И отправляет его на СУБД
    '''
    def __init__(self, flush_func, batch_size=100, **kwargs):
        '''Создание класса по колонкам'''
        self._result = []
        #размер пакета для потоковой записи в БД
        self._batch_size = int(batch_size)
        #адаптер доступа к данным
        self._flush_func = flush_func
        self._kwargs = kwargs
        self.success = True
#        self.encoder = CustomEncoder().default

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
    def append(self, new):
        '''Добавление новой строки к результату'''
        self._result.append(new)
        #self._append2pd_dataframe(new)
        self._check_batch()
    def _check_batch(self):
        '''Проверка наполнения batch'''
        if self._batch_size > 0 and self.result_count > self._batch_size:
            self.flush()
    def flush(self):
        '''Сбрасывает результаты расчета на СУБД'''
        if not self._flush_func or not callable(self._flush_func) or not self.result_count:
            return
        results = self._result
        #сбрасываем результаты для исключения повторной отправки
        self._result = []

        #получаем json для отправки
        js_data = json.dumps(results, cls=CustomEncoder)
        #отправка
        if not self._flush_func(js_data, **self._kwargs):
            self.success = False
