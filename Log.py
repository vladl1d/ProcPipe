# -*- coding: utf-8 -*-
"""
Система логирования
Created on Sun Dec  2 13:32:27 2018
@author: V-Liderman
"""
import logging

class ShellLogHandler(logging.Handler):
    '''Обработчик событий для записи в БД. Запись в БД осуществляется внешним методом'''
    def __init__(self, log_func, level=logging.NOTSET, **kwargs):
        assert log_func
        self._log_func = log_func
        self.kwargs = kwargs
        super().__init__(level)
    def emit(self, record):
        '''Стандартный обработчик записи'''
        assert callable(self._log_func)
        self._log_func(record, **self.kwargs)

