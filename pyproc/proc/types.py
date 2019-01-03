# -*- coding: utf-8 -*-
"""
@author: V-Liderman
"""

class ProcException(Exception):
    '''Исключение неполноты данных'''
    pass

#Классы для схемы данных
class _not_null(tuple):
    '''Клас обозначающий не пустое поле'''
    pass
class _null(tuple):
    '''Клас обозначающий пустое поле'''
    pass
class _calc(tuple):
    '''Клас обозначающий результат расчета'''
    pass
