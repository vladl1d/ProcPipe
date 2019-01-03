# -*- coding: utf-8 -*-
"""
Вспомогательные функции модуля расчета
@author: V-Liderman
"""

def get_record_by_key(array, key, value):
    '''Ищет запись в закешированном справочнике. Поиск исходит из того, что записи отсортированы
    по <key>. Поэтому поиск идет через bisect'''
    assert isinstance(array, list) and value
    if not array:
        return None
    lo, hi = 0, len(array)    
    while lo < hi:
        mid = (lo+hi)//2
        assert isinstance(array[mid], dict)
        aval = array[mid].get(key, 0)
        if value == aval:
            return array[mid]
        elif value < aval: hi = mid
        else: lo = mid+1
    return None
