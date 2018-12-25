# -*- coding: utf-8 -*-
"""
Кеширование записей
Created on Fri Dec 14 14:13:57 2018
@author: V-Liderman
"""
import pickle
import json
import os

class CacheHelper():
    '''Класс для поддержки кеширования'''
    def __init__(self, base_dir=None, depends=None):
        '''Общий конструктор'''
        self.run_dir = base_dir if base_dir else os.path.dirname(__file__)
        self.depends = depends if depends else dict()

    def pick_data(self, data_type, data_id=None, cache_dir='__cache__'):
        '''Восстанавливает данные с диска'''
        assert isinstance(data_type, (str, type)), 'Передать в параметре тип или имя класса данных'
        data_type = data_type.__name__ if isinstance(data_type, type) else str(data_type)
        #проверим дату изменения зависимостей
        depends = []
        if data_type in self.depends:
            depends += map((lambda x: x(data_id) if callable(x) else x), \
                           list(self.depends[data_type]))
        modified = []
        for file in depends:
            if not file:
                continue
            file_name = os.path.join(self.run_dir, file)
            if os.path.isfile(file_name):
                modified.append(os.stat(file_name).st_mtime)

        #существование и дата файла
        file_name = os.path.join(self.run_dir, cache_dir, \
                                 data_type + '__'+(data_id if data_id else '') + '.pickle')

        if os.path.isfile(file_name) and \
           (not modified or os.stat(file_name).st_mtime > max(modified)):
            try:
                with open(file_name, 'rb') as file:
                    return pickle.load(file)
            except:
                pass
        return None

    def dump_data(self, data, data_type=None, data_id=None, cache_dir='__cache__'):
        '''Кеширует данные на диске'''
        if data_type:
            assert isinstance(data_type, (str, type)), 'Передать в параметре тип или имя класса данных'
            data_type = data_type.__name__ if isinstance(data_type, type) else str(data_type)
        else:
            data_type = type(data).__name__
        file_name = os.path.join(self.run_dir, cache_dir, \
                                 data_type + '__'+(data_id if data_id else '') + '.pickle')
        with open(file_name, 'wb') as file:
            pickle.dump(data, file)

    def pick_dir(self, query_dir):
        '''Читает файлы с конфмгурацией запросов из директории'''
        base_dir = os.path.join(self.run_dir, query_dir)
        res_list = []
        try:
            for file in os.scandir(base_dir):
                with open(os.path.join(base_dir, file.name), 'r') as fin:
                    res = json.load(fin)
                    if res:
                        res_list.append(res)
        except:
            pass
        return res_list
