# -*- coding: utf-8 -*-
"""
Кеширование записей
Created on Fri Dec 14 14:13:57 2018
@author: V-Liderman
"""
import pickle
import os

class CacheHelper():
    '''Класс для поддержки кеширования'''
    def __init__(self, base_dir=None, depends=None):
        '''Общий конструктор'''
        self.run_dir = os.path.dirname(__file__)
        _cache_dir = os.path.join(self.run_dir, '__cache__')
        self.base_dir = base_dir if base_dir else _cache_dir
        self.depends = depends if depends else dict()

    def pick_data(self, data_type, data_id=None):
        '''Восстанавливает данные с диска'''
        assert isinstance(data_type, (str, type)), 'Передать в параметре тип или имя класса данных'
        data_type = data_type.__name__ if isinstance(data_type, type) else str(data_type)
        #проверим дату изменения зависимостей
        depends = []
        if data_type in self.depends:
            depends += list(self.depends[data_type])
        modified = []
        for file in depends:
            file_name = os.path.join(self.run_dir, file)
            if os.path.isfile(file_name):
                modified.append(os.stat(file_name).st_mtime)

        #существование и дата файла
        file_name = os.path.join(self.base_dir, data_type + '__'+(data_id if data_id else '') + \
                                 '.pickle')

        if os.path.isfile(file_name) and \
           (not modified or os.stat(file_name).st_mtime > max(modified)):
            try:
                with open(file_name, 'rb') as file:
                    return pickle.load(file)
            except:
                pass
        return None

    def dump_data(self, data, data_type=None, data_id=None):
        '''Кеширует данные на диске'''
        if data_type:
            assert isinstance(data_type, (str, type)), 'Передать в параметре тип или имя класса данных'
            data_type = data_type.__name__ if isinstance(data_type, type) else str(data_type)
        else:
            data_type = type(data).__name__
        file_name = os.path.join(self.base_dir, data_type + '__'+(data_id if data_id else '') + \
                                 '.pickle')
        with open(file_name, 'wb') as file:
            pickle.dump(data, file)
