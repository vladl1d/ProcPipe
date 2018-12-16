# -*- coding: utf-8 -*-
"""
Обертка для потокового бинарного чтения из любого итератора и Json-парсер
Created on Mon Oct 29 15:52:11 2018
@author: V-Liderman
"""
import io
import sys
from pprint import pprint

from yajl import (
    YajlContentHandler, YajlParser,
    YajlParseCancelled, YajlError
)

class lc_dict(dict):
    '''Справочник с ключами в нижнем регистре'''
    def __init__(self, *arg, **kwarg):
        '''Перегружаем стандартный конструктор'''
        super().__init__(*arg, **kwarg)
        for key in self.keys():
            if isinstance(key, str) and key!=key.lower():
                self[key] = super().pop(key)
    def get(self, index, default=None):
        '''обертка вокруг dict'''
        if isinstance(index, str):
            index = index.lower()
        return super().get(index, default)
    def pop(self, index, default=None):
        '''обертка вокруг dict'''
        if isinstance(index, str):
            index = index.lower()
        return super().pop(index, default)
    def __getitem__(self, index):
        if isinstance(index, str):
            index = index.lower()
        return super().get(index, None)
    def __setitem__(self, index, value):
        if isinstance(index, str):
            index = index.lower()
        return super().__setitem__(index, value)
    def __contains__(self, item):
        if isinstance(item, str):
            item = item.lower()
        return super().__contains__(item)
    def copy(self):
        return type(self)(super().copy())

#################################################################################################
class JsonStream(io.BufferedIOBase):
    '''
    Класс для чтения JSON в виде потока байтов
    '''
    def __init__(self, cursors, encoding='utf-8', get_value_cb=None):
        '''
        Чтение данных из буфера
        '''
        self.itr_cursors = iter(cursors)
        self.cursor = next(self.itr_cursors, None)
        self.encoding = encoding
        if self.cursor:
            self.cursor = iter(self.cursor)
        if get_value_cb:
            self.get_value_cb = get_value_cb
        else:
            self.get_value_cb = (lambda x: str(x).encode(encoding) if x else None)
        self.buffer = None

        super(JsonStream, self).__init__()

    def _next_cursor(self):
        '''
        Список курсоров - итератор
        '''
        if self.cursor:
            value = next(self.cursor, None)
            if value is None:
                if hasattr(self.cursor, 'commit') and callable(self.cursor.commit):
                    self.cursor.commit()
                self.cursor = next(self.itr_cursors, None)
#                if self.cursor:
#                    self.cursor = iter(self.cursor)
                return self._next_cursor()
            else:
                return value
        return None

    def _read_iter(self, size=0, max_size=-1):
        ''' Рекурсивное чтение данных из итератора. Читает сколько указано в буфере
        '''
        if self.buffer:
            data = self.buffer
            self.buffer = None
        else:
            data = self.get_value_cb(self._next_cursor())
        _len = len(data) if data else 0
        size += _len
        if data and (size <= max_size or max_size == -1):
            next_data = self._read_iter(size, max_size)
            if next_data:
                data += next_data
            return data
        elif data and size > max_size:
            self.buffer = data[max_size:]
            return data[:max_size]
        else:
            return data

    def read(self, size=-1):
        '''
        Чтение данных из буффера
        '''
        data = self._read_iter(max_size=size)
        return data

################################################################################################
class JsonTree(object):
    '''
    JSON в структурированном виде. Дерево для синтаксического разбора
    '''
    t_dict = lc_dict
    t_list = list

    def __init__(self, value=None, key=None, parent=None):
        self.parent = parent
        self.key = key
        self.value = value
        # Переменные для последующего разбора данных
        # Контекст узла (Интервалы).
        self.context = None        

    @property
    def type(self):
        '''Получение типа текущего элемента'''
        return type(self.value)
    @property
    def root(self):
        if self.parent:
            return self.parent.root
        else:
            return self
    def __iter__(self):
        if isinstance(self.value, (JsonTree.t_list, JsonTree.t_dict)):
            return iter(self.value)
        else:
            raise TypeError('Тип значения не поддерживает операцию')

    def __next__(self):
        pass
    def get(self, index, default):
        '''обертка вокруг dict'''
        if isinstance(self.value, (JsonTree.t_dict)):
            if isinstance(index, str):
                index = sys.intern(index)
            return self.value.get(index, default)
        elif index == 0:
            return self.value if self.value else default
        else:
            raise TypeError('Тип значения не поддерживает операцию')
    def __getitem__(self, index):
        if isinstance(self.value, (JsonTree.t_list, JsonTree.t_dict)):
            if isinstance(index, str):
                index = sys.intern(index)
            return self.value[index]
        elif index == 0:
            return self.value
        else:
            raise TypeError('Тип значения не поддерживает операцию')
    def __setitem__(self, index, value):
        if isinstance(self.value, (JsonTree.t_list, JsonTree.t_dict)):
            if isinstance(index, str):
                index = sys.intern(index)
            self.value[index] = value
        elif index == 0:
            self.value = value
        else:
            raise TypeError('Тип значения не поддерживает операцию')
    def __contains__(self, item):
        if isinstance(self.value, (JsonTree.t_list, JsonTree.t_dict)):
            return item in self.value
        else:
            raise TypeError('Тип значения не поддерживает операцию')
    def keys(self):
        '''
        Обертка вокруг dict
        '''
        if isinstance(self.value, JsonTree.t_dict):
            return self.value.keys()
        elif isinstance(self.value, JsonTree.t_list):
            return range(len(self.value))
        else:
            raise TypeError('Тип значения не поддерживает операцию')
    def __len__(self):
        if isinstance(self.value, (JsonTree.t_list, JsonTree.t_dict)):
            return len(self.value)
        else:
            raise TypeError('Тип значения не поддерживает операцию')
    def __call__(self, **args):
        return self.value
    def __enter__(self, *args):
        pass
    def __exit__(self, *args):
        pass
    def __str__(self):
        return str(self.json())
    #@property
    def json(self):
        '''
        Замещает экземпляры класса их значениями
        '''
        def _object_value(obj):
            if isinstance(obj, JsonTree):
                return _object_value(obj.value)
            elif isinstance(obj, JsonTree.t_list):
                for i, value in enumerate(obj):
                    obj[i] = _object_value(value)
            elif isinstance(obj, JsonTree.t_dict):
                for key in obj.keys():
                    obj[key] = _object_value(obj[key])

            return obj

        return _object_value(self)

class JsonParser(YajlContentHandler):
    '''
    Класс для сборки JSON-объекта из строки и вызова обработчика по подписке
    '''
    def __init__(self, callbacks=None, encoding='utf-8', **kwargs):
        '''Инициализация объекта для сборки JSON'''
        if callbacks:
            assert isinstance(callbacks, dict), 'Неверный тип параметра'
        self.encoding = encoding
        self.root = None
        self.current = None
        self.path = []
        self.callbacks = callbacks
        self.kwargs = kwargs
        self.parser = None
        self.error = None
        self.alias_map = None
#        self.lower_keys = False

    def _push(self, value=None, key=None):
        '''Создание нового уровня иерархии в стеке'''
        _next = JsonTree(value, parent=self.current, key=key)
        self.current = _next
        if self.root is None:
            self.root = self.current

    def _pop(self):
        '''Извлечение готового элемента из стека'''
        value = self.current
        if self.current is not None:
            self.current = self.current.parent
        return value

    def _set_value(self, value):
        '''
        Устанавливает  значение для нескалярного типа
        Для структуры - значение для ключа
        Для массиса - добавляет очередное значение
        '''
        if self.current is None:
            #Чтобы не вставлять изначально пустой узел, подбираем первый обработанный узел
            # в качестве корня. Если присляли скаляр - создаем узел
            if isinstance(value, JsonTree):
                self.current = value
                self.root = value
                return
            else:
                self.current = JsonTree()
                self.root = self.current
        if isinstance(self.current.value, JsonTree.t_dict):
            assert self.current.key, 'Ошибка интерпретации структуры. На задан ключ для значения'
            #TODO: проверить правильность заполнения ключа
            #if isinstance(value, JsonTree): value.key = self.current.key
            if isinstance(self.current.key, str):
                self.current.key = sys.intern(self.current.key)
            self.current[self.current.key] = value
            self.current.key = None
        elif isinstance(self.current.value, JsonTree.t_list):
            self.current.value.append(value)
        else:
            self.current.value = value

    def _callback_trigger(self, path, node):
        '''Вызов событий по подписке'''
        if self.callbacks:
            for trigger in self.callbacks.keys():
                _callback = self.callbacks[trigger]
                if isinstance(trigger, (list, tuple)):
                    trigger = '/'.join(trigger)
                else:
                    trigger = str(trigger)
                if isinstance(path, list):
                    path = '/'.join(path)

                _cmp = lambda x, y: x == y if y[:2] == './' else x.endswith(y)
                if _cmp(path.lower(), trigger.lower()):
                    #Верни True - если успешно обработал узел и мы его грохнем из обработки
                    #Иначе верни False
                    return _callback(node, self, **self.kwargs)

        return False

    #события от синтаксического генератора
    def parse_start(self):
        '''Обработка события начала разбора'''
        self.root = None #JsonTree()
        self.current = None #self.root
        self.path = ['.']
    def parse_buf(self):
        '''Обработка события окончания обработки буфера'''
        pass
    def parse_complete(self):
        '''Обработка события окончания логического разбора JSON'''
        pass
    def yajl_null(self, ctx):
        '''Обработка события обработки значения'''
        self._set_value(None)
    def yajl_boolean(self, ctx, boolVal):
        '''Обработка события обработки значения'''
        self._set_value(boolVal)
    def yajl_integer(self, ctx, integerVal):
        '''Обработка события обработки значения'''
        self._set_value(int(integerVal))
    def yajl_double(self, ctx, doubleVal):
        self._set_value(float(doubleVal))
    def yajl_number(self, ctx, stringVal):
        '''Обработка события обработки значения'''
        if stringVal.isdigit():
            self.yajl_integer(ctx, stringVal.decode(self.encoding))
        else:
            self.yajl_double(ctx, stringVal.decode(self.encoding))
    def yajl_string(self, ctx, stringVal):
        '''Обработка события обработки значения'''
        self._set_value(stringVal.decode(self.encoding))
    def yajl_start_map(self, ctx):
        '''Обработка события начала структуры'''
        # обработка пути
        if self.current is not None and self.current.key:
            self.path.append(self.current.key)
            
        self._push(JsonTree.t_dict())
    def _map_2key(self, key):
        if self.alias_map:
            val = self.alias_map.get(key, key)
            val = val.name.replace('.', '_') if hasattr(val, 'name') else str(val)
        else:
            val = key
        return val
    def yajl_map_key(self, ctx, stringVal):
        '''Обработка события ключ структуры'''
        self.current.key = self._map_2key(stringVal.decode(self.encoding))
    def yajl_end_map(self, ctx):
        '''Обработка события окончания структуры'''
        value = self._pop()
        #восстановим у структуры key. В current после pop - родитель
        if self.current is not None:
            value.key = self.current.key
        
        #обработка пути
        path = self.path.copy()
        if self.path:
            self.path.pop()
        #триггер
        if self._callback_trigger(path, value):
            value = None
            return
        else:
            self._set_value(value)
    def yajl_start_array(self, ctx):
        '''Обработка события начала массива'''
        # массив запоминает ключ родителя
        key = self.current.key if self.current is not None else None
        self._push(JsonTree.t_list(), key=key)
    def yajl_end_array(self, ctx):
        '''Обработка события окончания массива'''
        if self.current.key:
            self.current.key += '[]'
        self._set_value(self._pop())

    @property
    def json(self):
        '''Возвращает результат разбора'''
        return self.root

    @staticmethod
    def rebuild_root(node):
        '''Строит дерево вверх и востанавливает ссылки. Возвращает копию'''
        current = node
        parent = current.parent
        while parent is not None:
            # обработка. Не портим списки, в словари засовываем ссылки на фейковые массивы.
            # их все равно перезапишут
            if isinstance(parent.value, JsonTree.t_list):
                parent = JsonTree(JsonTree.t_list(), parent.key, parent.parent)
                parent.value.append(current)
            if isinstance(parent.value, JsonTree.t_dict) and parent.key:
                parent.value[parent.key] = current
            current = parent
            parent = current.parent
        return current

    def parse(self, input_stream, alias_map=None, buf_size=65536):
        '''Потоковый разбор JSON'''
        assert input_stream, 'Не задан источник данных'
        self.parser = YajlParser(self, buf_size)
        self.parser.allow_multiple_values = True
        self.parser.dont_validate_strings = True
        assert alias_map is None or isinstance(alias_map, dict), 'Неверный тип справочника aliasов'
        self.alias_map = alias_map
        try:
            self.parser.parse(input_stream)
        except YajlParseCancelled as err:
            print(err)
            self.error = err
            return False
        except YajlError as err:
            print(err)
            self.error = err
            return False
        else:
            return True

import time
from datetime import datetime
import sys, os
sys.path.append(os.path.dirname(__file__))
from Util import get_typed_value
from Cache import CacheHelper 
x = 0
def __test1__():
    ''' abc
    '''    
    def _cb(node, parser, fout, **kwargs):
        global x
        if x is None: x = 0
        else: x += 1
#        root = parser.rebuild_root(node)
        fout.write('%s\t%s\t%s' % (str(datetime.now()), x, node.get('LINK', 0)))
        fout.write('\n')
        return True
    
    c = CacheHelper(r'C:\Users\v-liderman\Desktop')
    alias_map = None
    alias_map = c.pick_data('Alias_map', 'APP_Calc_Subscr')
    with open(r'C:\Users\v-liderman\Desktop\t3.json', 'rb') as fin:
        with open(r'C:\Users\v-liderman\Desktop\out.json', 'w') as fout:
            #src = '{"key":[1,{"c":"d"}]}'
            #fout = io.StringIO()
            #json.dump(Q, fout)
            #src = fout.getvalue()
            #print('Source')
            #print(src)
            parser = JsonParser(callbacks={'SD_Subscr': _cb, \
                                           'SD_Conn_Points': lambda *k, **e: True}, \
                                encoding = 'windows-1251', \
                                fout = fout)
            res = parser.parse(fin, alias_map=alias_map)
            print('Result')
            if res:
#                print(str(parser.json))
                pass
            else:
                print(parser.error)

#__test1__()
from matplotlib import pyplot as plt
import numpy as np
import scipy.stats
def __test2__():
    tdata, i, _ = np.loadtxt(r'C:\Users\v-liderman\Desktop\out.json', delimiter='\t', dtype=object, \
                      converters={0: lambda x: get_typed_value(x, datetime),\
                                  1: lambda x: get_typed_value(x, float)}, unpack=True)
    #print(data[:100])
    fr = 1
    tdata1 = tdata[::fr]
    i1 = i[::fr]
    #вычиялем разницу
    max_i = tdata1.shape[0]
    new_data = np.zeros(max_i - 1)
    for idx in range(max_i):
        if idx < max_i - 1:
            new_data[idx] = (tdata1[idx+1] - tdata1[idx]).microseconds
    
    i1 = (i1/fr)[:-1]
    new_data = new_data/1000000
    
    plt.figure(1)
    plt.plot(i1, new_data)
    plt.figure(2)
    plt.hist(new_data)
    plt.show()
    print(scipy.stats.describe(new_data))

#__test2__()
    
