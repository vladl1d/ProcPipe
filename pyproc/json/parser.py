# -*- coding: utf-8 -*-
"""
Обертка для потокового бинарного чтения из любого итератора и Json-парсер.
Нахождение библиотеки необходимо прописать в переменных: INCLUDE, LIB, LD_LIBRARY_PATH
Created on Mon Oct 29 15:52:11 2018
@author: V-Liderman
"""
#import sys
import encodings
#from collections import deque
#from ..core.types import t_dict, t_list
from .tree import JsonTree

from yajl import (
    YajlContentHandler, YajlParser,
    YajlParseCancelled, YajlError
)

class JsonParser(YajlContentHandler):
    '''
    Класс для сборки JSON-объекта из строки и вызова обработчика по подписке
    '''
    def __init__(self, callbacks=None, encoding='utf-8'):
        '''Инициализация объекта для сборки JSON'''
        if callbacks:
            assert isinstance(callbacks, dict), 'Неверный тип параметра'
        self.encoding = encoding
        #корень дерева
        self.root = None
        #текуший узел, по которому идет парсинг
        self.current = None
        self.path = None
        #обработчки по подписке
        self.callbacks = callbacks
        self.kwargs = None
        self.parser = None
        self.error = None
        #словарь для преобразования кратких имен в полные
        self.alias_map = None
        #переменные которые будут переданы в callback
        self.kwargs = None
        #словарь с деревом запроса для определения PK
        self.schema = None
        # кеш схемф (путь->сущность)
        self.schema_map = dict()
        self.encode = encodings.search_function(encoding).encode
        self.decode = encodings.search_function(encoding).decode
        
        

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
                if self.root is None:
                    self.root = value
                return
            else:
                self.current = JsonTree()
                if self.root is None:
                    self.root = self.current
        if self.current.type == JsonTree.t_dict:
            if self.current.key is None:
                #Если не задан ключ - объединяем структуры. Сделано для объединения
                #последовательности json в 1 большой
                if isinstance(value, JsonTree) and value.type == JsonTree.t_dict:
                    self.current().update(value())
                    return
                if isinstance(value, JsonTree.t_dict):
                    self.current().update(value)
                    return
                else:
                    raise ValueError('Ошибка интерпретации структуры. На задан ключ для значения')

            self.current[self.current.key] = value
            self.current.key = None
        elif self.current.type == JsonTree.t_list:
            self.current().append(value)
        else:
            self.current.value = value

    def _callback_trigger(self, path, node):
        '''Вызов событий по подписке'''
        if self.callbacks:
            for trigger in self.callbacks:
#                if isinstance(trigger, (list, tuple)):
#                    trigger = '/'.join(trigger)
#                else:
#                    trigger = str(trigger)
#                if isinstance(path, list):
#                    path = '/'.join(path)

                _cmp = lambda x, y: x == y if y[:2] == './' else x.endswith(y)
                if _cmp(path.lower(), trigger.lower()):
                    #Верни True - если успешно обработал узел и мы его грохнем из обработки
                    #Иначе верни False
                    return self.callbacks[trigger](node, self, **self.kwargs)

        return False

    #события от синтаксического генератора
    def parse_start(self):
        '''Обработка события начала разбора'''
#        self.root = None #JsonTree()
#        self.current = None #self.root
        self.path = '.'
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
            self.yajl_integer(ctx, self.decode(stringVal)[0])
        else:
            self.yajl_double(ctx, self.decode(stringVal)[0])
    def yajl_string(self, ctx, stringVal):
        '''Обработка события обработки значения'''
        self._set_value(self.decode(stringVal)[0])
    def yajl_start_map(self, ctx):
        '''Обработка события начала структуры'''
        # обработка пути
        if self.current is not None and self.current.key:
            self.path += '/' + self.current.key

        self._push(JsonTree.t_dict())
    def _map_2key(self, key):
        if self.alias_map:
            val = self.alias_map.get(key, key)
            val = val.name.replace('.', '_') if hasattr(val, 'name') else str(val)
        else:
            val = key
        return val.lower()
    def yajl_map_key(self, ctx, stringVal):
        '''Обработка события ключ структуры'''
        self.current.key = self._map_2key(self.decode(stringVal)[0])
    def yajl_end_map(self, ctx):
        '''Обработка события окончания структуры'''
        value = self._pop()
        #восстановим у структуры key. В current после pop - родитель
        if self.current is not None:
            value.key = self.current.key

        #обработка пути
        path = self.path
        if self.path:
            pos = self.path.rfind('/')
            self.path = self.path[:pos] if pos>0 else '.'
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
        #Подставляем здесь ключ (PK) для индексирования
        pk = None
        if self.schema and key:
            path = (self.path + '/' + key).lower() if self.path else ''
            entity = self.schema_map.get(path, None)
            if not entity:
                self.schema_map[path] = entity = self.schema.query(path, None)
            pk = entity.PK[0] if getattr(entity, 'indexed', False) else None

        self._push(JsonTree.t_list(pk=pk), key=key)
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
            if parent.type == JsonTree.t_list:
                parent = JsonTree(JsonTree.t_list(parent().pk), parent.key, parent.parent)
                parent().append(current)
            if parent.type == JsonTree.t_dict and parent.key:
                parent[parent.key] = current
            current = parent
            parent = current.parent
        return current

    def _traverse_query_paths(self, enitity, path=None, name=None):
        '''Формирование кеша'''
        if not path:
            path = '.'

        name = name.lower() if name else enitity.name.lower()
        path += '/%s' % name
        self.schema_map[path] = enitity
        for ent in enitity.subnodes:
            self._traverse_query_paths(enitity.subnodes[ent], path=path, name=ent)


    def parse(self, input_stream, alias_map=None, schema=None, append_to=None, buf_size=65536, \
              **kwargs):
        '''Потоковый разбор JSON'''
        assert input_stream, 'Не задан источник данных'
        #переменные которые будут переданы в callback
        self.kwargs = kwargs
        #словарь с деревом запроса для определения PK
        if schema != self.schema:
            self.schema = schema
            self.schema_map.clear()
            self._traverse_query_paths(schema)
        # map(краткое имя -> полное имя)
        assert alias_map is None or isinstance(alias_map, dict), 'Неверный тип справочника aliasов'
        self.alias_map = alias_map
        #наш герой - C Stream JSon Parser
        self.parser = YajlParser(self, buf_size)
        self.parser.allow_multiple_values = True
        self.parser.dont_validate_strings = True
        #инициализуем для повторного использования
        self.root = append_to if append_to is not None else None
        self.current = append_to if append_to is not None else None
        #парсим
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
