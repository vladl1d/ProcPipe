# -*- coding: utf-8 -*-
"""
@author: V-Liderman

Представление json в виде структуры данных.
"""
import re
import sys
#from ..core.types import t_dict
from ..core.types import t_list

class JsonTree():
    t_dict = dict
    t_list = t_list
    '''
    JSON в структурированном виде. Дерево для синтаксического разбора
    '''
    def __init__(self, value=None, key=None, parent=None):
        self.parent = parent
        self.key = key
        self.value = value
        self.type = type(value)
        # Переменные для последующего разбора данных
        # Контекст узла (Интервалы).
        self.context = None

#    @property
#    def type(self):
#        '''Получение типа текущего элемента'''
#        return type(self.value)
    @property
    def root(self):
        '''Получение ссылки на вершину дерева json'''
        if self.parent:
            return self.parent.root
        return self
    @property
    def path(self):
        '''Возвращает путь к вершине
        '''
        this = [self.key] if self.type == JsonTree.t_dict else None
        if self.parent:
            top = self.parent.path
            if top:
                return top + this if this else top

        return this
    def __iter__(self):
        if self.type in (JsonTree.t_list, JsonTree.t_dict):
            return iter(self.value)
        raise TypeError('Тип значения не поддерживает операцию')

    def __next__(self):
        pass
    def get(self, index, default):
        '''обертка вокруг dict'''
        if self.type == JsonTree.t_dict:
            if isinstance(index, str):
                index = sys.intern(index.lower())
            return self.value.get(index, default)
        if self.type == JsonTree.t_list:
            return default if index >= len(self.value) else self.value
        if index == 0:
            return self.value if self.value else default

        raise TypeError('Тип значения не поддерживает операцию')
    def __getitem__(self, index):
        if self.type in (JsonTree.t_list, JsonTree.t_dict):
            if isinstance(index, str):
                index = sys.intern(index)
            return self.value[index]
        if index == 0:
            return self.value

        raise TypeError('Тип значения не поддерживает операцию')
    def __setitem__(self, index, value):
        if self.type in (JsonTree.t_list, JsonTree.t_dict):
            if isinstance(index, str):
                index = sys.intern(index.lower())
            self.value[index] = value
            return
        if index == 0:
            self.value = value
            return

        raise TypeError('Тип значения не поддерживает операцию')
    def __contains__(self, item):
        if self.type in (JsonTree.t_list, JsonTree.t_dict):
            return item in self.value

        raise TypeError('Тип значения не поддерживает операцию')
    def keys(self):
        '''
        Обертка вокруг dict
        '''
        if self.type == JsonTree.t_dict:
            return self.value.keys()
        if self.type == JsonTree.t_list:
            return range(len(self.value))

        raise TypeError('Тип значения не поддерживает операцию')
    def __len__(self):
        if self.type in (JsonTree.t_list, JsonTree.t_dict):
            return len(self.value)

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
            if isinstance(obj, JsonTree.t_list):
                for i, value in enumerate(obj):
                    obj[i] = _object_value(value)
            if isinstance(obj, JsonTree.t_dict):
                for key in obj.keys():
                    obj[key] = _object_value(obj[key])

            return obj

        return _object_value(self)
    _re_root = re.compile(r'^\.')
    _re_element = re.compile(r'^(/([\w|*]+))|(\[(\d+)\])')

    def query(self, path, default=None, this=None):
        '''возвращает элемент по заданному пути
        <query> = <query_root><selector>
        <query_root> = .
        <selector> = /<element_name><selector> | [<index>]<selector> | $
        <selector> = string literal
        <index> = int
        '''
        def _get(this, index, default):
            try:
                #любой первый индекс
                if index == '*':
                    index = next(iter(this))
                val = this.get(index, None)
                if val is None:
                    return default
                return val
            except:
                return default

        assert isinstance(path, str)
        #мы на конце пути
        if this is None:
            #мы наверху
            if not path:
                return self
            path = path.lower()
            match = re.match(JsonTree._re_root, path)
            assert match, 'Неверный формат пути'
            return self.query(path[match.span()[1]:], default, self)
        else:
            if not path:
                return this
            if not isinstance(this, (JsonTree, JsonTree.t_dict, JsonTree.t_list)):
                return default
            match = re.match(JsonTree._re_element, path)
            assert match, 'Неверный формат пути'
            if match:
                key = match.group(2) if match.group(2) else int(match.group(4))
                return self.query(path[match.span()[1]:], default, _get(this, key, default))
