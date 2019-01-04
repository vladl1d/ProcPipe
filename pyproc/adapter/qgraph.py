# -*- coding: utf-8 -*-
"""
Дерево запроса и хелпер который его обрабатывает. Выполняется только под Python 3.5+
Created on Fri Dec 21 08:52:08 2018
@author: V-Liderman
"""
#from __future__ import unicode_literals
#from __future__ import print_function as _print
import re
import sys
from ..core.types import t_dict

class QGraph():
    '''
    Класс для обработки сущностей
    '''
    _STANDART_TAGS = {'name', 'cols', 'filter', 'sort', 'ref', 'ref_filter', 'PK', 'type', \
                      'hints', 'param_values', 'sql', 'indexed'}
    _DEFAULT_SCHEMA = 'dbo'
    _COL_TEMPLATE = re.compile(r'([^\.]+)[.]([^\.]+)')

    def __init__(self, name='?', Type=None, cols=None, parent=None):
        '''
        Стандарный конструктор
        '''
        self._schema, self._name = None, None
        self.name = name
        self.cols = cols if cols else []
        self.parent = parent
        self.type = Type if Type else 'T'
        self.param_values = None
        self.sql = None

        if parent:
            self.ref = {'type': 'left', 'auto': True}
            if name != '?':
                parent.subnodes[name] = self
        else:
            self.ref = None
        #Значения по-умолчанию
        #Массив первичных ключей.
        self.PK = None
        #Колонки на которые ссылаются из вне
        self.refed_cols = set()
        #фильтр
        self.filter = None
#        self.ref_filter = None
        #Колонки сортировки
        self.sort = None
        #массив связанных сущностей
        self.joints = []
        #Коллекция зависимых сущностей (Название -> сущность)
        self.subnodes = t_dict()
        self.hints = []
        # Включить в join F_Division
        self.is_division_table = False
        #индексировать значения сущности
        self.indexed = False
        #Сущность отвалидирована
        self.validated = False

    @property
    def full_name(self):
        '''Возвращает имя вместе  со схемой'''
        return self._schema + '.' + self._name
    @full_name.setter
    def full_name(self, value):
        '''Устанавливает имя вместе  со схемой'''
        self._schema, self._name = value
    @property
    def name(self):
        '''Возвращает имя вместе  со схемой, если она отличается от схемы по-умолчанию'''
        return self._name if self._schema == QGraph._DEFAULT_SCHEMA else self.full_name
    @name.setter
    def name(self, value):
        '''Устанавливает имя вместе  со схемой'''
        #отделяем схему от имени
        if '.' in set(value):
            match = re.search(QGraph._COL_TEMPLATE, value)
            assert match, value + ': Неправильное имя объекта'
            self._schema = match.group(1)
            self._name = match.group(2)
        else:
            self._name = value
            self._schema = QGraph._DEFAULT_SCHEMA

    @property
    def path(self):
        '''
        Возвращает путь к вершине
        '''
        if self.parent:
            return self.parent.path + '/' + self.name
        else:
            return self.name
    @staticmethod
    def parse_conf_node(node, name=None, parent=None):
        '''
        Альтернативный конструктор для создания таблицы из конфигурации
        '''
        #Основной код
        assert  isinstance(node, dict), \
                name if name else '?'+': Неверный тип данных узла конфигурации'

        keys = node.keys()
        #Разбор имени объекта
        if not name:
            assert 'name' in keys, \
                    name if name else '?'+': Не задано имя объекта'
            name = node['name']

        #Разбор типа
        Type = node['type'] if 'type' in keys else 'T'
        this = QGraph(name, Type=Type, parent=parent)

        #параметры для функций
        if 'param_values' in keys:
            this.param_values = node['param_values']
        #альтернативное название таблицы
        if 'sql' in keys:
            this.sql = node['sql']
        #опция - индексировать массивы
        if 'indexed' in keys:
            this.indexed = True

        #Разбор колонок объекта
        assert 'cols' in keys or Type in 'P', \
                name if name else '?'+': Не заданы колонки объекта'
        if 'cols' in keys:
            assert isinstance(node['cols'], list), \
                    name if name else '?'+': Не верный тип колонок объекта'
            for col in node['cols']:
                col = QGraphHelper.parse_col_node(this, col, name)
                if col:
                    this.cols.append(col)
        else:
            assert Type in 'P', name if name else '?'+': Не заданы колонки объекта'

        #Разбор фильтра
        if 'filter' in keys:
            this.filter = node['filter']

        #Разбор сортировки
        if 'sort' in keys:
            this.sort = QGraphHelper.parse_sort_node(node['sort'], name)

        #Разбор хинтами
        if 'hints' in keys:
            this.hints = QGraphHelper.parse_hints_node(node['hints'], name)

        #Разбор PK
        if 'PK' in keys:
            this.PK = node['PK'] if isinstance(node['PK'], list) else [node['PK']]

        #Разбор ссылки на родитель
        if 'ref' in keys:
            this.ref = QGraphHelper.parse_ref_node(node['ref'], name)
        #дополнительный фильтр в join
        if 'ref_filter' in keys:
            this.ref['filter'] = node['ref_filter']
        #Разбор связей
        this.joints = []
        for subnode in (keys - QGraph._STANDART_TAGS):
            assert  isinstance(node, dict), \
                    name+': Неверно задана связанная таблица %s' % subnode
            this.joints.append(QGraph.parse_conf_node(node[subnode], name=subnode, parent=this))

        return this

    def validate(self, conn):
        '''
        Расстановка значений по умолчанию из БД
        '''
        #Основной код
        assert conn, self.name + ': Нет соединения к БД'
        crs = conn.cursor()
        #проверка существования объекта в БД
        if self.name != '?':
            QGraphHelper.check_table_existance(self._name, self._schema, self.type, crs)
        # проверка имен колонок
        if self.cols:
            col_names = [col[0] if isinstance(col, tuple) else col for col in self.cols]
            QGraphHelper.check_columns_existance(self._name, self._schema, col_names, crs)

            # разрешение связей в зоне cols
            for col in self.cols:
                if isinstance(col, tuple):
                    QGraphHelper.assign_ref_table_name(self._name, self._schema, col[0], col[1], \
                                                        crs)
                    # у ссылочной сущности тоже могут быть связи
                    col[1].validate(conn)

        # разрешение PK
        if not self.PK and self.type in 'TVF':
            self.PK = []
            self.is_division_table = QGraphHelper.assign_table_PK(self._name, self._schema, \
                                                                   self.PK, crs)

        #разерешение связи с родителем
        if self.parent:
            if not ('left' in self.ref and 'right' in self.ref):
                QGraphHelper.assign_ref_to_parent(self._name, self._schema, \
                                                   self.parent._name, self.parent._schema, \
                                                   self.ref, crs)
            self.parent.refed_cols |= set(self.ref['left'])

        #Запускаем валидацию для всех связей
        for ent in self.joints:
            ent.validate(conn)

        self.validated = True

    _re_root = re.compile(r'^\$')
    _re_element = re.compile(r'^(\.([\w|*]+))|(\[(\d+)\])')
    
    def query(self, path, default=None, this=None):
        '''возвращает элемент по заданному пути
        <query> = <query_root><selector>
        <query_root> = \$
        <selector> = .<element_name><selector> | [<index>]<selector> | $
        <selector> = string literal
        <index> = int
        '''
        def _get(this, index, default):
            try:
                #любой первый индекс
                if index == '*':
                    index = this.subnodes.keys[0]
                if index == this.name:
                    return this
                else:
                    return this.subnodes.get(index, default)
            except:
                return default

        assert isinstance(path, str)
        
        # основной вызов
        if this is None:
            #мы наверху
            if not path:
                return self
            match = re.match(QGraph._re_root, path)
            assert match, 'Неверный формат пути'
            entity = self.query(path[match.span()[1]:], default, self)
            return entity
        else:
            if not path:
                return this
            if not isinstance(this, QGraph):
                return default
            match = re.match(QGraph._re_element, path)
            assert match, 'Неверный формат пути'
            if match:
                key = match.group(2) if match.group(2) else int(match.group(4))
                return self.query(path[match.span()[1]:], default, _get(this, key, default))

    def traverse_query_paths(self):
        '''Формирование кеша'''
        schema_map = dict()

        def _traverse_query_paths(enitity, path=None, name=None):
            if not path:
                path = '$'
    
            name = name.lower() if name else enitity.name.lower()
            path += '.%s' % name
            schema_map[path] = enitity
            for ent in enitity.subnodes:
                _traverse_query_paths(enitity.subnodes[ent], path=path, name=ent)

        _traverse_query_paths(self)
        return schema_map
##################################################################################################
class QGraphHelper:
    '''helper-класс для разбора и валидации сущностей. Сделан в целях тестирования'''
    @staticmethod
    def parse_sort_node(node, name):
        '''Разбор списка сортировки
        '''
        assert  isinstance(node, list), \
                name if name else '?'+'Неверный тип атрибута с сортировкой'
        return node

    @staticmethod
    def parse_ref_node(node, name):
        '''Разбор связи с родительской сущностью
        '''
        assert  isinstance(node, dict), \
                name if name else '?'+'Неверный тип атрибута-связи'
        if 'type' not in node:
            node['type'] = 'left'
        if 'hint' not in node:
            node['hint'] = []
        elif not isinstance(node['hint'], list):
            node['hint'] = [node['hint']]
        node['auto'] = True
        if 'right' in node and 'left' in node:
            if isinstance(node['right'], str):
                node['right'] = [node['right']]
            if isinstance(node['left'], str):
                node['left'] = [node['left']]
            node['auto'] = False

            assert len(node['right']) == len(node['left']), name+': Ошибка в определении связи'

        return node

    @staticmethod
    def parse_col_node(parent, node, name=None):
        '''Разбор имени колонки
        '''
        assert  isinstance(parent, QGraph), \
                name+': Неверные параметры функции '
        assert  isinstance(node, (str, dict)), \
                name+': Неверный тип колонки '+str(node)

        if isinstance(node, str):
            return node
        if isinstance(node, dict):
            for key in node.keys():
                assert  isinstance(node[key], (str, list, dict)), \
                        name+': Неверный тип колонки %s' % key

                if isinstance(node[key], str):
                    # это алиас
                    return (key, node[key])
                if isinstance(node[key], list):
                    #список колонок из вышестоящей сущности
                    ref = QGraph(cols=node[key], parent=parent)
                    ref.ref = {'left':key, 'type': 'left', 'auto': True}
                    return (key, ref)
                if isinstance(node[key], dict):
                    name = '?' if not 'name' in node[key].keys() else None
                    return (key, QGraph.parse_conf_node(node[key], name=name, parent=parent))

        return None

    @staticmethod
    def parse_hints_node(node, name):
        '''Разбор хинтов к запросу
        '''
        assert  isinstance(node, list), \
                name if name else '?'+'Неверный тип атрибута с хинтами'
        return node

    @staticmethod
    def check_table_existance(name, schema, _type, crs):
        '''Проверяет наличие объекта из конфигурации в схеме БД
        '''
        if _type in 'TV':
            table_type = 'TABLE' if _type == 'T' else 'VIEW'
            crs.tables(table=name, schema=schema, tableType=table_type)
        elif _type in 'PF':
            crs.procedures(procedure=name, schema=schema)
        else:
            raise AssertionError(name + ': Неизвестный тип объекта')


        assert crs.fetchone(), name + ': Не смогли разрешить имя объекта'

    @staticmethod
    def check_columns_existance(name, schema, col_names, crs):
        '''Проверяет наличие колонок из конфигурации в схеме БД
        '''
        cols = crs.columns(table=sys.intern(name), schema=schema).fetchall()
        if not cols:
            return
        cols = list(filter(lambda x: x.lower() in set(map(str.lower, col_names)), \
                      [r.column_name for r in cols]))
        delta = set(col_names) - set(cols)
        assert len(delta) == 0, name + ': Не смогли разрешить имя колонок %s' % ','.join(delta)

    @staticmethod
    def assign_ref_table_name(name, schema, col_name, ref_entity, crs):
        '''Разрешает имя таблицы для FK и название ее первичного ключа
        '''
        ref_entity.ref['left'] = []
        ref_entity.ref['right'] = []

        res = crs.foreignKeys(foreignTable=name, foreignSchema=schema)
        res = filter(lambda x: x.fkcolumn_name == col_name, res)

        for r in res:
            ref_entity.full_name = r.pktable_schem, r.pktable_name
            ref_entity.parent.subnodes[ref_entity.name] = ref_entity
            ref_entity.ref['right'].append(r.pkcolumn_name)
            ref_entity.ref['left'].append(r.fkcolumn_name)

        assert ref_entity.ref['right'], name + ': Не смогли разрешить FK %s' % col_name

    @staticmethod
    def assign_ref_to_parent(name, schema, p_name, p_schema, ref, crs):
        '''
        Разрешает параметры связи с родительской таблицей
        '''
        r_col = ref.right[0] if 'right' in ref else None
        ref['left'] = []
        ref['right'] = []

        res = crs.foreignKeys(table=p_name, schema=p_schema, \
                              foreignTable=name, foreignSchema=schema)
        if r_col:
            res = filter(lambda x: x.fkcolumn_name == r_col, res)
        for row in res:
            ref['right'].append(row.fkcolumn_name)
            ref['left'].append(row.pkcolumn_name)

        assert ref['right'], name + ': Не смогли разрешить связь с %s' % p_name

    @staticmethod
    def assign_table_PK(name, schema, PK, crs):
        '''
        Разрешает имя таблицы для FK и название ее первичного ключа
        '''
        PK += [row.column_name for row in crs.primaryKeys(table=name, schema=schema)]
        assert PK, name + ': Не смогли разрешить PK'
        #Эвристика - пытаемся проверить тип PK
        cols = filter(lambda x: x.column_name in set(PK) | {'F_Division'}, \
            crs.columns(table=name, schema=schema))
        is_division, is_guid = False, False
        for col in cols:
            if col.column_name.lower() == 'f_division': is_division = True
            if col.type_name == 'uniqueidentifier': is_guid = True

        return is_division and not is_guid
