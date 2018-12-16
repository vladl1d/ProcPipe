# -*- coding: utf-8 -*-
'''
Содержит классы с запросом и инфраструктуру доступа к данным. Выполняется только под Python 3.5+
Created on Sun Oct 28 06:25:49 2018
@author: V-Liderman
'''
#from __future__ import unicode_literals
#from __future__ import print_function as _print
import sys
import os
sys.path.append(os.path.dirname(__file__))
from abc import ABC, abstractmethod
from pprint import pprint
from Query import Node
from JsonStream import JsonStream, JsonParser, JsonTree
from Cache import CacheHelper
import re

#import collections as c
import pyodbc, psycopg2

##################################################################################################
class QGrapthHelper:
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
        assert  isinstance(parent, QGrapth), \
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
                    ref = QGrapth(cols=node[key], parent=parent)
                    ref.ref = {'left':key, 'type': 'left', 'auto': True}
                    return (key, ref)
                if isinstance(node[key], dict):
                    name = '?' if not 'name' in node[key].keys() else None
                    return (key, QGrapth.parse_conf_node(node[key], name=name, parent=parent))

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
        cols = list(filter(lambda x: x in set(col_names), \
                      [r.column_name for r in crs.columns(table=name, schema=schema)]))
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
        for c in cols:
            if c.column_name.lower() == 'f_division': is_division = True
            if c.type_name == 'uniqueidentifier': is_guid = True

        return is_division and not is_guid

class QGrapth(object):
    '''
    Класс для обработки сущностей
    '''
    _STANDART_TAGS = {'name', 'cols', 'filter', 'sort', 'ref', 'ref_filter', 'PK', 'type', \
                      'hints', 'param_values', 'sql'}
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
        else:
            self.ref = None
        #Значения по-умолчанию
        self.PK = None
        self.refed_cols = set()
        self.filter = None
        self.ref_filter = None
        self.sort = None
        self.joints = []
        self.hints = []
        self.is_division_table = False
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
        return self._name if self._schema == QGrapth._DEFAULT_SCHEMA else self.full_name
    @name.setter
    def name(self, value):
        '''Устанавливает имя вместе  со схемой'''
        #отделяем схему от имени
        if '.' in set(value):
            m = re.search(QGrapth._COL_TEMPLATE, value)
            assert m, value + ': Неправильное имя объекта'
            self._schema = m.group(1)
            self._name = m.group(2)
        else:
            self._name = value
            self._schema = QGrapth._DEFAULT_SCHEMA

    @property
    def path(self):
        '''
        Возвращает путь к вершине
        '''
        if self.parent:
            return self.parent.path + '.' + self.name
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
        this = QGrapth(name, Type=Type, parent=parent)

        #параметры для функций
        if 'param_values' in keys:
            this.param_values = node['param_values']
        #альтернативное название таблицы
        if 'sql' in keys:
            this.sql = node['sql']

        #Разбор колонок объекта
        assert 'cols' in keys or Type in 'P', \
                name if name else '?'+': Не заданы колонки объекта'
        if 'cols' in keys:
            assert isinstance(node['cols'], list), \
                    name if name else '?'+': Не верный тип колонок объекта'
            for col in node['cols']:
                col = QGrapthHelper.parse_col_node(this, col, name)
                if col:
                    this.cols.append(col)
        else:
            assert Type in 'P', name if name else '?'+': Не заданы колонки объекта'

        #Разбор фильтра
        if 'filter' in keys:
            this.filter = node['filter']

        #Разбор сортировки
        if 'sort' in keys:
            this.sort = QGrapthHelper.parse_sort_node(node['sort'], name)

        #Разбор хинтами
        if 'hints' in keys:
            this.hints = QGrapthHelper.parse_hints_node(node['hints'], name)

        #Разбор PK
        if 'PK' in keys:
            this.PK = node['PK'] if isinstance(node['PK'], list) else [node['PK']]

        #Разбор ссылки на родитель
        if 'ref' in keys:
            this.ref = QGrapthHelper.parse_ref_node(node['ref'], name)
        #дополнительный фильтр в join
        if 'ref_filter' in keys:
            this.ref['filter'] = node['ref_filter']
        #Разбор связей
        this.joints = []
        for subnode in (keys - QGrapth._STANDART_TAGS):
            assert  isinstance(node, dict), \
                    name+': Неверно задана связанная таблица %s' % subnode
            this.joints.append(QGrapth.parse_conf_node(node[subnode], name=subnode, parent=this))

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
            QGrapthHelper.check_table_existance(self._name, self._schema, self.type, crs)
        # проверка имен колонок
        if self.cols:
            col_names = [col[0] if isinstance(col, tuple) else col for col in self.cols]
            QGrapthHelper.check_columns_existance(self._name, self._schema, col_names, crs)

            # разрешение связей в зоне cols
            for col in self.cols:
                if isinstance(col, tuple):
                    QGrapthHelper.assign_ref_table_name(self._name, self._schema, col[0], col[1], \
                                                        crs)
                    # у ссылочной сущности тоже могут быть связи
                    col[1].validate(conn)

        # разрешение PK
        if not self.PK and self.type in 'TVF':
            self.PK = []
            self.is_division_table = QGrapthHelper.assign_table_PK(self._name, self._schema, \
                                                                   self.PK, crs)

        #разерешение связи с родителем
        if self.parent:
            if not ('left' in self.ref and 'right' in self.ref):
                QGrapthHelper.assign_ref_to_parent(self._name, self._schema, \
                                                   self.parent._name, self.parent._schema, \
                                                   self.ref, crs)
            self.parent.refed_cols |= set(self.ref['left'])

        #Запускаем валидацию для всех связей
        for ent in self.joints:
            ent.validate(conn)

        self.validated = True

#################################################################################################
class IQueryBuilder(ABC):
    '''Абстрактный класс с методами для построения запроса. Dependency Enjection pattern
    '''
    def __init__(self, sql_lang=None, shrink_names=False):
        self.alias_map = JsonTree.t_dict()
        self.query_cache = dict()
        self.shrink_names = shrink_names
        self.sql_lang = sql_lang

    @staticmethod
    def get_short_name(name):
        '''Возвращает краткое имя сущности (алиас)'''
        assert name and isinstance(name, str), 'Ошибка формирования краткого имени таблицы'
        return (''.join([s[0] for s in name.replace('.', '_').split('_')])).upper()

    def get_alias(self, entity):
        '''Запоминает и находит для сущности алиас'''
        t_ref = self.alias_map.get(entity, None)
        if t_ref and isinstance(t_ref, str):
            return t_ref

        #t_ref = entity.name
        t_ref = self.get_short_name(entity.name if hasattr(entity, 'name') else entity)
        i = 1
        t_ref0 = t_ref
        while t_ref in self.alias_map:
            t_ref = '%s%.2i' % (t_ref0, i)
            i += 1
        #t_ref = 'T%.2i' % (len(self.alias_map.keys()) + 1)
        self.alias_map[entity] = t_ref
        self.alias_map[t_ref] = entity
        return t_ref

    def path(self, entity):
        '''
        Возвращает путь к вершине
        '''
        entity_name = self.get_alias(entity) if self.shrink_names else entity.name.replace('.', '_')
        if entity.parent:
            return '%s.%s' % (self.path(entity.parent), entity_name)
        else:
            return '%s' % entity_name

    def get_sql_col_name(self, t_ref, col_name, alias=None, path=None):
        '''Получает SQL-выражение для столбца таблицы'''
        if alias or path:
            alias = ((path+'.') if path else '') + (alias if alias else col_name)
            return '%s.%s AS "%s"' % (t_ref, col_name, alias)
        else:
            return '%s.%s' % (t_ref, col_name)

    def get_sql_table_name(self, t_ref, entity):
        '''Получает SQL-выражение для таблицы'''
        if entity.type in 'TV':
            full_name = entity.sql if entity.sql else entity.full_name
            return '%s AS %s' % (full_name, t_ref)
        elif entity.type == 'F':
            params = ', '.join(entity.param_values) if entity.param_values else ''
            return '%s(%s) AS %s' % (entity.full_name, params, t_ref)

    def get_sql_join_criteria(self, p_ref, PK_col, t_ref, FK_col):
        '''Возвращает SQL-условие для join on'''
        return '%s.%s = %s.%s' % (p_ref.replace('.', '_'), PK_col, t_ref.replace('.', '_'), FK_col)

    def get_sql_join(self, _type, entity_name, join_on):
        '''Возвращает JOIN-выражение к основной таблице'''
        return '%s JOIN %s ON %s' % (_type, entity_name, ' AND '.join(join_on))

    @abstractmethod
    def get_sql(self, t_ref, entity_name, query, skip_wrap=False, level=0):
        '''Возвращает SQL-запрос для одной сущности'''
        raise NotImplementedError('IQueryBuilder.get_sql')

    def build_select(self, entity, t_ref, path, query):
        '''Строит массив колонок для select'''
        _select, _from, _where, _sort, _ref_entities = query
        cols = entity.cols
        #добавляем PK
        _keys = set(entity.PK) | set(entity.refed_cols)
        if self.sql_lang == 'PGSQL' and entity.ref and entity.ref.get('right', None):
            _keys |= set(entity.ref['right'])
            if entity.is_division_table and entity.parent.is_division_table and entity.ref['auto']:
                _keys |= {'F_Division'}
        cols += [col for col in _keys - set(cols)]

        for col in entity.cols:
            if not isinstance(col, tuple):
                col_ref = self.get_alias(col) if self.shrink_names else None
                _select.append(self.get_sql_col_name(t_ref, col, col_ref, path=path))
            elif isinstance(col[1], str):
                _select.append(self.get_sql_col_name(t_ref, col[0], col[1], path=path))
            else:
                _ref_entities.append(col[1])

    def build_join_conditions(self, entity, t_ref, p_ref=None):
        '''Возвращает условие join_on'''
        assert isinstance(entity.ref, dict) and \
                   'left' in entity.ref and 'right' in entity.ref, \
                   entity.name + ': Ошибка формирования связи'

        join_on = []
        if not p_ref:
            p_ref = self.get_alias(entity.parent)
        for i in range(len(entity.ref['left'])):
            left = entity.ref['left'][i]
            right = entity.ref['right'][i]
            join_on.append(self.get_sql_join_criteria(p_ref, left, t_ref, right))

        if entity.is_division_table and entity.parent.is_division_table and entity.ref['auto']:
            join_on.append(self.get_sql_join_criteria(p_ref, 'F_Division', t_ref, 'F_Division'))

        return join_on

    __re_bool_cond = re.compile(r'([=]\s*)([01]\b)')
    def build_where(self, t_ref, _filter, p_ref=None):
        '''Возвращает условие where'''
        if p_ref:
            _filter = _filter.replace('@parent.', p_ref+'.')
        if self.sql_lang == 'PGSQL':
            _filter = _filter.replace('@.', t_ref+'.').replace('@', '_')
            #заменяем 1 и 0 на True, False
            return re.sub(self.__re_bool_cond, r'\1\2::bool', _filter)

        else:
            return _filter.replace('@.', t_ref+'.')

    @abstractmethod
    def build_from_where_sort(self, entity, t_ref, query):
        '''Строит части запроса where и sort'''
        raise NotImplementedError('IQueryBuilder.build_from_where_sort')

    @abstractmethod
    def build_table_sql(self, entity, level=0):
        '''Строит части sql-запроса для 1 сущности'''
        raise NotImplementedError('IQueryBuilder.build_table_sql')

    def build_sql(self, entity):
        '''Построение SQL запроса по дереву конфигурации'''
        if entity.type.upper() in 'TVF':
            _select, _from, _where, _sort = self.build_table_sql(entity)
            assert list(_select) and list(_from), 'Ошибка формирования запроса'
            skip_wrap = self.sql_lang != 'PGSQL'
            return self.get_sql(self.get_short_name(entity.name), entity.name, \
                            (_select, _from, _where, _sort), skip_wrap=skip_wrap)
        elif entity.type.upper() == 'P':
            return 'EXEC %s' % entity.full_name
#        elif entity.type.upper() == 'F':
#            return entity.full_name
        else:
            raise NotImplementedError('build_sql %s query type' % entity.type.upper())

    def get_db_options(self):
        '''Устанавливает правильные опции БД'''
        if self.sql_lang == 'PGSQL':
            return ''
        else:
            return '\nSET NOCOUNT ON\n'

    _re_has_select = re.compile(r'^\s*SELECT\b')

    def get_query_sql(self, query, params):
        '''Формирует финальный запрос с учетом параметров'''
        assert query.validated, '%s: Перед построением запроса необходимо валидировать' % query.name
        db_opt = self.get_db_options()
        sql = self.query_cache.get(query, None)
        if sql is None:
            sql = self.build_sql(query)
            self.query_cache[query] = sql
        if not self._re_has_select.match(sql):
            sql = '\nSELECT ' + sql
        if params:
            params = sorted(params, key=lambda x: x[0])
            constr = filter(lambda x: x[0] == '()', params)
            constr = '\n' + (';\n'.join([p[1] for p in constr]) if constr else '')

            if self.sql_lang == 'PGSQL':
                _pg_type = {'datetime': 'timestamp', 'smalldatetime': 'timestamp'}
                sql = db_opt + '\nDO $$' + \
                             '\nDECLARE\n\t'+ \
                             ';\n\t'.join(["%s %s default %%s::%s"  % \
                                           (p[0].replace('@', '_'), \
                                            _pg_type.get(p[1].lower(), p[1].upper()), \
                                            _pg_type.get(p[1].lower(), p[1].upper()) \
                                            ) \
                             for p in params if p[0] != '()']) + \
                            (';' if params else '') + \
                            '\nBEGIN'+ \
                            constr + \
                            '\n\tDROP TABLE IF EXISTS tmp_table;' + \
                            '\n\tCREATE TABLE tmp_table AS' + \
                            '\n\tSELECT to_json(%s)  as tag' % query.name + \
                            '\n\tFROM' + \
                            sql + ';' + \
                            '\nEND $$;'+ \
                            '\nselect * from tmp_table;'

            else:
                sql = db_opt + 'DECLARE\n\t' + \
                        ',\n\t'.join(["%s %s = ?"  % (p[0], p[1]) \
                                      for p in params if p[0] != '()']) + \
                        constr + \
                        sql + \
                        '\nOPTION(MAXDOP 1)'

        #print(sql)
        return sql

    def get_proc_sql(self, query, params_param_values):
        '''Формирует финальный запрос с учетом параметров'''
        assert query.validated, '%s: Перед построением запроса необходимо валидировать' % query.name
        sql = self.build_sql(query)
        if params_param_values:
            params_param_values = sorted(params_param_values, key=lambda x: x[0])

            if self.sql_lang == 'PGSQL':
                _pg_type = {'datetime': 'timestamp', 'smalldatetime': 'timestamp'}
                sql =   '\nDO $$\n' + \
                        'SELECT ' if query.type.upper() == 'F' else '' + \
                         sql + \
                         '(' if query.type.upper() == 'F' else '' + \
                         ', '.join(["%s = %%s::%s"  % \
                                       (p[0].replace('@', '_'), \
                                        _pg_type.get(p[1].lower(), p[1].upper()) \
                                        ) \
                         for p in params_param_values]) + \
                         ')' if query.type.upper() == 'F' else '' + \
                        (';' if params_param_values else '') + \
                        '\nEND $$;'
            else:
                sql += ' ' + ', '.join(["%s = ?"  % p[0] for p in params_param_values])

        #print(sql)
        return sql

##########################################################################################
class QueryBuilderNestedSelect(IQueryBuilder):
    '''Класс для построения запроса с подзапросами в select'''
    def build_from_where_sort(self, entity, t_ref, query):
        '''Строит части запроса where и sort'''
        _select, _from, _where, _sort, _ref_entities = query
        p_ref = None
        #получаем from и where
        t_name = self.get_sql_table_name(t_ref, entity)
        _from.append(t_name)

        if entity.parent:
            p_ref = self.get_alias(entity.parent)
            join_on = self.build_join_conditions(entity, t_ref)
            _where += join_on

        #добавляем where
        if entity.filter:
            _where.append('(%s)' % self.build_where(t_ref, entity.filter, p_ref))
        #получаем sort
        if entity.sort:
            _sort += ['%s.%s' % (t_ref, col) for col in entity.sort]

    def build_table_sql(self, entity, level=0):
        '''Строит части sql-запроса для 1 сущности'''
        _select = []
        _from = []
        _where = []
        _sort = []
        _ref_entities = []
        query = (_select, _from, _where, _sort, _ref_entities)

        #получаем индекс для таблицы
        t_ref = self.get_alias(entity)
        path = None #self.path(entity)

        #получаем select
        self.build_select(entity, t_ref, path, query)

        #получаем from и where
        self.build_from_where_sort(entity, t_ref, query)

        #Отработка связей
        _ref_entities += entity.joints
        for r_entity in _ref_entities:
            r_ref = self.get_alias(r_entity)
            r_query = self.build_table_sql(r_entity, level=level+1)

            #свертываем join
            sql = self.get_sql(r_ref, r_entity.name, r_query, level=level)
            _select.append(sql)

        #Итого
        return _select, _from, _where, _sort

    def get_sql(self, t_ref, entity_name, query, skip_wrap=False, level=0):
        '''Возвращает SQL-запрос для одной сущности'''
        def offset(size=1):
            return '\n'+'\t'*(level+size)

        _select, _from, _where, _sort = query
        o3 = 3 if self.sql_lang == 'PGSQL' else 2
        #offset = '\n'+'\t'*(level+1)
        sql = '%sSELECT %s' % (offset(o3), ', '.join(_select)) + \
              '%sFROM %s' % (offset(o3), offset(o3).join(_from))
        if _where:
            sql += '%sWHERE (%s)' % (offset(o3), ') AND ('.join(_where))
        if _sort:
            sql += '%sORDER BY %s' % (offset(o3), ', '.join(_sort))

        #по-умолчанию - мы MSSQL
        if   self.sql_lang == 'PGSQL':
            sql = ('{0}SELECT array_to_json(array_agg(row_to_json({2}))) as t '+\
                  'FROM ({1}{0}) {2}').format(offset(2), sql, t_ref)
        else:
            sql += '%sFOR JSON AUTO' % offset(o3)

        return  offset(1)+'('+sql+ \
               offset(1) +') AS %s' % (t_ref if self.shrink_names else entity_name.replace('.', '_'))
        #if level == 0 and not sql_lang:
        #    SQL = 'SELECT\n' + SQL
##################################################################################################
class QueryBuilderNestedFrom(IQueryBuilder):
    '''Класс для построения запроса с вложенными from и join'''
    def build_from_where_sort(self, entity, t_ref, query):
        '''Строит части запроса where и sort'''
        _select, _from, _where, _sort, _ref_entities = query
        #получаем from и where
        t_name = self.get_sql_table_name(t_ref, entity)
        _from.append(t_name)
        p_ref = self.get_alias(entity.parent) if entity.parent else None

        #добавляем where
        if entity.filter:
            _where.append('(%s)' % self.build_where(t_ref, entity.filter, p_ref))
        #получаем sort
        if entity.sort:
            _sort += ['"%s".%s' % (t_ref, col) for col in entity.sort]
    @staticmethod
    def _pg_json_row(entity_name):
        '''обертка для PG столбца с json вложенной сущности'''
        assert entity_name
        entity_name = entity_name.replace('.', '_')
        return 'to_json(%s) as %s' % (entity_name, entity_name)

    def build_table_sql(self, entity, level=0):
        '''Строит части sql-запроса для 1 сущности'''
        _select = []
        _from = []
        _where = []
        _sort = []
        _ref_entities = []
        query = (_select, _from, _where, _sort, _ref_entities)

        #получаем индекс для таблицы
        t_ref = self.get_alias(entity)
        path = self.path(entity) if self.sql_lang != 'PGSQL' else ''

        #получаем select
        self.build_select(entity, t_ref, path, query)

        #получаем from и where
        self.build_from_where_sort(entity, t_ref, query)

        #Отработка связей
        _ref_entities += entity.joints
        for r_entity in _ref_entities:
            r_ref = self.get_alias(r_entity)
            r_select, r_from, r_where, r_sort = self.build_table_sql(r_entity, level+1)
            #свертываем join
            _type = r_entity.ref['type'].upper()
            #обертка для PG-SQL
            if self.sql_lang == 'PGSQL':
                #свертываем join
                r_query = (r_select, r_from, r_where, r_sort)
                table_sql = self.get_sql(r_ref, r_entity.name, r_query, level)
                join_on = self.build_join_conditions(r_entity, r_entity.name, p_ref=t_ref)
                _from.append(self.get_sql_join(_type, table_sql, join_on))
                _select.append(self._pg_json_row(r_entity.name))
            else:
                join_on = self.build_join_conditions(r_entity, r_ref)
                if r_where:
                    join_on.append(r_where[0])
                _from.append(self.get_sql_join(_type, r_from[0], join_on))
                _from += r_from[1:]
                _select += r_select
                _sort += r_sort

        #Итого
        return _select, _from, _where, _sort

    def get_sql(self, t_ref, entity_name, query, level=0, skip_wrap=False):
        '''Возвращает SQL-запрос для одной сущности'''
        def offset(size=1):
            return '\n'+'\t'*(level+size)
        _select, _from, _where, _sort = query
        #offset = '\n'+'\t'*(level+1)
        sql = '%sSELECT %s' % (offset(2), ', '.join(_select)) + \
              '%sFROM %s' % (offset(2), offset(2).join(_from))
        if _where:
            sql += '%sWHERE (%s)' % (offset(2), ') AND ('.join(_where))
        if _sort:
            sql += '%sORDER BY %s' % (offset(2), ', '.join(_sort))

        #по-умолчанию - мы MSSQL
        if   self.sql_lang == 'PGSQL':
            pass
        else:
            sql += '%s FOR JSON PATH' % offset(2)

        if not skip_wrap:
            sql = '{0}({1}{0}) AS {2}'.format(offset(1), sql, entity_name.replace('.', '_'))
        return  sql
        #if level == 0 and not sql_lang:
        #    SQL = 'SELECT\n' + SQL
#################################################################################################
class DataAdapter:
    '''
    Класс для формирования запросов к БД
    '''
    _DATA_TAGS = {'id', 'params'}
    cache = CacheHelper(depends = {
                    'QGrapth[]' : ['dataadapter.py1', 'query.py']
                    })

    def __init__(self, dsn, config, module=pyodbc, \
                 query_builder=QueryBuilderNestedSelect(shrink_names=False, sql_lang=None)):
        '''
        Стандартный конструктор
        '''
        assert dsn, 'Не задана строка соединения'
        assert isinstance(query_builder, IQueryBuilder), 'Не задан построитель запроса'
        assert module and hasattr(module, 'connect'), 'Не передана библиотека работы с БД'

        self._dsn = dsn
        self._module = module
        self.query_builder = query_builder
        self.data = dict()
        self.connections = JsonTree.t_dict()
        self.__initialized = False

        self._parse_config(config)

    def _parse_config(self, config):
        '''
        Разбор файла с конфигурацией
        '''
        assert isinstance(config, dict), 'Не задана конфигурация адаптера'
        assert 'config' in config, 'Файл имеет не верную структуру'

        config = config['config']
        assert 'data' in config and isinstance(config['data'], list), 'Не верная конфигурация'

        for data in config['data']:
            assert isinstance(data, dict) and \
                   'id' in data and isinstance(data['id'], str) and \
                   'param' not in config or isinstance(data['params'], dict), 'Не верная конфигурация'
            queries = self.cache.pick_data('QGrapth[]', data['id'])
            if not queries:
                #разбор запросов
                self.new_db_job('QGrapth[]')
                queries = []
                for key in (data.keys() - DataAdapter._DATA_TAGS):
                    if key == 'query':
                        entity = QGrapth.parse_conf_node(data[key])
                        entity.validate(self.connections['QGrapth[]'])
                    else:
                        entity = QGrapth.parse_conf_node(data[key], name=key)
                        entity.validate(self.connections['QGrapth[]'])

                    queries.append(entity)

                self.commit_db_job('QGrapth[]')
                assert queries, 'Для адаптера не задано ни одного запроса'
                self.cache.dump_data(queries, 'QGrapth[]', data['id'])

            self.data[data['id']] = {'params': data['params'], 'queries': queries}

        self.__initialized = True

    def new_db_job(self, job_id=None):
        '''Открывает соединение с БД для выполнения операций'''
        if not job_id:
            job_id = len(self.connections.keys()) + 1
        conn = self.connections.get(job_id, None)
        if not conn:
            conn = self._module.connect(self._dsn)
#            conn.autocommit = True
            self.connections[job_id] = conn
        else:
            #сбор предыдущей транзакции
            print('Зашли снова в:', job_id)
            conn.commit()
        print('Новый запрос:', job_id)
        return job_id
    def _finish_db_job(self, tran_func=None, job_id=None):
        '''Закрывает операцию. Если не задан id операции закрывает все'''
        def _close_conn(conn, tran_func):
            try:
                getattr(conn, tran_func, lambda: True)()
                getattr(conn, 'close', lambda: True)()
            except Exception as e:
                print('Не смогли закрыть:', str(e))

        if job_id:
            if job_id in self.connections:
                _close_conn(self.connections[job_id], tran_func)
                self.connections.pop(job_id)
                print('Закрыли запрос:', job_id)
        else:
            for conn in self.connections:
                _close_conn(conn, tran_func)

    def commit_db_job(self, job_id=None):
        '''Закрывает операцию с применением результата'''
        self._finish_db_job(tran_func='commit', job_id=job_id)
    def rollback_db_job(self, job_id=None):
        '''Закрывает операцию с отменой результата'''
        self._finish_db_job(tran_func='rollback', job_id=job_id)

    def __del__(self):
        '''
        Стандартный деструктор
        '''
        if hasattr(self, 'connections') and self.connections:
            try:
                self._finish_db_job()
            except:
                pass

    def prepare(self, data_id, param_values=None, verbose=False):
        '''
        Подготавливает курсор для последовательного получения данных из БД
        '''
        assert self.__initialized, 'Адаптер не проинициализирован'
        assert data_id in self.data, 'Неверный идентификатор запроса %s' % data_id
        assert 'queries' in self.data[data_id], 'Не задан ни один запрос для %s' % data_id

        self.new_db_job(data_id)
        for query in self.data[data_id]['queries']:
            params = self.data[data_id]['params']
            if params:
                assert isinstance(params, dict), 'Не верно заданы параметры запроса'
                if param_values:
                    param_values = [param_values.get(p_name, None) \
                                    for p_name in sorted(params.keys()) if p_name != '()']
                params = [(p_name, params[p_name]) for p_name in params.keys()]
#                params = sorted(params, key=lambda x: x[0])

            if query.type.upper() in 'TV':
                query = self.query_builder.get_query_sql(query, params)
            else:
                query = self.query_builder.get_proc_sql(query, params)
            if verbose:
                print(query)
                print(str(param_values)[:500])
            yield self.connections[data_id].cursor().execute(query, param_values)

    @staticmethod
    def fetch_to_dict(cursor, result=None):
        '''записывает результаты в структуру list of maps(key, value)'''
        if result is None:
            result = []
        cursor = next(cursor, None)
        while cursor is not None:
            info = cursor.description
            #data = cursor.fetchall()
            for data in cursor:
                result.append({info[i][0] : val for i, val in enumerate(data)})
            cursor = next(cursor, None)
        return result

    @staticmethod
    def fetch_to_json(cursor, encoding='utf-8', **kwargs):
        '''получает данные в json-формате'''
        fin = JsonStream(cursor, encoding=encoding, \
                    get_value_cb=(lambda x: str(x[0]).encode(encoding) if x else None))
        parser = JsonParser(encoding=encoding)
        res = parser.parse(fin)
        #не пропускаем гавно
        assert res
        return parser.json.json()

    @staticmethod
    def write_output(cursor, output, headers=False, delim=';'):
        '''
        Запись результата запроса в файл. На входе итератор запроса
        '''
        assert output, 'Выходной поток не проинициализирован'
        if not cursor:
            print('Пустой курсор')
            return
        if headers:
            output.write(delim.join([cname[0] for cname in cursor.description]))
            output.write('\n')
        for row in cursor:
            output.write(delim.join(row))
                #output.write('\n')

    def execute(self, data_id, output, headers=False, delim=';', param_values=None, verbose=False):
        '''
        Получение результата запроса
        '''
        # Готовим итератор запроса
        for cursor in self.prepare(data_id, param_values, verbose=verbose):
            self.write_output(cursor, output, headers, delim)
        self.commit_db_job(data_id)

def __test__(DSN, config, driver=pyodbc):
    data = DataAdapter(DSN, config, driver, query_builder=QueryBuilderNestedSelect(shrink_names=True))
#    query = data.data['APP_Calc_Subscr']['queries'][0]
#    params = data.data['APP_Calc_Subscr']['params']
#    param_values = {'@id':389572, '@batch':0}
#    param_values = [param_values.get(p_name, None) for p_name in sorted(params.keys())]
#    params = [(p_name, params[p_name]) for p_name in params.keys()]
#    params = sorted(params, key=lambda x: x[0])

#    sql = data.query_builder.build_sql(query)
#    sql = data.query_builder.get_query_sql(query, params)

    #globals()['__builtin__'].print(sql.replace('@', '_'))
    #data._get_query_sql(query, params, sql_lang='PGSQL')
    #globals()['__builtin__'].print(sql)

#    queries = pick_data('QGrapth[]', 'APP_Calc_Subscr')
#    qb = QueryBuilderNestedSelect(shrink_names=True, sql_lang=None)
#    sql = qb.build_sql(queries[0])
#    print(sql)


    with open(r'C:\Users\v-liderman\Desktop\t3.json', 'w', encoding='utf-8') as file:
        data.execute('APP_Calc_Subscr', file,  \
                     param_values={'@id':389572, '@batch':0}, verbose=True)

#        c = CacheHelper(base_dir=r'C:\Users\v-liderman\Desktop')
#        c.dump_data(data.query_builder.alias_map, data_type='Alias_map', data_id='APP_Calc_Subscr')

# выполняем тест только для самостоятельного запуска
DSN = r'DRIVER={ODBC Driver 11 for SQL Server};SERVER=dev-db-v-02\sql2017;Trusted_Connection=Yes;Database=OmniUS;'
#    #DSN = r'postgresql://omnix:0.123@192.168.17.129:5432/omnix'
#__test__(DSN, Node)
