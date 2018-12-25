# -*- coding: utf-8 -*-
"""
Содержит класс для построения запросов с возвращение результатов в json.
Для получения результатов возможны 2 варианта:
    * NestedSelect - иерархия с агрегированием (один и тот же узел не повторяется, а подчиненные
    узлы - списки внутри узла)
    * NestedFrom - иерархия без агригирования (просто представление плоской записи в иерархической
    форме)
Фомирование запросов возможно в 2 видах (sql_lang): MSSQL и PGSQL. MSQL должен быть 2017+
Классы реализованы в виде общего базового класса и 2х подклассов, реализующих специфику.
Классы используются в DataAdapter
"""

#from __future__ import unicode_literals
#from __future__ import print_function as _print
import re
from abc import ABC, abstractmethod
from ..core.types import t_dict
#from pprint import pprint
#from Query import Node
#from ..json import JsonTree


class IQueryBuilder(ABC):
    '''Абстрактный класс с методами для построения запроса. Dependency Enjection pattern
    '''
    def __init__(self, sql_lang=None, shrink_names=False):
        self.alias_map = t_dict()
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

        q_name = t_ref if self.shrink_names else entity_name.replace('.', '_')
        #по-умолчанию - мы MSSQL
        if   self.sql_lang == 'PGSQL':
            sql = ('{0}SELECT array_to_json(array_agg(row_to_json({2}))) as t '+\
                  'FROM ({1}{0}) {2}').format(offset(2), sql, t_ref)
        else:
            sql += '%sFOR JSON AUTO' % offset(o3)
            sql += (", ROOT('%s')" % q_name) if skip_wrap else ''

        return  offset(1)+'('+sql+ offset(1) +') AS %s' % q_name
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
