# -*- coding: utf-8 -*-
"""
Created on Tue Nov 13 11:47:15 2018

@author: V-Liderman
"""
#from __future__ import with_statement#for python2.5
#from __future__ import unicode_literals
#from __future__ import print_function as _print
import sys
import os
import threading
__RUN_DIR__ = os.path.dirname(__file__)
sys.path.append(__RUN_DIR__)
from abc import ABC, abstractmethod
from JsonStream import JsonTree, lc_dict
from Util import get_typed_value
from datetime import datetime

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

class IProc(ABC):
    '''
    Абстрактный класс. Имплементация расчета
    '''
    # режим обхода дерева. Ожидаем братских узлов с тем же PK или нет. Если да - кешируем
    #контекст узла
    use_node_cache = False

    #class methods
    #Схема документа
    doc_schema = lc_dict()
    #схема строки документа
    details_schema = lc_dict()
    #__Details = '__Details'
    # тег, под которым записывается в дерево контекст, если работаем с обычным json
    __Context = '__Context'
    #__Parent= '__Parent'
    # При загрузке контекста обнаружили новый узел, которого нет в кеше
    _New_Node = True

    #contructors
    def __init__(self, shell):
        '''Базовый конструктор'''
        super().__init__()
        # дерево с исходными данными
        self.tree = None
        # глобальные переменные, переданные в расчет
        self.context = None
        # активный класс, вызвавший расчет
        self._shell = shell
        self.worker_id = None
        # лог для актирования событий расчет
        self.log = shell.log
        # кеш справочниов
        self.dict_cache = shell.dict_cache
        # список результатов расчета
        self._details = JsonTree.t_list()

    ###################### Результат расчета
    @property
    def result(self):
        '''Возвращает результат выполнения расчета'''
        if hasattr(self._shell, 'result') and self._shell.result is not None:
            return self._shell.result
        else:
            return self._details
    @result.setter
    def result(self, value):
        if hasattr(self._shell, 'result') and self._shell.result is not None:
            self._shell.result = value
        else:
            self._details = value

    def _push_result(self, detail):
        '''Запись результата расчета в БД'''
        self.result.append(detail)

    ###################### Обработка ошибок. Проверка обязательных полей. Обход дерева
    def log_tree_node_error(self, msg, *arg, **kwarg):
        '''Логирует ошибку обработки данных'''
        if hasattr(self._shell, 'log_tree_node_error') and callable(self._shell.log_tree_node_error):
            self._shell.log_tree_node_error(msg, *arg, **kwarg)
        else:
            #tree_node = kwarg['tree_node']
            error = kwarg['error']
            self.log.warning(msg, *arg, exc_info=error)
        #print(msg)

    @staticmethod
    def assert_requred(value, message):
        if not value:
            raise ProcException(message)
    @staticmethod
    def check_tree_node_type(tree_node, _type, msg=None):
        if not msg:
            entity = tree_node.key if hasattr(tree_node, 'key') else None
            pk = IProc.get_entity_PK(tree_node)
            msg = 'Неверный тип объекта %s[%s]' % (entity, pk)
        IProc.assert_requred( isinstance(tree_node, _type) or \
                isinstance(tree_node, JsonTree) and tree_node.type == _type and \
                    tree_node() != _type(), msg)
    @staticmethod
    def enter_subnode(tree_node, subnode_name):
        '''Входит в дочерний узел с проверкой'''
        #проверяем в то что в узел можно входить
        IProc.check_tree_node_type(tree_node, JsonTree.t_dict)
        entity = tree_node.key if hasattr(tree_node, 'key') else None
        pk = IProc.get_entity_PK(tree_node)
        if not subnode_name in tree_node:
            raise ProcException('Не найден узел %s в %s[%s]' % (subnode_name, entity, pk))
        else:
            subnode = tree_node[subnode_name]
            err_msg = 'Пустой список потомков % s узла %s[%s]' % (subnode_name, entity, pk)
            IProc.check_tree_node_type(subnode, JsonTree.t_list, err_msg)
#            if len(subnode) == 0:
#                raise ProcException(err_msg)
            return tree_node[subnode_name]

    def _check_required(self, tree_node, required_type, keys=None, out_detail=None,\
                        assert_required=False):
        '''Проверяет набор на обязательные поля. Копирует типизированые значения в выходной набор'''
        assert not out_detail or isinstance(out_detail, JsonTree.t_dict)
        assert tree_node and isinstance(tree_node, (JsonTree, JsonTree.t_dict))
        if not keys:
            keys = tree_node.keys()
        for key in keys:
            if isinstance(key, tuple):
                db_key = key[1]
                key = key[0]
            else:
                db_key = key
            assert key in self.details_schema, 'Неверное имя поля:_check_required: %s.%s' % \
                                            (tree_node.key, key)
            schema = self.details_schema.get(key, None)
            value = tree_node.get(db_key, None)
            if out_detail is not None and value is not None:
                value = get_typed_value(value, schema[0])
                out_detail[key] = value
            if isinstance(schema, required_type) and value is None:
                if len(schema) > 1:
                    #подставляем default
                    tree_node[key] = schema[1]
                    if out_detail is not None:
                        out_detail[key] = schema[1]
                else:
                    if not assert_required:
                        self.log.warning('Ошибка при валидации набора %s, поле: %s',\
                                         tree_node.key, key)
                        continue
                    else:
                        raise ProcException('Ошибка при валидации набора %s, поле: %s' % \
                                            (tree_node.key, key))
    ###################### Работа с контекстом
    def _init_details(self, keys, key_d0, key_d1, tree_node, assert_required=False):
        '''Заполняет интервал из записи. Проверяет на соответствие обязательным полям.
        Разбивает интервал на подинтервалы'''
        def _intersect_details(details, item):
            '''Пересекает интервалы 2 наборов данных и в пересечении собирает атрибуты'''
            def _helper(d_item):
                d0 = max(d_item['D_Date0'], item['D_Date0'])
                d1 = min(d_item['D_Date1'], item['D_Date1'])
                d_item.update(item)
                d_item['D_Date0'] = d0
                d_item['D_Date1'] = d1

                return d_item

            # копируем массив и удаляем не пересекающиеся части
            details = [_helper(d_item.copy()) \
                       for d_item in details \
                       if d_item['D_Date1'] > item['D_Date0'] and \
                          item['D_Date1'] > d_item['D_Date0']]

            return details

        def _get_default_dates(context):
            '''Возвращает универвальные границы интервалов расчета'''
            return get_typed_value(context['D_Date0'], datetime), \
                   get_typed_value(context['D_Date1'], datetime)

        ############### тело
        assert self.context, 'Не задан конекст выполнения операции'
        #проверяем поля. Заполняем запись
        node_detail = JsonTree.t_dict()
        if key_d0:
            keys += [('D_Date0', key_d0)]
        if key_d1:
            keys += [('D_Date1', key_d1)]

        self._check_required(tree_node, _not_null, keys, node_detail, assert_required)

        #обязательно заполняем поля с датами интервалов
        if not node_detail.get('D_Date0', None):
            node_detail['D_Date0'] = _get_default_dates(self.context)[0]
            node_detail['N_Year'] = node_detail['D_Date0'].year
            node_detail['N_Month'] = node_detail['D_Date0'].month
        if not node_detail.get('D_Date1', None):
            node_detail['D_Date1'] = _get_default_dates(self.context)[1]

        #Вставляем запись в контекст с разбиением интервалов
        parent_details = self.context.get('parent_details', None)
        if parent_details:
            self.context['node_details'] += _intersect_details(parent_details, node_detail)
        else:
            self.context['node_details'].append(node_detail)
        return node_detail

    def init_context(self):
        '''Конекст необходим для рекурсивоного обхода дерева с данными
        Контекст содержит кэш элементов, который используется если дереро не сгруппировано
        по элементам от вершины к узлам, а просто содержит иерархическое представление плоских
        записей.
        Контекст содержит массив строк расчета родительского узла и результат декартового
        произведения с учетом интервалов текущего уровня потомков'''
        #текущий тип. Используется для построения ключа кеша
        self.context['type'] = None
        #кеш элементов. Ключ - (тип, PK). Кеш будет глобальный в shell
        if self.context.get('context_cache', None) is None:
            self.context['context_cache'] = dict()
        #Оригинальные родительские строки
        self.context['parent_details'] = JsonTree.t_list()
        #Строки после пересечения с текущим уровнем
        self.context['node_details'] = JsonTree.t_list()
        #Родитель. Нужно для восстановления контекста при выходе из узла
        self.context['parent'] = None
        #Текущий узел. Нужно для восстановления контекста при выходе из узла
        self.context['node'] = None
        #название поля с текущий первичным ключом
        self.context['key'] = dict()
        #Использовать общий конекст родитель-потомок
        #Контекст проинициализирован
        self.context['_initialized'] = True

    @staticmethod
    def get_entity_PK(tree_node, key=None):
        '''Получение первичного ключа записи'''
        #TODO: сделать нормальный обработчик с учетом схемы!!!!
        assert isinstance(tree_node, (JsonTree.t_dict, JsonTree)), 'Неверный тип записи'
        key = key if key else 'LINK'
        pk = tree_node.get(key, None)
        if isinstance(tree_node, JsonTree) and tree_node.key:
            key = tree_node.key
        else:
            key = id(tree_node)
        assert pk, 'Не возможно определить PK записи %s' % key
        return (key, pk)

    def restore_context(self, tree_node, pk=None, key=None):
        '''Восстанавливает контекст по РК'''
        if not pk:
            pk = self.get_entity_PK(tree_node, key=key)
        return self.context['context_cache'].get(pk, None)

    def save_context(self, tree_node, key=None):
        '''Сохраняет контекст по РК'''
        pk = self.get_entity_PK(tree_node, key=key)
        self.context['context_cache'][pk] = self.context

    def enter_child_context(self, tree_node, common=False, key=None):
        ''' Начало нового уровня по иерархии данных. Аналог start_map
        common - братья разделяют общий контекст родителя
        key - название PK сущности'''
        assert isinstance(self.context, JsonTree.t_dict)
        # каждый узел хранит свою копию контекста. Копируются только ключи справочника
        self.context = self.context.copy()
        if not self.context.get('_initialized', False):
            self.init_context()
        # Если дерево не иерархия - я структурированная плоская таблица, используем кеш,
        # иначе сохраняем контекст в узле
        _context = None
        if self.use_node_cache:
            _context = self.restore_context(tree_node, key=key)
        else:
            if isinstance(tree_node, JsonTree):
                _context = tree_node.context
            else:
                _context = tree_node.get(self.__Context, None)
        #Если нет записанного контекста - заново его инициализируем
        if not _context:
            if self.use_node_cache:
                self.save_context(tree_node, key=key)
            else:
                if isinstance(tree_node, JsonTree):
                    tree_node.context = self.context
                else:
                    tree_node[self.__Context] = self.context

            #Запоминаем родителя для восстановления контекста
            self.context['parent'] = self.context['node']
            self.context['node'] = tree_node
            self.context['common'] = common

            #конекст вышестоящего узла становится узлом родителя
            self.context['parent_details'] = self.context['node_details']
            #Строки после пересечения с текущим уровнем
            self.context['node_details'] = JsonTree.t_list()
            # ключ
            if key:
                self.context['key'][id(tree_node)] = key
            return self._New_Node
        else:
            # при разделяемомо контексте строки родителя наполяются строками ребенка
            if common:
                _context['node_details'] = self.context['node_details']
            self.context = _context
            return not self._New_Node

    def leave_child_context(self, tree_node):
        ''' Конец уровня по иерархии данных. Возврат наверх. Аналог end_map'''
        assert isinstance(self.context, JsonTree.t_dict)
        if not self.context.get('_initialized', False):
            self.init_context()
        #получаем поле с ключом
        key = self.context['key'].get(id(tree_node), None)
        # рассчитываем на передачу по ссылке
        # Если дерево не иерархия - я структурированная плоская таблица, используем кеш
        if self.use_node_cache:
            self.context = self.save_context(tree_node, key=key)
        else:
            if isinstance(tree_node, JsonTree):
                self.context = tree_node.context
            else:
                self.context = tree_node[self.__Context]
        # Теперь надо восстановить контекст родителя
        is_new = None
        if self.context['parent']:
            is_new = self.enter_child_context(self.context['parent'], self.context['common'], key=key)
        else:
            #Что мы наделали в узел кладем родителю
            self.context['parent_details'] = self.context['node_details']
            self.context['node_details'] = JsonTree.t_list()

        # освободим память
        tree_node.context = None
        return is_new

    ###################### Helpers методов расчета
    def _prepare_check_time_intervals(self, detail):
        assert isinstance(detail, JsonTree.t_dict)
        #разница во времени
        date0, date1 = detail['D_Date0'], detail['D_Date1']
        t_delta = date1 - date0
        detail['N_Days'] = t_delta.days
        detail['N_Hours'] = int(t_delta.total_seconds()/3600)
        if not detail['N_Month']:
            detail['N_Month'] = date0.month
        if not detail['N_Year']:
            detail['N_Year'] = date0.year

    def _proc_calc_method(self, tree_node):
        '''Запуск метода расчета УП ЛС'''
        #проверка строчек для расчета на обязательные поля
        for detail in self.context['node_details']:
            #проверка набора для расчета на обязательные поля
            self._check_required(detail, _not_null)
            #подготовка временных интервалов для расчета
            self._prepare_check_time_intervals(detail)
            #обработка методов расчета
            _cm_proc = self._get_calc_method(detail)
            assert _cm_proc and callable(_cm_proc), 'Не удалось разрешить метод расчета'
            _cm_proc(detail)

            #проверка набора для расчета на обязательные поля
            self._check_required(detail, _calc)

            #тарификация
            #self._calc_cost(detail)

            #проталкивание результата
            self._push_result(detail)

    def _calc_cost(self, detail):
        '''Тарификация'''
        assert detail
        self._check_required(detail, tuple, keys=['N_Cons', 'N_Days', 'N_Tariff', 'N_Precision',
                                                  'B_Tax_Inside', 'N_Tax', 'F_Taxes'])
        #Расчет кол-ва
        cons = detail['N_Cons']
        days = detail['N_Days']
        #начальный контекст
        m_date0 = get_typed_value(self.context['D_Date0'], datetime)
        m_date1 = get_typed_value(self.context['D_Date1'], datetime)
        days0 = (m_date1 - m_date0).days

        #Тарификация
        precision = detail['N_Precision']
        tariff = detail['N_Tariff']
        amount = (round((cons * days / days0), precision) * tariff, 2)
        tax = detail['N_Tax']
        tax_amount = 0
        if detail['B_Tax_Inside']:
            tax_amount = round(amount / (1 + tax) * tax, 2)
        else:
            tax_amount = round(amount * tax, 2)
            amount += tax_amount

        detail['N_amount'] = amount
        detail['N_Tax_amount'] = tax_amount

    ###################### события расширения расчета
    @abstractmethod
    def _cb_init(self, tree_node):
        '''Запуск расчета'''
        pass

    @abstractmethod
    def _cb_complete(self, tree_node):
        '''Конец расчета'''
        pass
    
    def _cb_error(self, tree_node, error):
        '''ошибка в гланом обработчике'''
        self.log_tree_node_error('Ошибка в главном обработчике', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # Объекты
    @abstractmethod
    def _cb_conn_point_before(self, tree_node):
        '''Перед началом обработки объекта'''
        return self.enter_child_context(tree_node)

    def _cb_conn_point_after(self, tree_node):
        '''После обработки объекта'''
        return self.leave_child_context(tree_node)

    def _cb_conn_point_error(self, tree_node, error):
        '''Ошибка обработки объекта'''
        self.log_tree_node_error('Ошибка обработки объекта', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # /Объекты
    # Точки поставки
    @abstractmethod
    def _cb_network_pts_before(self, tree_node):
        '''Перед началом обработки ТоП'''
        return self.enter_child_context(tree_node)

    def _cb_network_pts_after(self, tree_node):
        '''После обработки ТоП'''
        return self.leave_child_context(tree_node)

    def _cb_network_pts_error(self, tree_node, error):
        '''Ошибка обработки ТоП'''
        self.log_tree_node_error('Ошибка обработки ТоП', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # /Точки поставки
    # Площади объекта или помещения
    @abstractmethod
    def _cb_contract_squares_before(self, tree_node):
        '''Перед началом обработки площади'''
        return self.enter_child_context(tree_node)

    def _cb_contract_squares_after(self, tree_node):
        '''После обработки площади'''
        return self.leave_child_context(tree_node)

    def _cb_contract_squares_error(self, tree_node, error):
        '''Ошибка обработки площади'''
        self.log_tree_node_error('Ошибка обработки площади', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # /Площади объекта или помещения
    # Помещения
    @abstractmethod
    def _cb_conn_point_sub_before(self, tree_node):
        '''Перед началом обработки помещения объекта'''
        return self.enter_child_context(tree_node)

    def _cb_conn_point_sub_after(self, tree_node):
        '''После обработки помещения объекта'''
        return self.leave_child_context(tree_node)

    def _cb_conn_point_sub_error(self, tree_node, error):
        '''Ошибка обработки помещения объекта'''
        self.log_tree_node_error('Ошибка обработки помещения объекта', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # /Помещения
    # Лицевые счета
    @abstractmethod
    def _cb_subscr_before(self, tree_node):
        '''Перед началом обработки ЛС'''
        return self.enter_child_context(tree_node)

    def _cb_subscr_after(self, tree_node):
        '''После обработки ЛС'''
        return self.leave_child_context(tree_node)

    def _cb_subscr_error(self, tree_node, error):
        '''Ошибка обработки ЛС'''
        self.log_tree_node_error('Ошибка обработки ЛС', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # /Лицевые счета
    # Учетные показатели
    @abstractmethod
    def _cb_registr_pts_before(self, tree_node):
        '''Перед началом обработки УП'''
        return self.enter_child_context(tree_node)

    def _cb_registr_pts_after(self, tree_node):
        '''После обработки УП'''
        return self.leave_child_context(tree_node)

    def _cb_registr_pts_error(self, tree_node, error):
        '''Ошибка обработки УП'''
        self.log_tree_node_error('Ошибка обработки УП', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # /Учетные показатели
    # Активность учетных показателей
    @abstractmethod
    def _cb_registr_pts_activity_before(self, tree_node):
        '''Перед началом обработки активности УП'''
        return self.enter_child_context(tree_node)

    def _cb_registr_pts_activity_after(self, tree_node):
        '''После обработки активности УП'''
        return self.leave_child_context(tree_node)

    def _cb_registr_pts_activity_error(self, tree_node, error):
        '''Ошибка обработки активности УП'''
        self.log_tree_node_error('Ошибка обработки активности УП', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # /Активность учетных показателей
    # Тариф учетных показателей
    @abstractmethod
    def _cb_registr_pts_tariff_before(self, tree_node):
        '''Перед началом обработки тарифа УП'''
        return self.enter_child_context(tree_node)

    def _cb_registr_pts_tariff_after(self, tree_node):
        '''После обработки тарифа УП'''
        return self.leave_child_context(tree_node)

    def _cb_registr_pts_tariff_error(self, tree_node, error):
        '''Ошибка обработки тарифа УП'''
        self.log_tree_node_error('Ошибка обработки тарифа УП', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # /Тариф учетных показателей
    # Метод расчета учетных показателей
    @abstractmethod
    def _cb_calc_method_before(self, tree_node):
        '''Перед началом обработки УП выбранным методом расчета'''
        return self.enter_child_context(tree_node)

    def _cb_calc_method_after(self, tree_node):
        '''После обработки УП выбранным методом расчета'''
        return self.leave_child_context(tree_node)

    def _cb_calc_method_error(self, tree_node, error):
        '''Ошибка обработки УП выбранным методом расчета'''
        self.log_tree_node_error('Ошибка обработки УП выбранным методом расчета',
                                 tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # /Метод расчета учетных показателей
    # Прибор учета
    @abstractmethod
    def _cb_device_before(self, tree_node):
        '''Перед началом обработки ПУ'''
        return self.enter_child_context(tree_node)

    def _cb_device_after(self, tree_node):
        '''После обработки ПУ'''
        return self.leave_child_context(tree_node)

    def _cb_device_error(self, tree_node, error):
        '''Ошибка обработки ПУ'''
        self.log_tree_node_error('Ошибка обработки ПУ', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # /Прибор учета
    # Показание прибора учета
    @abstractmethod
    def _cb_meter_reading_before(self, tree_node):
        '''Перед началом обработки показания ПУ'''
        return self.enter_child_context(tree_node)

    def _cb_meter_reading_after(self, tree_node):
        '''После обработки показания ПУ'''
        return self.leave_child_context(tree_node)

    def _cb_meter_reading_error(self, tree_node, error):
        '''Ошибка обработки показания ПУ'''
        self.log_tree_node_error('Ошибка обработки показания ПУ', tree_node=tree_node, error=error)
        return self.leave_child_context(tree_node)
    # / Показание прибора учета
    ######################получение данных для расчета
    def _get_root_node(self, tree_node):
        '''Получение корневого узла для обработки'''
        assert  isinstance(tree_node, (dict, list, JsonTree)), 'Неверный тип объекта'
        if isinstance(tree_node, JsonTree) and tree_node.type == JsonTree.t_list:
            return tree_node[0]
        if isinstance(tree_node, list):
            return tree_node[0]
        return tree_node

    def _get_conn_points_list(self, tree_node):
        '''Список объектов для обработки'''
        return self.enter_subnode(tree_node, 'SD_Conn_Points')

    def _get_conn_points_sub_list(self, tree_node):
        '''Список помещений для обработки'''
        return self.enter_subnode(tree_node, 'SD_Conn_Points_Sub')

    def _get_ed_network_pts_list(self, tree_node):
        '''Список ТоП для обработки'''
        return self.enter_subnode(tree_node, 'ED_Network_Pts')

    def _get_subscr_list(self, tree_node):
        '''Список ЛС для обработки'''
        return self.enter_subnode(tree_node, 'SD_Subscr')

    def _get_registr_pts_list(self, tree_node):
        '''Список УП для обработки'''
        return self.enter_subnode(tree_node, 'ED_Registr_Pts')

    def _get_calc_methods_list(self, tree_node):
        '''Список методов расчета УП для обработки'''
        return self.enter_subnode(tree_node, 'ED_Registr_Pts_Calc_Methods')

    def _get_contract_squares_list(self, tree_node):
        '''Список площадей объекта или помещения для обработки'''
        return self.enter_subnode(tree_node, 'SD_Contract_Squares')
    def _get_registr_pts_activities_list(self, tree_node):
        '''Список активности УП для обработки'''
        return self.enter_subnode(tree_node, 'ED_Registr_Pts_Activity')
    def _get_registr_pts_tariff_list(self, tree_node):
        '''Список тарифов УП для обработки'''
        return self.enter_subnode(tree_node, 'ED_Registr_Pts_Tariff')

    def _get_devices_list(self, tree_node):
        '''Список ПУ УП для обработки'''
        return self.enter_subnode(tree_node, 'ED_Devices')

    def _get_meter_readings_list(self, tree_node):
        '''Список показаний ПУ УП для обработки'''
        return self.enter_subnode(tree_node, 'ED_Meter_Readings')

    @abstractmethod
    def _get_calc_method(self, detail):
        '''Получить функцию метод расчета УП'''
        return lambda *arg: None
    ####################### Тело обобщенного расчета
    def run(self, tree_node, context):
        '''Запуск расчета. Передается узел для обработки, параметры'''
        self.tree = tree_node
        self.context = context
        self.worker_id = threading.current_thread().ident
        try:
            # получение верхнего узла расчета
            tree_node = self._get_root_node(tree_node)
            #начало расчета
            self._cb_init(tree_node)
            # обработчик
            self._proc_main(tree_node)
            # конец обработки
            self._cb_complete(tree_node)
        except Exception as error:
            self._cb_error(tree_node, error)
        finally:
            return self.result

    def _proc_main(self, tree_node):
        '''
        Обработка корня дерева
        '''
        for node in self._get_conn_points_list(tree_node):
            try:
                #событие вход
                self._cb_conn_point_before(node)
                # обработка площадей объекта
                self._proc_conn_point(node)
                #событие выход
                return self._cb_conn_point_after(node)
            except ProcException as error:
                self._cb_conn_point_error(node, error)

    def _proc_conn_point(self, tree_node):
        '''Обработка объекта'''

        #обработка ТоП объекта
        for node in self._get_ed_network_pts_list(tree_node):
            try:
                #событие вход
                self._cb_network_pts_before(node)
                #обработчик
                self._proc_network_pts(node)
                #событие выход
                self._cb_network_pts_after(node)
            except ProcException as error:
                self._cb_network_pts_error(node, error)
        #обработка площадей объекта
        for node in self._get_contract_squares_list(tree_node):
            try:
                #событие вход
                self._cb_contract_squares_before(node)
                #обработчик
                self._proc_contract_squares(node)
                #событие выход
                self._cb_contract_squares_after(node)
            except ProcException as error:
                self._cb_contract_squares_error(node, error)
        #обработка помещений
        for node in self._get_conn_points_sub_list(tree_node):
            try:
                #событие вход
                self._cb_conn_point_sub_before(node)
                #событие выход
                self._proc_conn_point_sub(node)
                #событие выход
                self._cb_conn_point_sub_after(node)
            except ProcException as error:
                self._cb_conn_point_sub_error(node, error)

    @abstractmethod
    def _proc_network_pts(self, tree_node):
        '''Обработка ТоП объекта'''

    @abstractmethod
    def _proc_contract_squares(self, tree_node):
        '''обработка площадей'''

    def _proc_conn_point_sub(self, tree_node):
        '''Обработка помещения'''
        #обработка площадей помещения
        for node in self._get_contract_squares_list(tree_node):
            try:
                #событие вход
                self._cb_contract_squares_before(node)
                #обработчик
                self._proc_contract_squares(node)
                #событие выход
                self._cb_contract_squares_after(node)
            except ProcException as error:
                self._cb_contract_squares_error(node, error)
        #обработка ЛС
        for node in self._get_subscr_list(tree_node):
            try:
                #событие вход
                self._cb_subscr_before(node)
                #обработка ЛС
                self._proc_subscr(node)
                #событие выход
                self._cb_subscr_after(node)
            except ProcException as error:
                self._cb_subscr_error(node, error)

    def _proc_subscr(self, tree_node):
        '''Обработка ЛС'''
        for node in self._get_registr_pts_list(tree_node):
            try:
                #событие вход
                self._cb_registr_pts_before(node)
                # обработка
                self._proc_registr_pts(node)
                #событие выход
                self._cb_registr_pts_after(node)
            except ProcException as error:
                self._cb_registr_pts_error(node, error)

    def _proc_registr_pts(self, tree_node):
        '''Обработка УП ЛС'''
        #обработка активности
        for node in self._get_registr_pts_activities_list(tree_node):
            try:
                 #событие вход
                self._cb_registr_pts_activity_before(node)
                # обработка
                self._proc_registr_pts_activity(node)
                #событие выход
                self._cb_registr_pts_activity_after(node)
            except ProcException as error:
                self._cb_registr_pts_activity_error(node, error)
        #обработка тарифов
        for node in self._get_registr_pts_tariff_list(tree_node):
            try:
                 #событие вход
                self._cb_registr_pts_tariff_before(node)
                # обработка
                self._proc_registr_pts_tariff(node)
                #событие выход
                self._cb_registr_pts_tariff_after(node)
            except ProcException as error:
                self._cb_registr_pts_tariff_error(node, error)
        #обработка методов расчета
        for node in self._get_calc_methods_list(tree_node):
            try:
                 #событие вход
                self._cb_calc_method_before(node)
                # обработка
                self._proc_calc_method(node)
                #событие выход
                self._cb_calc_method_after(node)
            except ProcException as error:
                self._cb_calc_method_error(node, error)

    @abstractmethod
    def _proc_registr_pts_tariff(self, tree_node):
        '''Обработка тарифов УП ЛС'''
        pass
    @abstractmethod
    def _proc_registr_pts_activity(self, tree_node):
        '''Обработка активности УП ЛС'''
        pass
    # новые обработчики
    def _proc_device_pts(self, tree_node):
        '''Обработка ПУ УП ЛС'''
        #обработка показаний
        for node in self._get_devices_list(tree_node):
            try:
                #событие вход
                self._cb_device_before(node)
                #обработка показания
                self._proc_device(node)
                #событие выход
                self._cb_device_after(node)
            except ProcException as error:
                self._cb_device_error(node, error)

    def _proc_device(self, tree_node):
        '''Обработка ПУ ЛС'''
        #обработка показаний
        for node in self._get_meter_readings_list(tree_node):
            try:
                #событие вход
                self._cb_meter_reading_before(node)
                #обработка показания
                self._proc_meter_reading(node)
                #событие выход
                self._cb_meter_reading_after(node)
            except ProcException as error:
                self._cb_meter_reading_error(node, error)
    @abstractmethod
    def _proc_meter_reading(self, tree_node):
        '''Обработка показания ПУ ЛС'''
        pass
