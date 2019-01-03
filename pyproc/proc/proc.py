# -*- coding: utf-8 -*-
"""
Created on Tue Nov 13 11:47:15 2018

@author: V-Liderman
"""
#from __future__ import with_statement#for python2.5
#from __future__ import unicode_literals
#from __future__ import print_function as _print
import threading
from abc import ABC, abstractmethod
from datetime import datetime
#from ..core.types import t_dict, t_list
from ..util.util import get_typed_value
from .context import Context
from .types import ProcException, _not_null, _calc

class IProc(ABC):
    '''
    Абстрактный класс. Имплементация расчета
    '''
    #class methods
    #Схема документа
    doc_schema = dict()
    #схема строки документа
    details_schema = dict()

    #contructors
    def __init__(self, shell, use_node_cache=False):
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
        self._details = list()
        # режим обхода дерева. Ожидаем братских узлов с тем же PK или нет. Если да - кешируем
        #контекст узла
        self.use_node_cache = use_node_cache

    ###################### Результат расчета
    @property
    def result(self):
        '''Возвращает результат выполнения расчета'''
        if hasattr(self._shell, 'result') and self._shell.result is not None:
            return self._shell.result
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



    def _check_required(self, tree_node, required_type, keys=None, out_detail=None,\
                        assert_required=False):
        '''Проверяет набор на обязательные поля. Копирует типизированые значения в выходной набор'''
        assert not out_detail or isinstance(out_detail, dict)
        assert isinstance(tree_node, dict)
        if not keys:
            keys = tree_node.keys()
        for key in keys:
            if isinstance(key, tuple):
                db_key = key[1]
                key = key[0]
            else:
                db_key = key
            assert key in self.details_schema, 'Неверное имя поля:_check_required: %s.%s' % \
                                            (getattr(tree_node, 'key', ''), key)
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
                                         self.context['type'], key)
                        continue
                    else:
                        raise ProcException('Ошибка при валидации набора %s, поле: %s' % \
                                            (self.context['type'], key))
    ###################### Работа со строками расчета
    def _init_details(self, keys, key_d0, key_d1, tree_node, assert_required=False):
        '''Заполняет интервал из записи. Проверяет на соответствие обязательным полям.
        Разбивает интервал на подинтервалы'''
        def _intersect_details(details, item):
            '''Пересекает интервалы 2 наборов данных и в пересечении собирает атрибуты'''
            def _helper(d_item):
                d0 = max(d_item['d_date0'], item['d_date0'])
                d1 = min(d_item['d_date1'], item['d_date1'])
                d_item.update(item)
                d_item['d_date0'] = d0
                d_item['d_date1'] = d1

                return d_item

            # копируем массив и удаляем не пересекающиеся части
            details = [_helper(d_item.copy()) \
                       for d_item in details \
                       if d_item['d_date1'] > item['d_date0'] and \
                          item['d_date1'] > d_item['d_date0']]

            return details

        ############### тело
        assert self.context, 'Не задан конекст выполнения операции'
        #проверяем поля. Заполняем запись
        node_detail = dict()
        if key_d0:
            keys += [('d_date0', key_d0)]
        if key_d1:
            keys += [('d_date1', key_d1)]

        self._check_required(tree_node, _not_null, keys, node_detail, assert_required)

        #обязательно заполняем поля с датами интервалов
        if not node_detail.get('d_date0', None):
            node_detail['d_date0'] = self.context['d_date0']
            node_detail['n_year'] = node_detail['d_date0'].year
            node_detail['n_month'] = node_detail['d_date0'].month
        if not node_detail.get('d_date1', None):
            node_detail['d_date1'] = self.context['d_date1']

        #Вставляем запись в контекст с разбиением интервалов
        parent_details = self.context['parent_details']
        if parent_details:
            self.context['node_details'] += _intersect_details(parent_details, node_detail)
        else:
            self.context['node_details'].append(node_detail)
        return node_detail


    ###################### Helpers методов расчета
    def _prepare_check_time_intervals(self, detail):
        assert isinstance(detail, dict)
        #разница во времени
        date0, date1 = detail['d_date0'], detail['d_date1']
        t_delta = date1 - date0
        detail['n_days'] = t_delta.days
        detail['n_hours'] = int(t_delta.total_seconds()/3600)
        if not detail['n_month']:
            detail['n_month'] = date0.month
        if not detail['n_year']:
            detail['n_year'] = date0.year

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
        self._check_required(detail, tuple, keys=['n_cons', 'n_days', 'n_tariff', 'n_precision',
                                                  'b_tax_inside', 'n_tax', 'f_taxes'])
        #Расчет кол-ва
        cons = detail['n_cons']
        days = detail['n_days']
        #начальный контекст
        m_date0 = get_typed_value(self.context['d_date0'], datetime)
        m_date1 = get_typed_value(self.context['d_date1'], datetime)
        days0 = (m_date1 - m_date0).days

        #Тарификация
        precision = detail['n_precision']
        tariff = detail['n_tariff']
        amount = round(round((cons * days / days0), precision) * tariff, 2)
        tax = detail['n_tax']
        tax_amount = 0
        if detail['b_tax_inside']:
            tax_amount = round(amount / (1 + tax) * tax, 2)
        else:
            tax_amount = round(amount * tax, 2)
            amount += tax_amount

        detail['n_amount'] = amount
        detail['n_tax_amount'] = tax_amount

    ###################### события расширения расчета
    @abstractmethod
    def _cb_init(self, tree_node):
        '''Запуск расчета'''
        pass

    @abstractmethod
    def _cb_complete(self, tree_node):
        '''Конец расчета'''
        self.context.clean_context()
        pass

    def _cb_error(self, tree_node, error):
        '''ошибка в гланом обработчике'''
        self.log_tree_node_error('Ошибка в главном обработчике', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # Объекты
    @abstractmethod
    def _cb_conn_point_before(self, tree_node):
        '''Перед началом обработки объекта'''
        return self.context.enter_child_context(tree_node)

    def _cb_conn_point_after(self, tree_node):
        '''После обработки объекта'''
        return self.context.leave_child_context(tree_node)

    def _cb_conn_point_error(self, tree_node, error):
        '''Ошибка обработки объекта'''
        self.log_tree_node_error('Ошибка обработки объекта', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # /Объекты
    # Точки поставки
    @abstractmethod
    def _cb_network_pts_before(self, tree_node):
        '''Перед началом обработки ТоП'''
        return self.context.enter_child_context(tree_node)

    def _cb_network_pts_after(self, tree_node):
        '''После обработки ТоП'''
        return self.context.leave_child_context(tree_node)

    def _cb_network_pts_error(self, tree_node, error):
        '''Ошибка обработки ТоП'''
        self.log_tree_node_error('Ошибка обработки ТоП', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # /Точки поставки
    # Площади объекта или помещения
    @abstractmethod
    def _cb_contract_squares_before(self, tree_node):
        '''Перед началом обработки площади'''
        return self.context.enter_child_context(tree_node)

    def _cb_contract_squares_after(self, tree_node):
        '''После обработки площади'''
        return self.context.leave_child_context(tree_node)

    def _cb_contract_squares_error(self, tree_node, error):
        '''Ошибка обработки площади'''
        self.log_tree_node_error('Ошибка обработки площади', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # /Площади объекта или помещения
    # Помещения
    @abstractmethod
    def _cb_conn_point_sub_before(self, tree_node):
        '''Перед началом обработки помещения объекта'''
        return self.context.enter_child_context(tree_node)

    def _cb_conn_point_sub_after(self, tree_node):
        '''После обработки помещения объекта'''
        return self.context.leave_child_context(tree_node)

    def _cb_conn_point_sub_error(self, tree_node, error):
        '''Ошибка обработки помещения объекта'''
        self.log_tree_node_error('Ошибка обработки помещения объекта', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # /Помещения
    # Лицевые счета
    @abstractmethod
    def _cb_subscr_before(self, tree_node):
        '''Перед началом обработки ЛС'''
        return self.context.enter_child_context(tree_node)

    def _cb_subscr_after(self, tree_node):
        '''После обработки ЛС'''
        return self.context.leave_child_context(tree_node)

    def _cb_subscr_error(self, tree_node, error):
        '''Ошибка обработки ЛС'''
        self.log_tree_node_error('Ошибка обработки ЛС', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # /Лицевые счета
    # Учетные показатели
    @abstractmethod
    def _cb_registr_pts_before(self, tree_node):
        '''Перед началом обработки УП'''
        return self.context.enter_child_context(tree_node)

    def _cb_registr_pts_after(self, tree_node):
        '''После обработки УП'''
        return self.context.leave_child_context(tree_node)

    def _cb_registr_pts_error(self, tree_node, error):
        '''Ошибка обработки УП'''
        self.log_tree_node_error('Ошибка обработки УП', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # /Учетные показатели
    # Активность учетных показателей
    @abstractmethod
    def _cb_registr_pts_activity_before(self, tree_node):
        '''Перед началом обработки активности УП'''
        return self.context.enter_child_context(tree_node)

    def _cb_registr_pts_activity_after(self, tree_node):
        '''После обработки активности УП'''
        return self.context.leave_child_context(tree_node)

    def _cb_registr_pts_activity_error(self, tree_node, error):
        '''Ошибка обработки активности УП'''
        self.log_tree_node_error('Ошибка обработки активности УП', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # /Активность учетных показателей
    # Тариф учетных показателей
    @abstractmethod
    def _cb_registr_pts_tariff_before(self, tree_node):
        '''Перед началом обработки тарифа УП'''
        return self.context.enter_child_context(tree_node)

    def _cb_registr_pts_tariff_after(self, tree_node):
        '''После обработки тарифа УП'''
        return self.context.leave_child_context(tree_node)

    def _cb_registr_pts_tariff_error(self, tree_node, error):
        '''Ошибка обработки тарифа УП'''
        self.log_tree_node_error('Ошибка обработки тарифа УП', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # /Тариф учетных показателей
    # Метод расчета учетных показателей
    @abstractmethod
    def _cb_calc_method_before(self, tree_node):
        '''Перед началом обработки УП выбранным методом расчета'''
        return self.context.enter_child_context(tree_node)

    def _cb_calc_method_after(self, tree_node):
        '''После обработки УП выбранным методом расчета'''
        return self.context.leave_child_context(tree_node)

    def _cb_calc_method_error(self, tree_node, error):
        '''Ошибка обработки УП выбранным методом расчета'''
        self.log_tree_node_error('Ошибка обработки УП выбранным методом расчета',
                                 tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # /Метод расчета учетных показателей
    # Прибор учета
    @abstractmethod
    def _cb_device_before(self, tree_node):
        '''Перед началом обработки ПУ'''
        return self.context.enter_child_context(tree_node)

    def _cb_device_after(self, tree_node):
        '''После обработки ПУ'''
        return self.context.leave_child_context(tree_node)

    def _cb_device_error(self, tree_node, error):
        '''Ошибка обработки ПУ'''
        self.log_tree_node_error('Ошибка обработки ПУ', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # /Прибор учета
    # Показание прибора учета
    @abstractmethod
    def _cb_meter_reading_before(self, tree_node):
        '''Перед началом обработки показания ПУ'''
        return self.context.enter_child_context(tree_node)

    def _cb_meter_reading_after(self, tree_node):
        '''После обработки показания ПУ'''
        return self.context.leave_child_context(tree_node)

    def _cb_meter_reading_error(self, tree_node, error):
        '''Ошибка обработки показания ПУ'''
        self.log_tree_node_error('Ошибка обработки показания ПУ', tree_node=tree_node, error=error)
        return self.context.leave_child_context(tree_node)
    # / Показание прибора учета
    ######################получение данных для расчета
    def _get_root_node(self, tree_node):
        '''Получение корневого узла для обработки'''
        assert  isinstance(tree_node, (dict, list)), 'Неверный тип объекта'
        if isinstance(tree_node, list):
            return tree_node[0]
        return tree_node

    def _get_conn_points_list(self, tree_node):
        '''Список объектов для обработки'''
        return self.context.enter_subnode(tree_node, 'sd_conn_points')

    def _get_conn_points_sub_list(self, tree_node):
        '''Список помещений для обработки'''
        return self.context.enter_subnode(tree_node, 'sd_conn_points_sub')

    def _get_ed_network_pts_list(self, tree_node):
        '''Список ТоП для обработки'''
        return self.context.enter_subnode(tree_node, 'ed_network_pts')

    def _get_subscr_list(self, tree_node):
        '''Список ЛС для обработки'''
        return self.context.enter_subnode(tree_node, 'sd_subscr')

    def _get_registr_pts_list(self, tree_node):
        '''Список УП для обработки'''
        return self.context.enter_subnode(tree_node, 'ed_registr_pts')

    def _get_calc_methods_list(self, tree_node):
        '''Список методов расчета УП для обработки'''
        return self.context.enter_subnode(tree_node, 'ed_registr_pts_calc_methods')

    def _get_contract_squares_list(self, tree_node):
        '''Список площадей объекта или помещения для обработки'''
        return self.context.enter_subnode(tree_node, 'sd_contract_squares')
    def _get_registr_pts_activities_list(self, tree_node):
        '''Список активности УП для обработки'''
        return self.context.enter_subnode(tree_node, 'ed_registr_pts_activity')
    def _get_registr_pts_tariff_list(self, tree_node):
        '''Список тарифов УП для обработки'''
        return self.context.enter_subnode(tree_node, 'ed_registr_pts_tariff')

    def _get_devices_list(self, tree_node):
        '''Список ПУ УП для обработки'''
        return self.context.enter_subnode(tree_node, 'ed_devices')

    def _get_meter_readings_list(self, tree_node):
        '''Список показаний ПУ УП для обработки'''
        return self.context.enter_subnode(tree_node, 'ed_meter_readings')

    @abstractmethod
    def _get_calc_method(self, detail):
        '''Получить функцию метод расчета УП'''
        return lambda *arg: None
    ####################### Тело обобщенного расчета
    def run(self, tree_node, context, hold_node=None):
        '''Запуск расчета. Передается узел для обработки, параметры'''
        #дерево для обработки
        self.tree = tree_node
        # инициализация дат расчета
        context['d_date0'] = get_typed_value(context.pop('d_date1'), datetime)
        context['d_date1'] = get_typed_value(context.pop('d_date2'), datetime)
        #переменные текущего контекста
        self.context = Context(context, self.use_node_cache)
        #id потока в котором обрабатывается расчета
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
            self.tree = None
            self.context = None
            self.worker_id = None
            #хранение узла по подписке (чтобы не грохнул сборщик мусора в многопоточном режиме)
            if hold_node:
                del hold_node

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
            except Exception as error:
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
            except Exception as error:
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
            except Exception as error:
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
            except Exception as error:
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
            except Exception as error:
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
            except Exception as error:
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
            except Exception as error:
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
            except Exception as error:
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
            except Exception as error:
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
            except Exception as error:
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
            except Exception as error:
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
            except Exception as error:
                self._cb_meter_reading_error(node, error)
    @abstractmethod
    def _proc_meter_reading(self, tree_node):
        '''Обработка показания ПУ ЛС'''
        pass
