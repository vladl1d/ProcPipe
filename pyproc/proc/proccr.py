# -*- coding: utf-8 -*-
"""
Класс прикладного расчета.
Created on Wed Nov 14 17:16:24 2018
@author: V-Liderman
"""
from uuid import UUID
from datetime import datetime
#from ..core.types import t_dict, t_list
from .proc import IProc
from .types import ProcException, _null, _not_null, _calc

class ProcCR(IProc):
    '''
    Имплементация расчета
    '''
    #class methods
    #Схема документа
    doc_schema = dict()
    #схема строки документа
    details_schema = dict({
        'session':           _not_null((int,)),
        'f_division':        _not_null((int,)),
        'f_subscr':          _not_null((int,)),
        'f_balance_types':   _null((int,)),
        'f_energy_levels' :  _null((int,)),
        'f_sale_items':      _not_null((int,)),
        'c_sale_items':      _null((str,)),
        'f_units':           _not_null((int,)),
        'f_registr_pts':     _not_null((int,)),
        'f_calc_methods':    _not_null((int,)),
        'f_tariff':          _not_null((int,)),
        'f_norms':           _null((int,)),
        'f_time_zones':      _null((int,)),
        'f_cons_zones':      _null((int,)),
        'f_conn_points':     _not_null((str,)),
        'f_conn_points_sub': _not_null((str,)),
        'f_network_pts':     _not_null((str,)),
        'f_prop_forms':      _null((int,)),
        'd_date0':           _not_null((datetime,)),
        'd_date1':           _null((datetime,)),
        'n_square':          _null((float,)),
        'n_count':           _null((int, 1)),
        'n_percent':         _not_null((float, 1.,)),
        #'n_cons1':           _null((float,)),
        #'n_cons0':           _null((float,)),
        'n_cons':            _calc((float,)),
        'n_norm':            _null((float,)),
        'n_precision':       _not_null((int, 0,)),
        'n_days':            _calc((int,)),
        'n_hours':           _calc((int,)),
        'n_month':           _calc((int,)),
        'n_year':            _calc((int,)),
        'n_tariff':          _not_null((float,)),
        'n_tax':             _not_null((float,)),
        'b_tax_inside':      _not_null((bool, 1)),
        'f_sale_accounts_1': _null((int,)),
        'f_taxes':           _not_null((int,)),
        'n_amount':          _calc((float,)),
        'n_tax_amount':      _calc((float,)),
        })

    #constructors
    def __init__(self, shell, use_node_cache=False):
        '''Базовый конструктор. Дополнительно передаем куда результат вставить'''
        super().__init__(shell, use_node_cache)
        #иницализация объекта с результатом расчета
        self.__init_result(shell)

    def __init_result(self, shell):
        if not hasattr(shell, 'result') or shell.result is None:
            self._details = []

    #helpers
    # события расширения расчета
    # Шапка расчета
    def _cb_init(self, tree_node):
        '''Запуск расчета'''
        # проверка кеша
        assert isinstance(self.context['dict_map'], dict), 'Кеш не проинициализирован'
        #вход в корневой узел
        if self.context.enter_child_context(tree_node, key='sd_division') == self.context.NEW:
            self._init_details([('f_division', 'link')], None, None, tree_node)
        # обработка тарифов
        tariffs = self.context.enter_subnode(tree_node, 'fs_tariff')
        entity = self.context.get_entity()
        if tariffs and entity:
            self._shell.dict_hook({'fs_tariff': tariffs}, entity)

    def _cb_complete(self, tree_node):
        '''Конец расчета'''
        pass

    def _cb_error(self, tree_node, error):
        '''ошибка в гланом обработчике'''
        self.log_tree_node_error('Ошибка в главном обработчике', tree_node=tree_node, error=error)
    # Конец - шапка расчета

    def _cb_conn_point_before(self, tree_node):
        '''Перед началом обработки объекта'''
        # обработка объекта
        if self.context.enter_child_context(tree_node) == self.context.NEW:
            self._init_details([('f_conn_points', 'link')], None, None, tree_node)

    def _cb_contract_squares_before(self, tree_node):
        '''Перед началом обработки площади'''
        if self.context.enter_child_context(tree_node, common=True) == self.context.NEW:
            self._init_details(['n_square', 'f_prop_forms'], 'd_date', 'd_date_end',
                               tree_node)


    def _cb_conn_point_sub_before(self, tree_node):
        '''Перед началом обработки помещения объекта'''
        if self.context.enter_child_context(tree_node) == self.context.NEW:
            self._init_details([('f_conn_points_sub', 'link')], None, None, tree_node)


    def _cb_subscr_before(self, tree_node):
        '''Перед началом обработки ЛС'''
        if self.context.enter_child_context(tree_node) == self.context.NEW:
            self._init_details([('f_subscr', 'link')], 'd_date_begin', 'd_date_end',
                               tree_node)

    def _cb_registr_pts_before(self, tree_node):
        '''Перед началом обработки УП'''
        if self.context.enter_child_context(tree_node) == self.context.NEW:
            self._init_details([('f_registr_pts', 'link'), 'f_network_pts', 'f_sale_items',
                                'f_balance_types', 'f_energy_levels'],
                                'd_date_begin', 'd_date_end', tree_node)

        # получаем услугу
        subnode_name = 'fs_sale_items'
        sub_node = self.context.get_dict_record('fs_sale_items', tree_node['f_sale_items'])
        if not sub_node:
            raise ProcException('Не найдена услуга: %s', tree_node['f_tariff'])
        # перед началом действий сохраняем в текуший контекст
        self.context['path'] = self.context.path
        self.context['type'] = self.context.type
        self.context.path += '.' + subnode_name
        self.context.type = subnode_name

        if self.context.enter_child_context(sub_node, common=True) == self.context.NEW:
            self._init_details(['f_units', 'n_precision', ('c_sale_items', 'c_const')],
                               None, None, sub_node)
        self.context.leave_child_context(sub_node)

    def _cb_registr_pts_activity_before(self, tree_node):
        '''Перед началом обработки активности УП'''
        if self.context.enter_child_context(tree_node, common=True) == self.context.NEW:
            self._init_details([(('n_percent', 'n_rate'))], 'd_date', 'd_date_end', tree_node)

    def _cb_registr_pts_tariff_before(self, tree_node):
        '''Перед началом обработки тарифа УП'''
        if self.context.enter_child_context(tree_node, common=True) == self.context.NEW:
            self._init_details(['f_tariff'], 'd_date', 'd_date_end', tree_node)
        # получаем тариф
        subnode_name = 'fs_tariff'
        sub_node = self.context.get_dict_record(subnode_name, tree_node['f_tariff'])
        if not sub_node:
            raise ProcException('Не найден тариф: %s', tree_node['f_tariff'])
        # перед началом действий сохраняем в текуший контекст
        self.context['path'] = self.context.path
        self.context['type'] = self.context.type
        self.context.path += '.' + subnode_name
        self.context.type = subnode_name

        if self.context.enter_child_context(sub_node, common=True) == self.context.NEW:
            self._init_details(['f_units', 'f_taxes', 'f_sale_accounts_1', 'f_energy_levels', \
                                'b_tax_inside'], None, None, sub_node)
            # история тарифа
            for node in self.context.enter_subnode(sub_node, 'fs_tariff_history'):
                if self.context.enter_child_context(node, common=True) == self.context.NEW:
                    self._init_details(['f_time_zones', 'f_cons_zones', 'f_sale_accounts_1', 'n_tariff'], \
                                       'd_date_begin', 'd_date_end', node)
                self.context.leave_child_context(node)
            # /
            # история НДС
            for node in self.context.enter_subnode(sub_node, 'fs_tax_history'):
                if self.context.enter_child_context(node, common=True) == self.context.NEW:
                    self._init_details([('n_tax', 'n_value')], 'd_date_begin', 'd_date_end', node)
                self.context.leave_child_context(node)
            # /
        self.context.leave_child_context(sub_node)

    def _cb_calc_method_before(self, tree_node):
        '''Перед началом обработки УП выбранным методом расчета'''
        if self.context.enter_child_context(tree_node, common=True) == self.context.NEW:
            self._init_details(['f_calc_methods'], 'd_date', 'd_date_end', tree_node)

    # Только 1 метод расчета
    def _get_calc_method(self, detail):
        '''Получить функцию метод расчета УП'''
        return self._calc_square

    def _calc_square(self, detail):
        '''Расчет по площади'''
        assert detail and isinstance(detail, dict)
        self._check_required(detail, tuple, keys=['n_square', 'n_percent'])
        square = detail['n_square']
        percent = detail['n_percent']
        precision = detail.get('n_precision', 0)

        detail['n_cons'] = round(square * percent, precision)

    # обобщенный расчет
    def _proc_conn_point(self, tree_node):
        '''Обработка объекта'''
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

    def _proc_registr_pts_activity(self, tree_node):
        '''Обработка активности УП ЛС'''
        pass
    def _proc_registr_pts_tariff(self, tree_node):
        '''Обработка тарифов УП ЛС'''
        pass

    ########################## UNUSED #################################
    #неиспользуемые события
    def _cb_network_pts_before(self, tree_node):
        pass
    def _cb_device_before(self, tree_node):
        pass
    def _cb_meter_reading_before(self, tree_node):
        pass
    #не используемые обработчики
    def _proc_network_pts(self, tree_node):
        pass
    def _proc_contract_squares(self, tree_node):
        pass
    def _proc_device_pts(self, tree_node):
        pass
    def _proc_device(self, tree_node):
        pass
    def _proc_meter_reading(self, tree_node):
        pass
