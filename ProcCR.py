# -*- coding: utf-8 -*-
"""
Класс прикладного расчета.
Created on Wed Nov 14 17:16:24 2018
@author: V-Liderman
"""
import sys
import os
from uuid import UUID
from pprint import pprint
__RUN_DIR__ = os.path.dirname(__file__)
sys.path.append(__RUN_DIR__)
from datetime import datetime
from Proc import IProc, ProcException, _null, _not_null, _calc
from JsonStream import JsonTree, JsonParser

class ProcCR(IProc):
    '''
    Имплементация расчета
    '''
    #class methods
    #Схема документа
    doc_schema = JsonTree.t_dict()
    #схема строки документа
    details_schema = JsonTree.t_dict({
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
        'f_conn_points':     _not_null((UUID,)),
        'f_conn_points_sub': _not_null((UUID,)),
        'f_network_pts':     _not_null((UUID,)),
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
        #'n_tariff':          _not_null((float,)),
        'f_sale_accounts_1': _null((float,)),
        'f_taxes':           _not_null((int,)),
        'n_amount':          _calc((float,)),
        'n_tax_amount':      _calc((float,)),
        })

    #constructors
    def __init__(self, shell):
        '''Базовый конструктор. Дополнительно передаем куда результат вставить'''
        super().__init__(shell)
        #иницализация объекта с результатом расчета
        self.__init_result(shell)

    def __init_result(self, shell):
        if not hasattr(shell, 'result') or shell.result is None:
            self._details = []

    #helpers
    # события расширения расчета
    #TODO: Вызвать родительский метод сначала
    # Шапка расчета
    def _cb_init(self, tree_node):
        '''Запуск расчета'''
        if self.enter_child_context(tree_node, key='F_Division') == self._New_Node:
            self._init_details([('F_Division', 'LINK')], None, None, tree_node)

    def _cb_complete(self, tree_node):
        '''Конец расчета'''
        pass

    def _cb_error(self, tree_node, error):
        '''ошибка в гланом обработчике'''
        self.log_tree_node_error('Ошибка в главном обработчике', tree_node=tree_node, error=error)
    # Конец - шапка расчета

    def _cb_conn_point_before(self, tree_node):
        '''Перед началом обработки объекта'''
        if self.enter_child_context(tree_node) == self._New_Node:
            self._init_details([('F_Conn_Points', 'LINK')], None, None, tree_node)

    def _cb_contract_squares_before(self, tree_node):
        '''Перед началом обработки площади'''
        if self.enter_child_context(tree_node, common=True) == self._New_Node:
            self._init_details(['N_Square', 'F_Prop_Forms'], 'D_Date', 'D_Date_End',
                               tree_node)


    def _cb_conn_point_sub_before(self, tree_node):
        '''Перед началом обработки помещения объекта'''
        if self.enter_child_context(tree_node) == self._New_Node:
            self._init_details([('F_Conn_Points_Sub', 'LINK')], None, None, tree_node)


    def _cb_subscr_before(self, tree_node):
        '''Перед началом обработки ЛС'''
        if self.enter_child_context(tree_node) == self._New_Node:
            self._init_details([('F_Subscr', 'LINK')], 'D_Date_Begin', 'D_Date_End',
                               tree_node)

    def _cb_registr_pts_before(self, tree_node):
        '''Перед началом обработки УП'''
        if self.enter_child_context(tree_node) == self._New_Node:
            self._init_details([('F_Registr_Pts', 'LINK'), 'F_Network_Pts', 'F_Sale_Items',
                                'F_Balance_Types', 'F_Energy_Levels'],
                                'D_Date_Begin', 'D_Date_End', tree_node)
        sub_node = self.enter_subnode(tree_node, 'FS_Sale_Items')
        if sub_node.type == JsonTree.t_list:
            sub_node = sub_node[0]
        if self.enter_child_context(sub_node, common=True) == self._New_Node:
            self._init_details(['F_Units', 'N_Precision', ('C_Sale_Items', 'C_Const')],
                               None, None, sub_node)
        self.leave_child_context(sub_node)

    def _cb_registr_pts_activity_before(self, tree_node):
        '''Перед началом обработки активности УП'''
        if self.enter_child_context(tree_node, common=True) == self._New_Node:
            self._init_details([(('N_Percent', 'N_Rate'))], 'D_Date', 'D_Date_End', tree_node)

    def _cb_registr_pts_tariff_before(self, tree_node):
        '''Перед началом обработки тарифа УП'''
        if self.enter_child_context(tree_node, common=True) == self._New_Node:
            self._init_details(['F_Tariff'], 'D_Date', 'D_Date_End', tree_node)
        sub_node = self.enter_subnode(tree_node, 'FS_Tariff')
        if sub_node.type == JsonTree.t_list:
            sub_node = sub_node[0]
        if self.enter_child_context(sub_node, common=True) == self._New_Node:
            self._init_details(['F_Units', 'F_Taxes', 'F_Sale_Accounts_1'], None, None, sub_node)
        self.leave_child_context(sub_node)

    def _cb_calc_method_before(self, tree_node):
        '''Перед началом обработки УП выбранным методом расчета'''
        if self.enter_child_context(tree_node, common=True) == self._New_Node:
            self._init_details(['F_Calc_Methods'], 'D_Date', 'D_Date_End', tree_node)

    # Только 1 метод расчета
    def _get_calc_method(self, detail):
        '''Получить функцию метод расчета УП'''
        return self._calc_square

    def _calc_square(self, detail):
        '''Расчет по площади'''
        assert detail and isinstance(detail, JsonTree.t_dict)
        self._check_required(detail, tuple, keys=['N_Square', 'N_Percent'])
        square = detail['N_Square']
        percent = detail['N_Percent']

        detail['N_Cons'] = square * percent

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
            except ProcException as error:
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

import logging, json
from Util import CustomEncoder
from Cache import CacheHelper 
class _t1:
    log = logging.getLogger(__name__)
    dict_cache = JsonTree.t_dict()
    result = None
def __test__():
    shell = _t1()
    alias_map = None
#    alias_map = CacheHelper(r'C:\Users\v-liderman\Desktop').pick_data('Alias_map', 'APP_Calc_Subscr')
    context = JsonTree.t_dict({'D_Date0' : '2018-10-01', 'D_Date1' : '2018-11-01'})
    with open(r'C:\Users\v-liderman\Desktop\t2.json', 'rb') as fin:
        with open(r'C:\Users\v-liderman\Desktop\result.json', 'w', encoding='utf-8') as fout:
            #data = json.load(fin)
            proc = ProcCR(shell)
            def _cb(node, parser, context): 
                proc.run(parser.rebuild_root(node), context)
                return True
                
            cbs = {'SD_Subscr' : _cb}
            parser = JsonParser(encoding='windows-1251', callbacks=cbs, context=context)
            if parser.parse(fin, alias_map=alias_map):
#                data = parser.json
                pass
#
#            proc.run(data, context)
            if proc.result:
                json.dump(proc.result, fout, cls=CustomEncoder)
                #proc.result.to_json(fout, orient='records')
#__test__()
