# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 11:53:07 2018

@author: V-Liderman
"""
import sys
import os
sys.path.append(os.path.dirname(__file__)+'/..')

import logging, json
from Util import CustomEncoder
from JsonStream import JsonTree, JsonParser 
from ProcCR import ProcCR
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
__test__()
