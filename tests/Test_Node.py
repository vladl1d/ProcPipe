# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 11:03:58 2018

@author: V-Liderman
"""

import sys
import os
__BASE_DIR = os.path.dirname(__file__)+'/..'
sys.path.append(__BASE_DIR)

from Node import ProcShell
from JsonStream import JsonTree
def __test__(config=None):
    shell = ProcShell(debug=True, base_dir = __BASE_DIR)#dba_cfg_dict=config)
    reader = None
#    reader = shell._data_adapter.fetch_to_dict
    #rec = shell._get_record('APP_Fetch_Next_Batch', {'@Node_id':1})#, reader)
    rec = shell._data_adapter.get_record('APP_Calc_Subscr', {'@id': 389572, '@batch': 0}, reader)
    if isinstance(rec, JsonTree):
        rec = rec.json()
#    with open(r'C:\Users\v-liderman\Desktop\result2.json', 'w', encoding='utf-8') as fout:
#        json.dump(rec, fout, cls=CustomEncoder)
#        shell._data_adapter.execute('APP_Calc_Subscr', fout,  \
#                     param_values = {'@id':389572, '@batch':0}, verbose=True)

    #param_values=shell.context_param_values
    #shell._push_record('Text', 'APP_Append_Log', param_values)
#    shell._run_poll(1)
    #shell.run()
#    param_values = {'@id':389572, '@batch':0}
#    shell._new_job(param_values)
    shell.stop()

__test__()
