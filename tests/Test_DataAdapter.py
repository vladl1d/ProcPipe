# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 09:59:08 2018

@author: V-Liderman
"""

import sys
import os
sys.path.append(os.path.dirname(__file__)+'/..')
from DataAdapter import DataAdapter, QueryBuilderNestedSelect
import pyodbc

def __test__(DSN, config, driver=pyodbc):
    data = DataAdapter(DSN, '', driver, query_builder=QueryBuilderNestedSelect(shrink_names=False))
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

#    queries = pick_data('QGraph[]', 'APP_Calc_Subscr')
#    qb = QueryBuilderNestedSelect(shrink_names=True, sql_lang=None)
#    sql = qb.build_sql(queries[0])
#    print(sql)


    with open(r'C:\Users\v-liderman\Desktop\t2.json', 'w', encoding='utf-8') as file:
        data.execute('APP_Calc_Subscr', file,  \
                     param_values={'@id':389572, '@batch':0}, verbose=True)

#        c = CacheHelper(base_dir=r'C:\Users\v-liderman\Desktop')
#        c.dump_data(data.query_builder.alias_map, data_type='Alias_map', data_id='APP_Calc_Subscr')

# выполняем тест только для самостоятельного запуска
DSN = r'DRIVER={ODBC Driver 13 for SQL Server};SERVER=V-LIDERMAN\SQL2017;Trusted_Connection=Yes;Database=OmniUS;'
#    #DSN = r'postgresql://omnix:0.123@192.168.17.129:5432/omnix'
__test__(DSN, None)