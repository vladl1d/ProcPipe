# -*- coding: utf-8 -*-
"""
Test cases для модуля JsonStream
Created on Fri Dec 21 09:43:04 2018

@author: V-Liderman
"""
import sys
import os
sys.path.append(os.path.dirname(__file__)+'/..')

from JsonStream import JsonParser, JsonStream, JsonTree
from Util import get_typed_value
from QGraph import QGraph
from datetime import datetime
from Cache import CacheHelper 


######################################## Tests ################################################
def __test__():
    json = '{"a":[1, {"b":2}]}'
    parser = JsonParser()
    fin = JsonStream([json])
    res = parser.parse(fin)
    print(res)
    print(parser.json.json())
    print(JsonTree.query(parser.json, './a[1]'))
#__test__()

x = 0
def __test1__():
    ''' abc
    '''    
    def _cb(node, parser, fout, **kwargs):
        global x
        if x is None: x = 0
        else: x += 1
#        root = parser.rebuild_root(node)
        fout.write('%s\t%s\t%s' % (str(datetime.now()), x, node.get('LINK', 0)))
        fout.write('\n')
        return True
    
    c = CacheHelper(r'C:\Users\v-liderman\Desktop')
    alias_map = None
    alias_map = c.pick_data('QGraph[]', 'APP_Calc_Subscr')
    with open(r'C:\Users\v-liderman\Desktop\t2.json', 'rb') as fin:
        with open(r'C:\Users\v-liderman\Desktop\out.json', 'w') as fout:
            #src = '{"key":[1,{"c":"d"}]}'
            #fout = io.StringIO()
            #json.dump(Q, fout)
            #src = fout.getvalue()
            #print('Source')
            #print(src)
            parser = JsonParser(callbacks={'SD_Subscr': _cb, \
                                           'SD_Conn_Points': lambda *k, **e: True}, \
                                encoding = 'windows-1251',  schema=alias_map[0], \
                                fout = fout)
            res = parser.parse(fin)
            print('Result')
            if res:
#                print(str(parser.json))
                pass
            else:
                print(parser.error)

__test1__()
from matplotlib import pyplot as plt
import numpy as np
import scipy.stats
def __test2__():
    tdata, i, _ = np.loadtxt(r'C:\Users\v-liderman\Desktop\out.json', delimiter='\t', dtype=object, \
                      converters={0: lambda x: get_typed_value(x, datetime),\
                                  1: lambda x: get_typed_value(x, float)}, unpack=True)
    #print(data[:100])
    fr = 1
    tdata1 = tdata[::fr]
    i1 = i[::fr]
    #вычиялем разницу
    max_i = tdata1.shape[0]
    new_data = np.zeros(max_i - 1)
    for idx in range(max_i):
        if idx < max_i - 1:
            new_data[idx] = (tdata1[idx+1] - tdata1[idx]).microseconds
    
    i1 = (i1/fr)[:-1]
    new_data = new_data/1000000
    
    plt.figure(1)
    plt.plot(i1, new_data)
    plt.figure(2)
    plt.hist(new_data)
    plt.show()
    print(scipy.stats.describe(new_data))

#__test2__()
    
