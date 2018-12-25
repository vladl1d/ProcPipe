# -*- coding: utf-8 -*-
"""
Базовые типы, используемые для работы
@author: V-Liderman
"""
class t_dict(dict):
    '''Справочник с ключами в нижнем регистре'''
    def __init__(self, *args, **kwargs):
        '''Перегружаем стандартный конструктор'''
        super().__init__(*args, **kwargs)
        for key in self.keys():
            if isinstance(key, str) and key!=key.lower():
                self[key] = super().pop(key)
    def get(self, index, default=None):
        '''обертка вокруг dict'''
        if isinstance(index, str):
            index = index.lower()
        return super().get(index, default)
    def setdefault(self, index, default=None):
        '''обертка вокруг dict'''
        if isinstance(index, str):
            index = index.lower()
        return super().setdefault(index, default)
    def pop(self, index, default=None):
        '''обертка вокруг dict'''
        if isinstance(index, str):
            index = index.lower()
        return super().pop(index, default)
    def __getitem__(self, index):
        if isinstance(index, str):
            index = index.lower()
        return super().get(index, None)
    def __setitem__(self, index, value):
        if isinstance(index, str):
            index = index.lower()
        return super().__setitem__(index, value)
    def __contains__(self, item):
        if isinstance(item, str):
            item = item.lower()
        return super().__contains__(item)
#    def __repr__(self):
#        return super().__str__()
    def copy(self):
        return type(self)(super().copy())

class t_list(list):
    '''Справочник ведет себя как список, но позволяет индексировать значения по ключу'''
    def _get_key(self, node, default=None):
        try:
            if self.pk:
                if hasattr(node, self.pk):
                    val = getattr(node, self.pk)
                elif callable(self.pk):
                    val = self.pk(node)
                elif hasattr(node, 'get') and callable(node.get):
                    val = node.get(self.pk, default)
                else:
                    val = default
                return default if val is None else val
        except:
            pass
        return default
            
    def _set_key(self, value):
        if self.pk:           
            pk_val = self._get_key(value, None)
            if pk_val is not None:
                self.keys[pk_val] = value
        
    def __init__(self, iterable=None, pk=None):
        '''Перегружаем стандартный конструктор'''
        if iterable:
            super().__init__(iterable)
        self.pk = pk
        if pk:
            self.keys = t_dict({self._get_key(node, self.pk): node for node in self \
                         if self._get_key(node)})
        else:
            self.keys = t_dict()
    def get(self, pk_val, default=None):
        '''Возвращает запись по ключу'''
        if self.pk:
            return self.keys.get(pk_val, default)
        return default
    def __setitem__(self, index, value):
        self._set_key(value)
        return super().__setitem__(index, value)
    def __delitem__(self, index):
        if self.pk:
            value = self[index]
            pk_val = self._get_key(value, None)
            self.keys.pop(pk_val, None)
        return super().__setitem__(index)
    def append(self, value):
        self._set_key(value)
        return super().append(value)
    def pop(self, index):
        if self.pk:
            if not isinstance(index, list):
                index = [index]
            for itm in index:
                value = self[itm]
                pk_val = self._get_key(value, None)
            self.keys.pop(pk_val, None)
        return super().pop(index)
    def copy(self):
        ret =  type(self)(super().copy())
        ret.keys = self.keys.copy()
        return ret
#    def __repr__(self):
#        return super().__str__()


