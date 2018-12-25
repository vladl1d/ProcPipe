# -*- coding: utf-8 -*-
"""
Обертка для потокового бинарного чтения из любого итератора.
Created on Mon Oct 29 15:52:11 2018
@author: V-Liderman
"""
import io

class JsonStream(io.BufferedIOBase):
    '''
    Класс для чтения JSON в виде потока байтов
    '''
    def __init__(self, cursors, encoding='utf-8', get_value_cb=None):
        '''
        Чтение данных из буфера
        '''
#        #итератор курсоров
#        self.itr_cursors = iter(cursors)
#        #текущий курсор
#        self.cursor = next(self.itr_cursors, None)
        self.encoding = encoding
        if cursors:#self.cursor:
            self.cursor = iter(cursors)#iter(self.cursor)
        if get_value_cb:
            self.get_value_cb = get_value_cb
        else:
            self.get_value_cb = (lambda x: str(x).encode(encoding) if x else None)
        self.buffer = None

        super(JsonStream, self).__init__()

    def _next_cursor(self):
        '''
        Список курсоров - итератор
        '''
        if self.cursor:
            value = next(self.cursor, None)
            if value is None:
                if hasattr(self.cursor, 'commit') and callable(self.cursor.commit):
                    self.cursor.commit()
                self.cursor = None
#                self.cursor = next(self.itr_cursors, None)
#                if self.cursor:
#                    self.cursor = iter(self.cursor)
#                return self._next_cursor()
            else:
                return value
        return None

    def _read_iter(self, size=0, max_size=-1):
        ''' Рекурсивное чтение данных из итератора. Читает сколько указано в буфере
        '''
        if self.buffer:
            data = self.buffer
            self.buffer = None
        else:
            data = self.get_value_cb(self._next_cursor())
        _len = len(data) if data else 0
        size += _len
        if data and (size <= max_size or max_size == -1):
            next_data = self._read_iter(size, max_size)
            if next_data:
                data += next_data
            return data
        if data and size > max_size:
            self.buffer = data[max_size:]
            return data[:max_size]
        return data

    def read(self, size=-1):
        '''
        Чтение данных из буффера
        '''
        data = self._read_iter(max_size=size)
        return data
