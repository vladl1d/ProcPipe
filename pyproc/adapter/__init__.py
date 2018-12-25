# -*- coding: utf-8 -*-
""" Адаптер для работы с БД. 
Содержит классы:
	* для представления запроса в виде дерева данных
	* преобразования дерева в SQL
	* управления соединениями и исполнения запросов
"""

__all__ = ['DataAdapter', 'QGraph', 'IQueryBuilder', 'QueryBuilderNestedSelect', 'QueryBuilderNestedFrom']

from .adapter import DataAdapter
from .qgraph import QGraph
from .qbuilder import IQueryBuilder, QueryBuilderNestedSelect, QueryBuilderNestedFrom
