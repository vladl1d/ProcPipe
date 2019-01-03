# -*- coding: utf-8 -*-
"""
Обработка контекста при обходе узлов дерева json
@author: V-Liderman
"""
from .types import ProcException

class Context():
    '''Конекст необходим для рекурсивоного обхода дерева с данными
        Контекст содержит кэш элементов, который используется если дереро не сгруппировано
        по элементам от вершины к узлам, а просто содержит иерархическое представление плоских
        записей.
        Контекст содержит массив строк расчета родительского узла и результат декартового
        произведения с учетом интервалов текущего уровня потомков'''

     # тег, под которым записывается в дерево контекст, если работаем с обычным json
    __Context = '__Context'
    #__Parent= '__Parent'
    # При загрузке контекста обнаружили новый узел, которого нет в кеше
    NEW = True

    def __init__(self, context=None, use_node_cache=False):
        '''Конструктор'''
        assert isinstance(context, dict), 'Неверный тип записи'
        self.context = context
        self.use_node_cache = use_node_cache
        #текущий тип (схема) элемента
        self.type = None
        #текущий путь
        self.path = '$'
        #иницализируем конетекст
        self.context.update({
            #текущий тип. Используется для построения ключа кеша (копия)
            'type': None,
            #Оригинальные родительские строки
            'parent_details': list(),
            #Строки после пересечения с текущим уровнем
            'node_details': list(),
            #Родитель. Нужно для восстановления контекста при выходе из узла
            'parent': None,
            #Текущий узел. Нужно для восстановления контекста при выходе из узла
            'node': None,
            #название поля с текущий первичным ключом
            'key': dict()
            })

        if not context:
            self.context.update({
                # текущий путь (копия)
                'path': '$',
                #кеш элементов. Ключ - (тип, PK). Кеш будет глобальный в shell
                'context_cache': dict(),
                #схема элементов
                'schema': dict(),
                'schema_map': dict()
                })
        else:
            self.path = context['path']

    @staticmethod
    def assert_requred(value, message):
        '''Проверка параметра на обязательность'''
        if not value:
            if callable(message):
                message = message()
            raise ProcException(message)

    def check_tree_node_type(self, tree_node, _type, msg=None):
        '''Проверка типа узла для последующей обработки'''
        if not msg:
            #чтобы вычислять выражение только в случае ошибки
            msg = lambda: 'Неверный тип объекта %s[%s]' % (self.context['type'], \
                                                  self.get_entity_PK(tree_node))
        self.assert_requred(isinstance(tree_node, _type) and tree_node, msg)

    def enter_subnode(self, tree_node, subnode_name):
        '''Входит в дочерний узел с проверкой'''
        #проверяем в то что в узел можно входить
        subnode_name = subnode_name.lower()
        self.check_tree_node_type(tree_node, dict)
        entity = self.context['type']
        if not subnode_name in tree_node:
            pk = self.get_entity_PK(tree_node)
            raise ProcException('Не найден узел %s в %s[%s]' % (subnode_name, entity, pk))
        else:
            subnode = tree_node[subnode_name]
            err_msg = lambda: 'Пустой список потомков %s узла %s[%s]' % (subnode_name, entity, \
                                                  self.get_entity_PK(tree_node))
            self.check_tree_node_type(subnode, list, err_msg)

            # перед началом действий сохраняем в текуший контекст
            self.context['path'] = self.path
            self.context['type'] = self.type
            self.path += '.' + subnode_name
            self.type = subnode_name

            return subnode

    def get_entity_PK(self, tree_node, key=None):
        '''Получение первичного ключа записи'''
        #Обработка с учетом схемы
        assert isinstance(tree_node, dict), 'Неверный тип записи'
        entity, key = None, None
        if not key:
            if self.context['schema']:
#                path = tree_node.path
#                path = '.' + ('/'.join(path) if path else '')
                entity = self.context['schema_map'].setdefault(self.context['path'], \
                                     self.context['schema'].query(self.context['path']))
                if entity and entity.PK:
                    key = entity.PK[0]
            else:
                key = 'link'

        pk = tree_node.get(key, None)
        if entity:
            key = entity.name
        else:
            key = id(tree_node)

        return (key, pk)

    def restore_context(self, tree_node, pk=None, key=None):
        '''Восстанавливает контекст по РК'''
        if not pk:
            pk = self.get_entity_PK(tree_node, key=key)
        return self.context['context_cache'].get(pk, None)

    def save_context(self, tree_node, key=None):
        '''Сохраняет контекст по РК'''
        pk = self.get_entity_PK(tree_node, key=key)
        self.context['context_cache'][pk] = self.context

    def enter_child_context(self, tree_node, common=False, key=None):
        ''' Начало нового уровня по иерархии данных. Аналог start_map
        common - братья разделяют общий контекст родителя
        key - название PK сущности'''
#        assert isinstance(self.context, dict)        
        #если не передали ключ пытаемся его иницилизировать самостоятельно
        if not key:
            key = self.context['key'].get(id(tree_node), self.type)
        # Если дерево не иерархия - я структурированная плоская таблица, используем кеш,
        # иначе сохраняем контекст в узле
        _context = None
        if self.use_node_cache:
            _context = self.restore_context(tree_node, key=key)
        else:
            _context = tree_node.get(self.__Context, None)
        #Если нет записанного контекста - заново его инициализируем
        if not _context:
            # каждый узел хранит свою копию контекста. Копируются только ключи справочника
            self.context = self.context.copy()
            # запоминаем контекст
            if self.use_node_cache:
                self.save_context(tree_node, key=key)
            else:
                tree_node[self.__Context] = self.context

            #запоминаем тип и путь контекста
            self.context['path'] = self.path
            self.context['type'] = key
            #Запоминаем родителя для восстановления контекста
            self.context['parent'] = self.context['node']
            self.context['node'] = tree_node
            self.context['common'] = common

            #конекст вышестоящего узла становится узлом родителя
            self.context['parent_details'] = self.context['node_details']
            #Строки после пересечения с текущим уровнем
            self.context['node_details'] = list()
            # ключ
            self.context['key'][id(tree_node)] = key
            return self.NEW
        else:
            # при разделяемомо контексте строки родителя наполяются строками ребенка
            if common:
                _context['node_details'] = self.context['node_details']
            self.context = _context
            self.path = _context['path'] 
            self.type = _context['type']
            return not self.NEW

    def leave_child_context(self, tree_node):
        ''' Конец уровня по иерархии данных. Возврат наверх. Аналог end_map'''
        #получаем поле с ключом
#        key = self.context['key'].get(id(tree_node), None)
        # рассчитываем на передачу по ссылке
        # Если дерево не иерархия - я структурированная плоская таблица, используем кеш
#        if self.use_node_cache:
#            self.context = self.restore_context(tree_node, key=key)
#        else:
#            self.context = tree_node[self.__Context]
        # Теперь надо восстановить контекст родителя
        is_new = None
        if self.context['parent']:
            is_new = self.enter_child_context(self.context['parent'], common=self.context['common'])
        else:
            #Что мы наделали в узел кладем родителю
            self.context['parent_details'] = self.context['node_details']
            self.context['node_details'] = list()

        # освободим память
        tree_node.pop(self.__Context, None)
        self.context['key'].pop(id(tree_node), None)
        return is_new

    def clean_context(self):
        '''Убирает следы'''
        for itm in ['path', 'type', 'parent_details', 'node_details', 'parent', 'node', 'key']:
            self.context.pop(itm, None)
    def __getitem__(self, index):
        return self.context.get(index, None)
    def __setitem__(self, index, value):
        self.context[index] = value
    def __call__(self, **args):
        return self.context
    def __str__(self):
        return str(self())
