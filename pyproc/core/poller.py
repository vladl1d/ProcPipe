# -*- coding: utf-8 -*-
"""
Ассинхронное исполнение чего угодно. Содержит обертку вокруг очереди и управление потоками
Created on Fri Nov  9 08:08:17 2018
@author: V-Liderman
"""

#from __future__ import print_function
#from __future__ import unicode_literals

import multiprocessing
import threading
from six.moves import queue

#import logging

# We don't print for errors/warnings, we use Python logging system.
#logger = logging.getLogger("OmnisX")
# Avoid No handlers could be found for logger.
#logger.addHandler(logging.NullHandler())


#if six.PY2:
    # Under Python2 a permission denied error raises an OSError
    # with errno 13.
#    PermissionError = OSError

# ==============================================================================
class PollError(Exception):
    """For exceptions generated directly by Poll wrapper.
    """
    pass
# ==============================================================================
class Poller():
    """Keep a poll of Master objects for processing with different threads.

    Each poll manage a set of threads, able to do parallel processing, and a
    set of master objects, able to do (more real) parallel things.

    If you want to **properly terminate** a :class:`IPoll`, you must
    call its :func:`IPoll.stop_poll` method.

    .. note::

        Parallel processing via threads in Python within the same
        process is limited due to the global interpreter lock
        (Python's GIL).
    """
    def __init__(self, object_type, log, workers_count=None, keep_objects=True, \
                       max_queue_size=0, verbose=False, **kwargs):
        """
        Создает новый пул мастер-объектов для исполнения и очередь обработки сообщений
        """
        assert object_type, 'Не задан тип объекта для конвейерной обработки'

        if workers_count is None:
            workers_count = multiprocessing.cpu_count()
        assert workers_count > 0, 'Не правильное кол-во потоков'

        #if DEBUG_MULTITHREAD:
        #    logger.debug("Creating TaggerPoll, %d workers, %d taggers",
        #                 workerscount,taggerscount )
        self._stopping = False
        #Система логирования
        self.logger = log

        #очередь заданий
        self._wait_jobs = queue.Queue(max_queue_size if max_queue_size > 0 else 0)
        #мастер-объекты для исполнения заданий
        self.object_constructor = lambda: object_type(**kwargs)
        self.wait_objects = None
        if keep_objects:
            self.wait_objects = queue.Queue()
            self._build_objects(workers_count)

        # потоки для раздельного исполнения мастер-объектов
        self._workers = []
        self._build_workers(workers_count)

        #Счетчик ошибок в транзакции
        self.success = threading.Event()
        self.success.set()

        self.verbose = verbose

    def _build_objects(self, object_count):
        #if DEBUG_MULTITHREAD:
        #    logger.debug("Creating taggers for TaggerPoll")
        assert self.wait_objects, 'Очередь мастер-объектов не создана'
        for _ in range(object_count):
            obj = self.object_constructor()
            self.wait_objects.put(obj)

    def _build_workers(self, workerscount):
        #if DEBUG_MULTITHREAD:
        #    logger.debug("Creating workers for TaggerPoll")
        for _ in range(workerscount):
            thread = threading.Thread(target=self._worker_main)
            thread.daemon = True
            self._workers.append(thread)
            thread.start()

    def create_job(self, methname, callback, **kwargs):
        '''
        Создает новое задание для выполнения. Метод зависнет если достигнут максимальный размер
        очереди
        '''
        assert not self._stopping, 'Новое задание после окончания работы пула'
        job = Job(self, methname, kwargs, callback)
        if self.verbose:
            self.logger.debug("%s::Создано задание %d в очереди", type(self).__name__, id(job))
        self._wait_jobs.put(job)
        return job

    def _worker_main(self):
        #try:
        while True:
            #if DEBUG_MULTITHREAD:
#            self.logger.debug("%s::Поток ожидает задания %d" % (type(self).__name__, \
#                                                              threading.current_thread().ident))
            job = self._wait_jobs.get()  # Pickup a job.
            if job is None:
                #if DEBUG_MULTITHREAD:
                #    logger.debug("Worker finishing")
                break   # Put Nones in jobs queue to stop workers.
            #if DEBUG_MULTITHREAD:
            #    logger.debug("Worker doing picked job %d", id(job))
            job.execute()                       # Do the job
            self._wait_jobs.task_done()
            if self.verbose:
                self.logger.debug("%s::Задание выполнено %d" % (type(self).__name__, id(job)))


    def stop_poll(self):
        """
        Останавливает потоки и удаляет мастер-объекты
        """
        #if DEBUG_MULTITHREAD:
        #    logger.debug("TaggerPoll stopping")
        if not self._stopping:          # Just stop one time.
        #    if DEBUG_MULTITHREAD:
        #        logger.debug("Signaling to threads")
            self._stopping = True       # Prevent more Jobs to be queued.
            # Put one None by thread (will awake threads).
            if hasattr(self, '_workers') and hasattr(self, '_wait_jobs'):
                for _ in range(len(self._workers)):
                    self._wait_jobs.put(None)

        # Remove refs to threads.
        if hasattr(self, '_workers'):
            # Wait for threads to be finished.
            for thread in self._workers:
                #if DEBUG_MULTITHREAD:
                #    logger.debug("Signaling to thread %s (%d)", th.name, id(th))
                thread.join()

            del self._workers
        # Remove references to master objects.
        if hasattr(self, 'wait_objects'):
            del self.wait_objects
        #if DEBUG_MULTITHREAD:
        #    logger.debug("TaggerPoll stopped")
    def wait_finished(self):
        '''Ожидание завершения всех потоков'''
        self._wait_jobs.join()
    def __del__(self):
        self.stop_poll()


class Job():
    """Задание для асинхронного выполнения
    """
    def __init__(self, poll, methname, kwargs, callback):
        self._poll = poll
        self._methname = methname
        self._kwargs = kwargs
        self._event = threading.Event()
        self._result = None
        self._callback = callback

    def execute(self):
        '''
        Выполнение задания
        '''
        # Забираем мастер-объект.
        #logger.debug("Job %d waitin for a tagger", id(self))
        if self._poll.wait_objects:
            obj = self._poll.wait_objects.get()
        else:
            obj = self._poll.object_constructor()
        #if DEBUG_MULTITHREAD:
        #    logger.debug("Job %d picked tagger %d for %s", id(self),
        #                 id(tagger), self._methname)
        # протаскиваем метод
        try:
            meth = getattr(obj, self._methname)
            self._result = meth(**self._kwargs)
        except Exception as error:
            #if DEBUG_MULTITHREAD:
            self._poll.success.clear()
            self._poll.logger.exception("%s::Задание %d закончилось исключением", \
                                       type(self._poll).__name__, id(self))
            self._result = error
        # Освобождаем мастер-объект.
        #if DEBUG_MULTITHREAD:
        #    logger.debug("Job %d give back tagger %d", id(self),
        #                 id(tagger))
        if self._poll.wait_objects:
            self._poll.wait_objects.put(obj)
        else:
            del obj
        #устанавливаем признак "конец"
        self._event.set()
        #вызов callback
        if self._callback:
            self._callback(self._result, **self._kwargs)

        #if DEBUG_MULTITHREAD:
        #    logger.debug("Job %d finished", id(self))

    @property
    def finished(self):
        '''
        Свойство: конец выполнения задания
        '''
        return self._event.is_set()

    def wait_finished(self, timeout=None):
        """Lock on the Job event signaling its termination.
        """
        self._event.wait(timeout)

    @property
    def result(self):
        '''
        Результат выполнения задания
        '''
        return self._result
