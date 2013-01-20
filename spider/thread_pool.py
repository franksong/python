#!/usr/bin/python
#-*- coding: utf-8 -*-
# Author: Frank

import Queue, threading
from threading import Thread
import time
import logging
import sys

class ThreadWork(Thread):
    """
    """
    timeout = 1
    def __init__(self, workQueue, resultQueue, **argv):
        """
        
        Arguments:
        - `workQueue`:
        - `resultQueue`:
        - `**argv`:
        """
        Thread.__init__(self, **argv)
        
        self.setDaemon(True)
        self._workQueue = workQueue
        self._resultQueue = resultQueue
        self.start()

    def run(self):
        """
        
        Arguments:
        - `self`:
        """
        while True:
            try:
                callfunc, args, argv = self._workQueue.get(timeout = \
                                                               ThreadWork.timeout)
                result = callfunc(*args, **argv)
                self._resultQueue.put(result)
            except Queue.Empty:
                logging.info('threadpool work queue empty!')
                break
            except:
                logging.error('ThreadWork: %s', sys.exc_info())
                raise


class ThreadPoolManager:
    """
    线程池管理类
    """
    
    def __init__(self, num_of_threads = 10, timeout = 1):
        """
        """
        self.workQueue = Queue.Queue(0)
        self.resultQueue = Queue.Queue(0)
        self.threads = []
        self.timeout = timeout
        self.createThreads(num_of_threads)

    def createThreads(self, num_of_threads):
        """
        
        Arguments:
        - `self`:
        - `num_of_threads`:
        """
        for i in range(num_of_threads):
            thread = ThreadWork(self.workQueue, self.resultQueue)
            self.threads.append(thread)

    def wait_for_complete(self):
        """
        
        Arguments:
        - `self`:
        """
        while len(self.threads):
            thread = self.threads.pop()
            thread.join()
            if thread.isAlive() and not self.workQueue.empty():
                self.threads.append(thread)
        logging.info('All threads are completed.')

    def add_job(self, callfunc, *args, **argv):
        """
        
        Arguments:
        - `self`:
        - `callfunc`:
        - `*args`:
        - `**argv`:
        """
        self.workQueue.put( (callfunc, args, argv) )
        logging.info('threadpool addjob: %s, %s', args, argv)
