#!/usr/bin/python
#-*-coding:utf-8-*-

# Filename: spider3.py
# Author: Frank spf.doudou@gmail.com
# Usage: python spider3.py
# deep = 2 (default)

from thread_pool import *
from sgmllib import SGMLParser
import os, sys
import urllib2
import re
import sqlite3
import string
import logging
import chardet
import time
import threading
import Queue

class GetUrls(SGMLParser):
    '''
    解析HTML文档中的url
    '''
    def reset(self):
        self.urls = []
        SGMLParser.reset(self)
    def start_a(self, attrs):
        """
        
        Arguments:
        - `self`:
        - `attrs`:
        """
        url = [value for key, value in attrs if key == 'href']
        if url:
            self.urls.extend(url)

class OperatorSqlite():
    """
    包装操作sqlite3的函数作为类
    """
    
    def __init__(self, database = 'spider.db'):
        """
        
        Arguments:
        - `database`:
        """
        self.database = database
        self.conn = sqlite3.connect(self.database)
        self.conn.text_factory = str
        self.cur = self.conn.cursor()
    def __del__(self):
        """
        
        Arguments:
        - `self`:
        """
        self.conn.commit()
        self.cur.close()
        self.conn.close()
    def create_table(self, table = 'html'):
        """
        
        Arguments:
        - `self`:
        - `table`:
        """
        self.cur.execute('create table html (url TINYBLOB, info MEDIUMBLOB)')
    def insert(self, data, table = 'html'):
        """
        
        Arguments:
        - `self`:
        - `data`:
        - `table`:
        """
        self.cur.execute('insert into html values (?, ?)', data)


def get_urldata(url_link):
    """
    读取参数url指向的网页内容，解析出网页中的url备用，把网页内容和对应的url插入数据库
    表中
    """
    logging.info('Begin: %s', url_link)
    try:
        fd = urllib2.urlopen(url_link)
    except:
        logging.warning('wrong url: %s', url_link)
        return []
    content = fd.read()
    logging.debug('encodeing_info: %s', chardet.detect(content))
    #content = content.decode('gb2312').encode('utf-8')
    logging.info('Finished: %s', url_link)

    if argv_dict['--key']:
        if argv_dict['--key'] not in content:
            return []

    parser = GetUrls()
    try:
        parser.feed(content)
    except:
        logging.warning('can not parser HTML!')

    data = [url_link, content]
    lock.acquire()
    sql = OperatorSqlite(argv_dict['--dbfile'])
    try:
        sql.insert(data)
    except:
        logging.error('INSERT html data error!!!')
        print 'INSERT ERROR!!!'
        pass
    del sql
    lock.release()
    
    count_queue.put(url_link)
    return parser.urls

def do_spider(deep, url_list):
    """
    1、递归控制爬取深度
    2、启动线程池爬取网页
    3、去除已经爬取过的url
    """
    get_urls = []
    tpm = ThreadPoolManager(argv_dict['--thread'])
    for url in url_list:
        tpm.add_job(get_urldata, url)
    tpm.wait_for_complete()

    while tpm.resultQueue.qsize():
        data = tpm.resultQueue.get()
        get_urls.extend(data)
    deep = deep -1
    if deep <= 0:
        return 0
    result_urls = []
    for url in get_urls:
        if url not in url_list:
            result_urls.append(url)
            url_list.append(url)
    return do_spider(deep, result_urls)

def print_info():
    """
    定时（10s）在终端打印已经爬取网页数量
    """
    while True:
        time.sleep(10)
        if count_queue:
            print '已爬取 %d 个网页。。。' % count_queue.qsize()
            print
            continue
        sys.exit()

def init(argv_list):
    """
    处理命令行参数列表
    初始化日志系统
    Arguments:
    - `argv_list`:存储有所有命令行参数的列表
    """
    if len(argv_list) % 2 != 0:
        if '--testself' in argv_list:
            argv_list.remove('--testself')
            argv_dict['--testself'] = True
        else:
            print 'wrong args.'
            sys.exit()

    while len(argv_list):
        value = argv_list.pop()
        key = argv_list.pop()
        if not argv_dict.has_key(key):
            print 'wrong args!'
            sys.exit()
        argv_dict[key] = value

    argv_dict['-d'] = int(argv_dict['-d'])
    argv_dict['-l'] = int(re.search('[0-9]', argv_dict['-l']).group(0))
    argv_dict['--thread'] = int(argv_dict['--thread'])
    logging.basicConfig(filename = argv_dict['-f'], level = \
                            int(log_levels[argv_dict['-l']-1]))
    logging.info('Started:')

def main():
    """
    初始化数据库
    启动spider主线程和打印信息线程
    """
    init(argv_list)
    url_list.append(argv_dict['-u'])
    logging.debug(url_list)
    sql = OperatorSqlite(argv_dict['--dbfile'])
    sql.create_table()
    del sql
    count_thread = threading.Thread(target = print_info)
    count_thread.start()
    do_spider(argv_dict['-d'], url_list)
    logging.debug(argv_dict)
    logging.info('Finished\n')
    
if __name__ == '__main__':
    """
    定义一些全局变量
    启动main()函数
    """
    argv_dict = {
        '-u': 'http://www.qq.com',
        '-d': 2,
        '-f': 'spider.log',
        '-l': '4',
        '--thread': 10,
        '--dbfile': 'spider.db',
        '--key': False,
        '--testself': False
        }
    log_levels = ['50', '40', '30', '20', '10']
    argv_list = sys.argv[1:]
    print argv_list # test for output argv
    print
    url_list = []
    count_queue = Queue.Queue(0)
    global lock
    lock = threading.Lock()

    #if argv_dict['--testself']:
    #    import doctest
    #    doctest.testmod()

    main()
    count_queue = False
    print 'Done'
