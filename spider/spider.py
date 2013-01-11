#!/usr/bin/python
#-*-coding:utf-8-*-

# Filename: spider.py
# Author: Frank
# Usage: python spider.py
# deep = 2

from thread_pool import *
from sgmllib import SGMLParser
import os, sys
import urllib2
import re
import sqlite3
import string
import logging
import chardet
import MySQLdb

#sys.setdefaultencoding('utf-8')

class GetUrls(SGMLParser):
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

def do_get_content(url_link):
    logging.info('Begin: %s', url_link)
    try:
        fd = urllib2.urlopen(url_link)
    except:
        logging.warning('wrong url: %s', url_link)
        return ''
    content = fd.read()
    logging.debug('encodeing_info: %s', chardet.detect(content))
    #content = content.decode('gb2312').encode('utf-8')
    logging.info('Finished: %s', url_link)
    return content

def html_text(html):
    """
    
    Arguments:
    - `html`:
    """
    html = string.replace(html, '“', "'")
    html = string.replace(html, '"', "'")
    html = string.replace(html, '；', ";")
    html = string.replace(html, '/', '&quot')
    html = string.replace(html, '%', '&percent')
    return html

def save_parser(data, sql):
    """
    
    Arguments:

    - `data`:
    """
    #filename = '/home/frank/mywork/html/qq.html'
    parser = GetUrls()
    try:
        parser.feed(data)
    except:
        logging.warning('can not parser HTML!')
    #f = open(filename, 'a')
    #f.write(data)
    #f.write('Finished')
    #f.close()
    #text = html_text(data)
    text = (data,)
    #statement = 'INSERT INTO html VALUES (?)', text
    try:
        sql.execute('INSERT INTO html (data) VALUES (?)',(data,))
    except:
        logging.error('INSERT html data error!')
        print 'INSERT ERROR!!!'
        pass
    return parser.urls
    

def do_get_con(deep, url_list, sql):
    get_urls = []
    tpm = ThreadPoolManager(argv_dict['--thread'])
    for url in url_list:
        tpm.add_job(do_get_content, url)

    tpm.wait_for_complete()
    while tpm.resultQueue.qsize():
        data = tpm.resultQueue.get()
        get_urls.extend(save_parser(data, sql))
    deep = deep -1
    if deep <= 0:
        return 0
    result_urls = []
    for url in get_urls:
        if url not in finished_urls:
            result_urls.append(url)
            finished_urls.append(url)
    return do_get_con(deep, result_urls, sql)

argv_dict = {
    '-u': 'http://www.qq.com',
    '-d': 3,
    '-f': 'spider.log',
    '-l': '4',
    '--thread': 10,
    '--dbfile': '/home/frank/mywork/html/spider.db',
    '--key': False,
    '--testself': False
    }
#log_levels = ['logging.CRITICAL', 'logging.ERROR', 'logging.WARNING', \
#                  'logging.INFO', 'logging.DEBUG']
log_levels = ['50', '40', '30', '20', '10']
argv_list = sys.argv[1:]
print argv_list # test for output argv

def init(argv_list):
    """
    
    Arguments:
    - `argv_list`:
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

def main():
    """
    """
    


logfile = argv_dict['-f']
log_level = log_levels[argv_dict['-l']-1]
logging.basicConfig(filename = logfile, level = int(log_level))
logging.info('Started')

url_list = []
url_list.append(argv_dict['-u'])
logging.debug(url_list)

global finished_urls
finished_urls = []
finished_urls.extend(url_list)

conn = sqlite3.connect(argv_dict['--dbfile'])
cur_sql = conn.cursor()
cur_sql.execute('CREATE TABLE html (data BLOB)')
do_get_con(argv_dict['-d'], url_list, cur_sql)
conn.commit()
conn.close()
logging.debug(argv_dict)
logging.info('Finished\n')

print 'Done'
