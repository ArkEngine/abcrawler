# -*- coding:utf-8 -*-
import MySQLdb, json
import os, hashlib, time
import logging
import sys
import linkdbLogic
from constants import *

if __name__ == '__main__':
    url = sys.argv[1]
    head = 'http://'
    if not url.startswith(head):
        print 'url:%s must start with %s' % (url, head)
        sys.exit(1)
    host = url.replace(head, '')
    host = host.split('/')[0].split(':')[0]
    #print 'host:%s' % (host)
    #sys.exit(1)
    mysql_config = {}
    mysql_config['username'] = 'worker'
    mysql_config['password'] = 'worker'
    mysql_config['database'] = 'yaopin'
    mysql_config['host']     = '127.0.0.1'
    mysql_config['port']     = 3306
    dao = linkdbLogic.linkdbLogic(mysql_config)

    host_list = dao.select_all_host()
    host_info_dict = {}
    host_no = 0
    for x in host_list:
        host_info_dict[x['host']] = x['host_no']

    if host_info_dict.has_key(host):
        host_no = host_info_dict[host]
        print 'old host:%s host_no:%u' % (host, host_no)
    else:
        hret = dao.insert_host_info(host)
        host_no = int(hret['host_no'])
        print 'new host:%s host_no:%u' % (host, host_no)

    url_sign = long( hashlib.md5( url ).hexdigest()[:16], 16 ) & 0x7fffffffffffffff
    ret = dao.select_url_sign([url_sign,])
    if ret == []:
        print 'new seed', 
        linkdb_item = {}
        linkdb_item['url'] = sys.argv[1]
        linkdb_item['url_sign'] = long(hashlib.md5(linkdb_item['url']).hexdigest()[:16], 16) & 0x7fffffffffffffff
        linkdb_item['refer_sign'] = 0L
        linkdb_item['url_type'] = 0
        linkdb_item['host_no'] = int(host_no)
        linkdb_item['status_code'] = 0
        linkdb_item['creat_time'] = int(time.time())
        linkdb_item['check_time'] = 0
        print dao.insert(linkdb_item)
    else:
        print 'url already in link_info table,', ret
        #print type(linkdb_item['refer_sign'])
    #linkdb_item = {}
    ##linkdb_item['url_no'] = 1
    #linkdb_item['url'] = sys.argv[1]
    #linkdb_item['url_sign'] = long(hashlib.md5(linkdb_item['url']).hexdigest()[:16], 16) & 0x7fffffffffffffff
    #linkdb_item['refer_sign'] = 0L
    #linkdb_item['url_type'] = 0
    #linkdb_item['host_no'] = int(hret['host_no'])
    #linkdb_item['status_code'] = 0
    #linkdb_item['creat_time'] = int(time.time())
    #linkdb_item['check_time'] = 0
    ##print type(linkdb_item['refer_sign'])
    #print dao.insert(linkdb_item)
    ##print dao.insert_batch([linkdb_item,linkdb_item])
    ##print dao.select_new_url(1,1)
    ##print dao.select_all_host()
    ##lst = [1234569, 1234568, 123, 456, 789]
    ##print dao.select_url_sign(lst)
