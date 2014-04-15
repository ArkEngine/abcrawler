# -*- coding:utf-8 -*-
import MySQLdb, json
import os, hashlib, time
import logging
import sys
from constants import *

class linkdbLogic(object):
    def __init__(self, mysql_config):
        self.__nessary_list = [ \
                (type(''), "url"), \
                (type(10), "url_type"), \
                (type(10), "host_no"), \
                (type(10), "status_code"), \
                (type(10), "creat_time"), \
                (type(10), "check_time"), \
                (type(2**63), "refer_sign"), \
                (type(2**63), "url_sign"),]
        self.__db_linkdb_table = 'link_info'
        self.__db_hostno_table = 'host_info'
        self.__linkdb_output_list = ['url_no', 'url', 'url_type', 'host_no', 'status_code', \
                'creat_time', 'check_time', 'refer_sign', 'url_sign', ]
        self.__db_conn     = MySQLdb.connect(\
                host   = mysql_config['host'],\
                port   = mysql_config['port'],\
                user   = mysql_config['username'],\
                passwd = mysql_config['password'])
        self.__db_conn.autocommit(True)
        self.__db_conn.select_db(mysql_config['database'])

    def mysql_ping(self,):
        self.__db_conn.ping(True)

    def __is_valid(self, linkdb_item):
        ret = {}
        ret['retcode'] = 0
        ret['message'] = "OK"
        if not linkdb_item:
            ret['retcode'] = E_VALUE_NONE
            ret['message'] = 'linkdb_item is None'
            return ret
        for key_item in self.__nessary_list:
            if not linkdb_item.has_key(key_item[1]):
                ret['retcode'] = E_KEY_NOT_EXIST
                ret['message'] = 'field[%s] not exist' % (key_item[1], )
                return ret
            if type(linkdb_item[key_item[1]]) != key_item[0]:
                ret['retcode'] = E_VALUE_TYPE_ERROR
                ret['message'] = 'field[%s] type error. wish[%s] but t[%s] v[%s]' % (key_item[1],\
                    key_item[0], type(linkdb_item[key_item[1]]), linkdb_item[key_item[1]])
                return ret
            if None == linkdb_item[key_item[1]]:
                ret['retcode'] = E_VALUE_IS_EMPTY
                ret['message'] = 'field[%s] empty value' % (key_item[1], )
                return ret
        return ret

    def insert(self, linkdb_item):
        ret = {}
        ret['retcode'] = 0
        ret['message'] = "OK"

        check_ret = self.__is_valid(linkdb_item)
        if 0 != check_ret['retcode']:
            return check_ret
        linkdb4insert = {}
        for key in self.__nessary_list:
            linkdb4insert[key[1]] = linkdb_item[key[1]]

        field_list = self.__nessary_list
        field_key_list = ''
        field_value_list = ''
        
        for  field in field_list:
            field_key_list += '%s, ' % (field[1],)
            if field[0] == type(10):
                field_value_list += '%u, ' % (linkdb4insert[field[1]],)
            elif field[0] == type(1L):
                field_value_list += '%u, ' % (linkdb4insert[field[1]],)
            elif field[0] == type(''):
                field_value_list += '"%s", ' % (self.__db_conn.escape_string(linkdb4insert[field[1]]))
            else:
                assert 0
        field_key_list = field_key_list[:-2] + ' '
        field_value_list = field_value_list[:-2] + ' '

        strSQL = 'INSERT INTO %s (%s) VALUES (%s);' % (self.__db_linkdb_table, field_key_list, field_value_list,)
        print "sql:%s" % strSQL
        # -1- 插入linkdb表
        #print strSQL
        cursor = self.__db_conn.cursor()
        cursor.execute(strSQL)
        return ret

    def insert_batch(self, linkdb_item_list):
        ret = {}
        ret['retcode'] = 0
        ret['message'] = "OK"

        err_count = 0

        field_key_list = ''
        field_value_list = []
        is_field_key_list_done = False
        for linkdb_item in linkdb_item_list:
            check_ret = self.__is_valid(linkdb_item)
            if 0 != check_ret['retcode']:
                err_count += 1
                logging.warning("message: %s, jsonstr:%s" % (check_ret['message'], json.dumps(linkdb_item),))
            linkdb4insert = {}
            for key in self.__nessary_list:
                linkdb4insert[key[1]] = linkdb_item[key[1]]

            field_list = self.__nessary_list
            field_value = ''
            
            for  field in field_list:
                if not is_field_key_list_done:
                    field_key_list += '%s, ' % (field[1],)
                if field[0] == type(10):
                    field_value += '%u, ' % (linkdb4insert[field[1]],)
                elif field[0] == type(1L):
                    field_value += '%u, ' % (linkdb4insert[field[1]],)
                elif field[0] == type(''):
                    field_value += '"%s", ' % (self.__db_conn.escape_string(linkdb4insert[field[1]].replace('%', '％')),)
                else:
                    assert 0
            if not is_field_key_list_done:
                field_key_list = field_key_list[:-2] + ' '
            is_field_key_list_done = True

            field_value_list.append(field_value[:-2] + ' ')

        str_value_list = '),('.join(field_value_list)
        strSQL = 'INSERT INTO %s (%s) VALUES (%s);' % (self.__db_linkdb_table, field_key_list, str_value_list,)
        # -1- 插入linkdb表
        #print len(strSQL)
        cursor = self.__db_conn.cursor()
        cursor.execute(strSQL)
        ret['failnum'] = err_count
        return ret

    def update(self, linkdb_item):
        ret = {}
        ret['retcode'] = 0
        ret['message'] = "OK"
        if not linkdb_item.has_key('url_no'):
            ret['retcode'] = E_KEY_NOT_EXIST
            ret['message'] = "linkdb_item field [%s] not exist" % ('url_no', )
            return ret
        url_no = linkdb_item['url_no']

        field_list = self.__nessary_list
        key_value_list = ''
        for  field in field_list:
            if not linkdb_item.has_key(field[1]):
                continue
            key_value_list += '%s=' % (field[1],)
            if field[0] == type(10):
                key_value_list += '%u, ' % (linkdb_item[field[1]],)
            elif field[0] == type(''):
                key_value_list += '"%s", ' % (self.__db_conn.escape_string(linkdb_item[field[1]].replace('%', '％')),)
            else:
                assert 0
        key_value_list = key_value_list[:-2] + ' '

        strSQL = 'UPDATE %s SET %s WHERE url_no = %u;' % (self.__db_linkdb_table, key_value_list, url_no,)
        # -1- 插入linkdb表
        #print strSQL
        cursor = self.__db_conn.cursor()
        cursor.execute(strSQL)
        return ret

    def select_new_url(self, url_no_min, url_no_max):
        select_list = self.__linkdb_output_list
        select_key_string = ', '.join(select_list)
        strSQL = 'SELECT %s FROM %s WHERE url_no >= %u and url_no <= %u and check_time = 0;' %\
                (select_key_string, self.__db_linkdb_table, url_no_min, url_no_max,)
        cursor = self.__db_conn.cursor()
        count = cursor.execute(strSQL)
        db_result_list = cursor.fetchmany(count)
        output_list = []
        for x in db_result_list:
            output_item = {}
            for step in range(len(select_list)):
                output_item[select_list[step]] = x[step]
                #print select_list[step], '->', x[step]
            output_list.append(output_item)
        return output_list

    def select_url_sign(self, url_sign_list):
        select_list = ['url_no', 'url_sign']
        select_key_string = ', '.join(select_list)
        str_url_sign_list = ' OR '.join(['url_sign = %u' % x for x in url_sign_list])
        strSQL = 'SELECT %s FROM %s WHERE %s' % (select_key_string, self.__db_linkdb_table, str_url_sign_list,)

        cursor = self.__db_conn.cursor()
        count = cursor.execute(strSQL)
        db_result_list = cursor.fetchmany(count)
        output_list = []
        for x in db_result_list:
            output_item = {}
            for step in range(len(select_list)):
                output_item[select_list[step]] = x[step]
                #print select_list[step], '->', x[step]
            output_list.append(output_item)
        return output_list

    def insert_host_info(self, host):
        ret = {}
        ret['retcode'] = 0
        ret['message'] = "OK"

        strSQL = 'INSERT INTO host_info (host) VALUES ("%s");' % (host,)
        # -1- 插入linkdb表
        #print strSQL
        cursor = self.__db_conn.cursor()
        cursor.execute(strSQL)
        ret['host_no'] = self.__db_conn.insert_id()
        return ret

    def select_all_host(self):
        select_list = ['host_no', 'host']
        select_key_string = ', '.join(select_list)
        strSQL = 'SELECT %s FROM %s;' % (select_key_string, self.__db_hostno_table)

        cursor = self.__db_conn.cursor()
        count = cursor.execute(strSQL)
        db_result_list = cursor.fetchmany(count)
        output_list = []
        for x in db_result_list:
            output_item = {}
            for step in range(len(select_list)):
                output_item[select_list[step]] = x[step]
                #print select_list[step], '->', x[step]
            output_list.append(output_item)
        return output_list

    def select_url_by_sign(self, url_sign_list):
        select_list = ['url', 'url_sign']
        select_key_string = ', '.join(select_list)
        str_url_sign_list = ' OR '.join(['url_sign = %u' % x for x in url_sign_list])
        strSQL = 'SELECT %s FROM %s WHERE %s' % (select_key_string, self.__db_linkdb_table, str_url_sign_list,)

        cursor = self.__db_conn.cursor()
        count = cursor.execute(strSQL)
        db_result_list = cursor.fetchmany(count)
        output_list = []
        for x in db_result_list:
            output_item = {}
            for step in range(len(select_list)):
                output_item[select_list[step]] = x[step]
                #print select_list[step], '->', x[step]
            output_list.append(output_item)
        return output_list

    def select_url_by_host(self, host_no, url_no, limit=100):
        select_list = self.__linkdb_output_list
        select_key_string = ', '.join(select_list)
        strSQL = 'SELECT %s FROM %s WHERE host_no = %u and url_no > %u limit %u;' \
                % (select_key_string, self.__db_linkdb_table, host_no, url_no, limit)

        cursor = self.__db_conn.cursor()
        count = cursor.execute(strSQL)
        db_result_list = cursor.fetchmany(count)
        output_list = []
        for x in db_result_list:
            output_item = {}
            for step in range(len(select_list)):
                output_item[select_list[step]] = x[step]
                #print select_list[step], '->', x[step]
            output_list.append(output_item)
        return output_list

if __name__ == '__main__':
    url = sys.argv[1]
    head = 'http://'
    if not url.startswith(head):
        print 'url:%s must start with %s' % (url, head)
        sys.exit(1)
    host = url.replace(head, '')
    host = host.split('/')[0].split(':')[0]
    print 'host:%s' % (host)
    #sys.exit(1)
    mysql_config = {}
    mysql_config['username'] = 'crawler'
    mysql_config['password'] = 'pajk2014'
    mysql_config['database'] = 'wenda'
    mysql_config['host']     = '127.0.0.1'
    mysql_config['port']     = 3306
    dao = linkdbLogic(mysql_config)

    hret = dao.insert_host_info(host)

    linkdb_item = {}
    #linkdb_item['url_no'] = 1
    linkdb_item['url'] = sys.argv[1]
    linkdb_item['url_sign'] = long(hashlib.md5(linkdb_item['url']).hexdigest()[:16], 16) & 0x7fffffffffffffff
    linkdb_item['refer_sign'] = 0L
    linkdb_item['url_type'] = 0
    linkdb_item['host_no'] = int(hret['host_no'])
    linkdb_item['status_code'] = 0
    linkdb_item['creat_time'] = int(time.time())
    linkdb_item['check_time'] = 0
    #print type(linkdb_item['refer_sign'])
    print dao.insert(linkdb_item)
    #print dao.insert_batch([linkdb_item,linkdb_item])
    #print dao.select_new_url(1,1)
    #print dao.select_all_host()
    #lst = [1234569, 1234568, 123, 456, 789]
    #print dao.select_url_sign(lst)
