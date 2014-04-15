# -*- coding:utf8 -*-
from tornado.tcpserver import TCPServer
from tornado.ioloop  import IOLoop
import struct, sys
import json
import time
import logging
import hashlib
import utils, linkdbLogic

class Connection(object):
    def __init__(self, stream, address, my_linkdb_logic):
        self.FMT_XHEAD  = "I16sIIHHI"
        self.step       = 0

        self.log_id     = 0
        self.srvname    = ''
        self.fileno     = 0
        self.blockid    = 0
        self.piece1_len = 0
        self.status     = 0
        self.detail_len = 0
        self._stream = stream
        self._address = address
        self.linkdb_logic = my_linkdb_logic

        self.recv_message()

    def recv_message(self):
        if 0 == self.step:
            self._stream.read_bytes(36,  self.process_head)
        else:
            self._stream.read_bytes(self.detail_len,  self.process_detail)

    def process_head(self, data):
        self.step = 1
        self.log_id, self.srvname, self.fileno, self.blockid, self.status, self.piece1_len, self.detail_len = \
                struct.unpack(self.FMT_XHEAD, data)
        #print "fileno:%u blockid:%u piece1_len:%u detail_len:%u" \
        #        % (self.fileno, self.blockid, self.piece1_len, self.detail_len,)
        self.recv_message()

    def process_detail(self, data):
        # -1- 先更新该抓取页面的check_time
        assert len(data) == self.detail_len
        head_dict = json.loads(data[0:self.piece1_len - 1])
        #print head_dict
        update_item = {}
        update_item['url_no'] = head_dict['url_no']
        update_item['check_time'] = head_dict['timestamp']
        update_item['status_code'] = head_dict['Status-Code']
        ret = self.linkdb_logic.update(update_item)
        if ret['retcode'] != 0:
            logging.warning("update error. uno[%u] status_code[%u] check_time[%u] code[%u] mesg[%s] fno[%u] bid[%u]" \
                    % (update_item['url_no'], update_item['status_code'], update_item['check_time'], \
                    ret['retcode'], ret['message'], self.fileno, self.blockid))
        #f = open('data.o', 'w')
        #f.write(utils.unzipData(data[self.piece1_len:]))
        #f.close()
        #sys.exit(1)
        #log_message = "log_id[%u] p1l[%u] zlen[%u] delay[%u] jsonstr[%s]" \
        #        % (self.log_id, self.piece1_len, self.detail_len-self.piece1_len, \
        #        data[0:self.piece1_len - 1], int(time.time()) - head_dict['timestamp'])

        # -2- 分析页面，得到一系列扩展连接(只在域内扩展)
        assert self.detail_len >= self.piece1_len
        if (head_dict['Status-Code'] == 301 or head_dict['Status-Code'] == 302) and head_dict.has_key('Location'):
            pass
            # 跳转的都不要!
            #link_item = {}
            #try:
            #    link_item['refer_sign']  = long(hashlib.md5(head_dict['url']).hexdigest()[:16], 16) & long('7fffffffffffffff', 16)
            #except:
            #    rfurl = 'http://'+head_dict['host']
            #    link_item['refer_sign']  = long(hashlib.md5(rfurl).hexdigest()[:16], 16) & long('7fffffffffffffff', 16)
            #link_item['creat_time']  = head_dict['timestamp']
            #link_item['host_no']     = head_dict['host_no']
            #link_item['url_type']    = 0
            #link_item['status_code'] = 0
            #link_item['url']         = str(head_dict['Location'])
            #link_item['url_sign']  = long(hashlib.md5(link_item['url']).hexdigest()[:16], 16) & long('7fffffffffffffff', 16)
            #link_item['check_time']  = 0;

            ##print 'link_item["url_sign"]:', link_item['url_sign']
            #url_sign_retlist = self.linkdb_logic.select_url_sign([link_item['url_sign'],])
            #url_sign_set = set([x['url_sign'] for x in url_sign_retlist])
            #if link_item['url_sign'] not in url_sign_set:
            #    ret = self.linkdb_logic.insert(link_item)
            #    if ret['retcode'] != 0:
            #        logging.warning('redirection insert fail. retcode:%u message:%s status_code:%u jsonstr:%s fno:%u bid:%u' %\
            #                (ret['retcode'], ret['message'], head_dict['Status-Code'], data[0:self.piece1_len - 1], \
            #                self.fileno, self.blockid,))
        elif head_dict['Status-Code'] == 200 and self.detail_len > self.piece1_len:
            #print '-'*80
            t = time.time()
            try:
                page_content = utils.unzipData(data[self.piece1_len:])
                #print 'unzip time_cost: %.3f' % (time.time() - t,)
                t = time.time()
                url_set = utils.get_links(page_content, host = head_dict['host'], inhost = True, base_url = head_dict['url'])
                #print 'get_links time_cost: %.3f' % (time.time() - t,)
                #print 'url_set:', url_set, self.piece1_len, self.detail_len
                #print 'content-len:', len(page_content)
                #self.send_response()
                #sys.exit(1)
                try:
                    refer_sign = long(hashlib.md5(head_dict['url']).hexdigest()[:16], 16) & long('7fffffffffffffff', 16)
                    # 先查询url_sign是否已经存在于数据库中
                    url_sign_list = [long(hashlib.md5(url).hexdigest()[:16], 16) & long('7fffffffffffffff', 16) for url in url_set]

                    if url_sign_list:
                        t = time.time()
                        url_sign_retlist = self.linkdb_logic.select_url_sign(url_sign_list)
                        #print 'select_url_sign time_cost: %.3f' % (time.time() - t,)

                        t = time.time()
                        url_sign_set = set([x['url_sign'] for x in url_sign_retlist])
                    else:
                        url_sign_set = set([])
                    link_item_list = []
                    for url in url_set:
                        #if -1 != url.find('ActionData.aspx'):
                        #    logging.warning ('got it. %s from %s' % (url, head_dict['url']))
                        #    continue
                        link_item = {}
                        link_item['url_sign']  = long(hashlib.md5(url).hexdigest()[:16], 16) & long('7fffffffffffffff', 16)
                        if link_item['url_sign'] in url_sign_set:
                            continue
                        else:
                            link_item['refer_sign']  = refer_sign
                            link_item['creat_time']  = head_dict['timestamp']
                            link_item['host_no']     = head_dict['host_no']
                            link_item['check_time']  = 0
                            link_item['url_type']    = 0
                            link_item['status_code'] = 0
                            link_item['url'] = str(url)
                            #print link_item
                            link_item_list.append(link_item)
                    if link_item_list:
                        #print len(link_item_list), head_dict['url']
                        ret = self.linkdb_logic.insert_batch(link_item_list)
                        if ret['retcode'] != 0:
                            logging.warning("insert error. code[%u] mesg[%s] fno[%u] bid[%u]" %\
                                    (ret['retcode'], ret['message'], self.fileno, self.blockid))

                        log_message = "log_id[%u] p1l[%u] zlen[%u] delay[%u] all[%u] err[%u] jsonstr[%s] time_cost[%0.3f] fno[%u] bid[%u]" \
                                % (self.log_id, self.piece1_len, self.detail_len-self.piece1_len, \
                                int(time.time()) - head_dict['timestamp'], len(link_item_list), \
                                ret['failnum'], data[0:self.piece1_len - 1], time.time() - t, self.fileno, self.blockid)
                    else:
                        log_message = 'link_item_list empty. uno[%u] host[%s] delay[%u] p1l[%u] dlen[%u] url_count[%u] filted to zero. fno:%u bid:%u' \
                                % (head_dict['url_no'], head_dict['host'], int(time.time()) - head_dict['timestamp'], self.piece1_len, \
                                self.detail_len, len(url_set), self.fileno, self.blockid)
                    logging.info(log_message)
                    #print 'insert time_cost: %.3f' % (time.time() - t,)

                    #charset_start = page_content.find('charset=')
                    #charset_end   = page_content[charset_start:].find('"')
                    #charset = page_content[charset_start+8 : charset_start+charset_end]
                except:
                    logging.warning('shit happens in url. jsonstr:%s fno[%u] bid[%u]' % \
                        (data[0:self.piece1_len - 1], self.fileno, self.blockid,))
            except:
                logging.warning('unzip fail jsonstr:%s fno:%u bid:%u' % (data[0:self.piece1_len - 1], self.fileno, self.blockid,))
        else:
            logging.warning('shit happens. jsonstr:%s fno[%u] bid[%u]' % (data[0:self.piece1_len-1],self.fileno,self.blockid,))
            pass
            # TODO got some error
            #sys.exit(1)
        self.send_response()
    def send_response(self):
        sbuf = struct.pack(self.FMT_XHEAD, self.log_id, "link_db_updater", 0, 0, 0, 0, 0)
        self._stream.write(sbuf)
        self.detail_len = 0
        self.piece1_len = 0

        self.step = 0
        self.recv_message()

class appServer(TCPServer):
    def __init__(self, mysql_config):
        TCPServer.__init__(self,)
        self.__linkdb_logic = linkdbLogic.linkdbLogic(mysql_config)
    def handle_stream(self, stream, address):
        self.__linkdb_logic.mysql_ping()
        Connection(stream, address, self.__linkdb_logic)

if __name__ == '__main__':
    print "%s link_db_updater Server start ......" % (time.asctime(),)
    logger = logging.getLogger()
    hdlr = logging.FileHandler('./log/link_db_update.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    # load the config
    my_config = utils.load_config(sys.argv[1])
    server = appServer(my_config['mysql_config'])
    server.listen(my_config['server_config']['port'])
    IOLoop.instance().start()
