# -*- coding:utf8 -*-
from tornado.tcpserver import TCPServer
from tornado.ioloop  import IOLoop
import struct, sys
import json
import time
import bson
import pymongo
import logging
import utils, pagedbLogic

class Connection(object):
    def __init__(self, stream, address, mongo_handler):
        self.FMT_XHEAD = "I16sIIHHI"
        self.step = 0
        self.log_id     = 0
        self.srvname    = ''
        self.fileno     = 0
        self.blockid    = 0
        self.piece1_len = 0
        self.status     = 0
        self.detail_len = 0
        self._stream = stream
        self._address = address
        self.pagedb_logic = mongo_handler

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
        #print "piece1_len:%u detail_len:%u" % (self.piece1_len, self.detail_len,)
        self.recv_message()

    def process_detail(self, data):
        assert len(data) == self.detail_len
        head_dict = json.loads(data[0:self.piece1_len - 1])
        #print head_dict
        #f = open('data.o', 'w')
        #f.write(utils.unzipData(data[self.piece1_len:]))
        #f.close()
        #sys.exit(1)
        head_dict['url'] = head_dict['url'].replace('\'', '').replace('"', '').split(' ')[0].split('?')[0].split('<')[0].split('#')[0]
        assert self.detail_len >= self.piece1_len
        if head_dict['Status-Code'] == 200 and self.detail_len > self.piece1_len:
            head_dict["zipped_page"]  = bson.Binary(data[self.piece1_len:])
            self.pagedb_logic.insert(head_dict, upsert = True)
            #sys.exit(1)
            log_message = "log_id[%u] fno[%u] bid[%u] p1l[%u] zlen[%u] deley[%u] jsonstr[%s]" \
                    % (self.log_id, self.fileno, self.blockid, self.piece1_len, self.detail_len-self.piece1_len, \
                    int(time.time()) - head_dict['timestamp'], data[0:self.piece1_len - 1], )
            logging.info(log_message)
        else:
            logging.warning('shit happens. log_id[%u] fno[%u] bid[%u] p1l[%u] zlen[%u] jsonstr:%s' % (self.log_id, \
                self.fileno, self.blockid, self.piece1_len, self.detail_len-self.piece1_len, data[0:self.piece1_len-1]))
        self.send_response()
    def send_response(self):
        sbuf = struct.pack(self.FMT_XHEAD, self.log_id, "page_db_updater", 0, 0, 0, 0, 0)
        self._stream.write(sbuf)
        self.detail_len = 0
        self.piece1_len = 0

        self.step = 0
        self.recv_message()

class appServer(TCPServer):
    def __init__(self, mongodb_config):
        TCPServer.__init__(self,)
        self.__mongodb_handler = pagedbLogic.pagedbLogic(mongodb_config)
    def handle_stream(self, stream, address):
        Connection(stream, address, self.__mongodb_handler)

if __name__ == '__main__':
    print "%s page_db_updater Server start ......" % (time.asctime(),)
    logger = logging.getLogger()
    hdlr = logging.FileHandler('./log/page_db_update.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    # load the config
    my_config = utils.load_config(sys.argv[1])
    server = appServer(my_config['mongo_config'])
    server.listen(my_config['server_config']['port'])
    IOLoop.instance().start()
