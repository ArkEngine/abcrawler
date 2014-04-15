#!/home/xurui/opensource/python2.7/bin/python2.7
# -*- coding:utf8 -*-
# -1- 向evcrawler查询当前的抓取状态
# -2- 根据各个host的抓取队列情况，开始选取1个小时左右的url列表(根据配额进行计算)
# -3- 发送url列表
import socket, struct, sys, json, time
import utils, linkdbLogic
import logging

FMT_XHEAD = "I16sIIHHI"

def xRequest(sock, log_id, status, dd = []):
    jsonstr = ''
    if dd:
        jsonstr = json.dumps(dd)
    sbuf = struct.pack(FMT_XHEAD, log_id, "stator", 0, 0, status, 0, len(jsonstr))
    sbuf += jsonstr
    ret = sock.send(sbuf)
    #print "send %d bytes" % ret
    rbuf = sock.recv(36)
    log_id, srvname, headid, headversion, status, piece1_len, detail_len = struct.unpack(FMT_XHEAD, rbuf)
    #print "log_id[%u] srvname[%s] status[%u] detail_len[%u]" % (log_id, srvname, status, detail_len)
    if detail_len > 0:
        rrbuf = sock.recv(detail_len)
        return status, json.loads(rrbuf)
    else:
        return status, {}

if __name__ == '__main__':
    print '%s stator starting...' % (time.asctime(),)

    # 初始化每个host选取到的url_no
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", 2006));
        status, stat_info = xRequest(sock, log_id = 1, status = 1)
        assert status == 0
        for host, stat_dict in stat_info.items():
            print host, stat_dict
        sock.close()
        time.sleep(1)
        #sys.exit(1)
