#!/home/xurui/opensource/python2.7/bin/python2.7
# -*- coding:utf8 -*-
# -1- 向evcrawler查询当前的抓取状态
# -2- 根据各个host的抓取队列情况，开始选取1个小时左右的url列表(根据配额进行计算)
# -3- 发送url列表
import socket, struct, sys, json, time
import utils, linkdbLogic
import logging, copy

FMT_XHEAD = "I16sIIHHI"

def is_valid_url(url):
    d = {}
    d['url'] = url
    try:
        json.dumps(d)
        return True
    except:
        return False
    return True

def xRequest(sock, log_id, status, dd = []):
    jsonstr = ''
    if dd:
        jsonstr = json.dumps(dd)
    sbuf = struct.pack(FMT_XHEAD, log_id, "selector", 0, 0, status, 0, len(jsonstr))
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

def get_all_host_no(linkdb_logic):
    ret_list = linkdb_logic.select_all_host()

    host_info = {}
    for x in ret_list:
        host_info[x['host']] = x['host_no']
    return host_info

def dump_json_to_file(json_root, file_path):
    f = open(file_path, 'w')
    f.truncate()
    f.write(json.dumps(json_root, indent = 4))
    f.flush()
    f.close()

if __name__ == '__main__':
    print '%s selector starting...' % (time.asctime(),)
    logger = logging.getLogger()
    hdlr = logging.FileHandler('./log/selector.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)

    # 初始化每个host选取到的url_no
    config = utils.load_config(sys.argv[1])
    host_min_urlno_path = sys.argv[2]
    host_min_urlno = utils.load_config(host_min_urlno_path)

    linkdb_logic = linkdbLogic.linkdbLogic(config['mysql_config'])
    log_count = 0
    while True:
        host_info = get_all_host_no(linkdb_logic,)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", 2008));
        log_count += 1
        status, stat_dict = xRequest(sock, log_id = log_count, status = 1)
        assert status == 0
        for host,stat_info in stat_dict.items():
            if host == 'www.baidu.com': continue
            #{u'queue_length': 1490, u'max_crawling_number': 250, u'pick_url_interval_ms': 200, u'crawling_number': 250}
            # 保持queue_length在300以上，这样可以够crawler喝个5分钟的。
            # 当queue_length在200一下时，开始补齐到300以上
            # 每次选取100个
            #print host,stat_info
            url_sent_count = 0
            while True:
                if stat_info['queue_length'] > 500:
                    logging.info('host[%s] queue_length[%u] max_crawling[%u] crawling[%u] is enough' % \
                            (host, stat_info['queue_length'], stat_info['max_crawling_number'], stat_info['crawling_number']))
                    break
                if not host_info.has_key(host):
                    logging.warning('unknown host[%s]' % host)
                    break
                else:
                    url_list = linkdb_logic.select_url_by_host(host_info[host], host_min_urlno.get(host, 0), limit = 400)
                    if len(url_list) == 0:
                        logging.info('host[%s] select url empty, min url_no[%u]' % (host, host_min_urlno.get(host, 0)))
                        # TODO host_min_urlno[host]是否需要更新?
                        time.sleep(1)
                        continue

                    max_url_no = url_list[-1]['url_no']
                    # 简陋而充满补丁的调度算法
                    if host == 'club.xywy.com':
                        url_new_list = []
                        for url_item in url_list:
                            # 跳过含有target的url，这是get_links中引入的bug
                            if -1 != url_item['url'].find('target'):
                                continue
                            url_new_list.append(copy.copy(url_item))
                        url_list = url_new_list
                    # 调度算法结束
                    refer_sign_set = set([x['refer_sign'] for x in url_list])
                    refer_sign_lst = [url_sign for url_sign in refer_sign_set]
                    refer_sign_lst.sort()
                    refer_url_list = []
                    if refer_sign_lst:
                        refer_url_list = linkdb_logic.select_url_by_sign(refer_sign_lst)
                    refer_url_dict = {}
                    for x in refer_url_list:
                        refer_url_dict[x['url_sign']] = x['url']
                    link_info_list = []
                    for x in url_list:
                        #print x
                        if x['check_time'] > 0: # TODO 目前是只抓取新的url，当页面分类的工作完成后，这里需要修改一下
                            continue
                        if not is_valid_url(x['url']) : # TODO 目前是只抓取新的url，当页面分类的工作完成后，这里需要修改一下
                            continue
                        link_info_item = {}
                        link_info_item['url']      = x['url']
                        link_info_item['url_no']   = x['url_no']
                        link_info_item['host_no']  = host_info[host]
                        # TODO 分表的时候再考虑这个问题，考虑到refer的话，同域名下的url需要在同一表中
                        link_info_item['shard_no'] = 0 
                        if refer_url_dict.has_key(x['refer_sign']) and is_valid_url(refer_url_dict[x['refer_sign']]):
                            link_info_item['refer'] = refer_url_dict[x['refer_sign']]
                        #print link_info_item
                        link_info_list.append(link_info_item)
                    #print refer_sign_lst
                    #print refer_url_dict
                    #print link_info_list
                    #sys.exit(1)
                    x_status, ret_dict = xRequest(sock, log_id = int(time.time()), status = 0, dd = link_info_list)
                    #print ret_dict
                    if len(link_info_list) == 0:
                        logging.info('host[%s] skip a lot url. min[%u] max[%u]' % \
                                (host, host_min_urlno[host], max(max_url_no, host_min_urlno.get(host, 0))))
                        host_min_urlno[host] = max(max_url_no, host_min_urlno.get(host, 0))
                        dump_json_to_file(host_min_urlno, host_min_urlno_path)
                    elif x_status != 0 :
                        logging.warning('host[%s] send url list error. status[%u]' % (host, x_status,))
                        time.sleep(1)
                        break
                    elif x_status != 0 and len(link_info_list) > 0:
                        logging.warning('error happens in crawler. status[%u]' % (x_status,))
                        time.sleep(1)
                    elif x_status == 0 and ret_dict['all_num'] > 0 and (ret_dict['all_num'] == ret_dict['err_num']):
                        logging.warning('all_num[%u] all error.' % (ret_dict['all_num']))
                        time.sleep(1)
                        #sys.exit(1)
                    else:
                        host_min_urlno[host] = max(max_url_no, host_min_urlno.get(host, 0))
                        dump_json_to_file(host_min_urlno, host_min_urlno_path)
                        url_sent_count += ret_dict['all_num'] - ret_dict['err_num']
                        logging.info('host[%s] qlen[%u] crawling[%u] max_crawling[%u] sent_count[%u] all[%u] err[%u] min_url_no[%u]' \
                                % (host,\
                                stat_info['queue_length'],\
                                stat_info['crawling_number'], \
                                stat_info['max_crawling_number'],\
                                url_sent_count,\
                                ret_dict['all_num'],\
                                ret_dict['err_num'],\
                                host_min_urlno[host]))
                        if url_sent_count + stat_info['queue_length'] > 500:
                            break
        sock.close()
        time.sleep(10)
        #sys.exit(1)
