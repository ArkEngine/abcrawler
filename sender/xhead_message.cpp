#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <json/json.h>
#include "sender.h"
#include "filelinkblock.h"
#include "MyException.h"
#include "xhead.h"
#include "mylog.h"

using namespace evcrawler;

int myconnect(const sender_config_t* psender_config)
{
	struct sockaddr_in adr_srvr;  /* AF_INET */
	int len_inet = sizeof (adr_srvr);
	memset (&adr_srvr, 0, len_inet);
	adr_srvr.sin_family = AF_INET;
	adr_srvr.sin_port = htons (psender_config->port);
	adr_srvr.sin_addr.s_addr = inet_addr (psender_config->host);

	if (adr_srvr.sin_addr.s_addr == INADDR_NONE)
	{
		ALARM("inet_addr[%s:%u] channel[%s] error [%m]",
				psender_config->host, psender_config->port,
				psender_config->channel);
		return -1;
	}

	int sockfd = socket (PF_INET, SOCK_STREAM, 0);
	if (sockfd == -1)
	{
		ALARM("socket()[%s:%s] channel[%s] error [%m]",
				psender_config->host, psender_config->port,
				psender_config->channel);
		return -1;
	}

	struct timeval  timeout = {0, 0};
	timeout.tv_sec  = psender_config->conn_toms/1000;
	timeout.tv_usec = 1000*(psender_config->conn_toms%1000);
	setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeval));

	int err = connect (sockfd, (struct sockaddr *) &adr_srvr, len_inet);
	if (err == -1)
	{
		ALARM("connent() [%s:%u] channel[%s] error [%m]",
				psender_config->host, psender_config->port,
				psender_config->channel);
        close(sockfd);
		return -1;
	}
	return sockfd;
}

void* xhead_message(void* arg)
{
	sender_config_t* psender_config = (sender_config_t*) arg;
	filelinkblock myflb(psender_config->qpath, psender_config->qfile, true);
	char channel[128];
	snprintf(channel, sizeof(channel), "%s.offset", psender_config->channel);
	myflb.set_channel(channel);
    // 取本地的进度，然后发过去
    // flexse首先忽略这个包，然后回复一个flexse需要的进度，
    // 然后回滚到那个进度即可
    // 几种情况的考虑:
    // -1- 当flexse正常工作，sender重启时，会继续当前进度发送
    // -2- 当flexse重新启动，sender正常工作时，会接受来自flexse的反馈进度，重新发送
    // -3- 当二者都重新启动，sender
	myflb.seek_message();
	const uint32_t READ_BUFF_SIZE = 10*1024*1024;
	xhead_t* sxhead = (xhead_t*) malloc(READ_BUFF_SIZE + sizeof(xhead_t));
	char*    msgbuf = (char*)    (&sxhead[1]);
	MyThrowAssert(sxhead != NULL && msgbuf != NULL);
	snprintf(sxhead->srvname, sizeof(sxhead->srvname), "%s", PROJNAME);
    char recv_buff[1024];
	xhead_t* rxhead = (xhead_t*)recv_buff;

    timeval tv_begin, tv_end;

	char all_event_string[128];
	snprintf(all_event_string, sizeof(all_event_string), FORMAT_QUEUE_OP, 0, 0);

	int sock = -1;

	while(1)
	{
		// read disk message
        flb_basic_head basic_head;
		uint32_t message_len = myflb.read_message(basic_head, msgbuf, READ_BUFF_SIZE);


        // TODO
        // 这里可以监听摘要库的进度，防止索引进度领先于摘要库。
        // 即每次都读取摘要库的进度文件./offset/mongod_video_search_online
        // 与file_no和block_id进行比较即可。如果落后，则sleep0.5秒后继续查看摘要库进度
        // 这有个危险，这个摘要库进度文件一直被写，会不会读取到一个中间状态呢

        char event_string[128];
        snprintf(event_string, sizeof(event_string), FORMAT_QUEUE_OP, basic_head.queue_type, basic_head.event_type);
        if ((psender_config->events_set.end() == psender_config->events_set.find(string(event_string)))
                && (psender_config->events_set.end() == psender_config->events_set.find(string(all_event_string))))
        {
            // 忽略既不是指定的消息也不是all all的消息
            DEBUG("channel[%s]: event[%s] is NOT in mylist. file_no[%u] block_id[%u]",
                    psender_config->channel, event_string, basic_head.file_no, basic_head.block_id);
            myflb.save_offset();
            continue;
        }

        sxhead->log_id     = basic_head.log_id;
        sxhead->version    = basic_head.file_no;
        sxhead->reserved   = basic_head.block_id;
        sxhead->piece1_len = basic_head.piece1_len;
        sxhead->detail_len = message_len;

        while(1)
        {
            if (sock == -1)
            {
                sock = myconnect(psender_config);
                if (sock == -1)
                {
                    sleep(1);
                    continue;
                }
            }

            if (0 != xsend(sock, sxhead, psender_config->send_toms))
            {
                ALARM("xsend error log_id[%u] file_no[%u] block_id[%u] "
                        "channel[%s] qpath[%s] qfile[%s] "
                        "host[%s] port[%u] event[%s] msglen[%u] e[%m]",
                        basic_head.log_id, basic_head.file_no, basic_head.block_id,
                        psender_config->channel,
                        psender_config->qpath,
                        psender_config->qfile,
                        psender_config->host,
                        psender_config->port,
                        event_string, message_len);
                close(sock);
                sock = -1;
                // 防止日志打太多
                sleep(1);
                continue;
            }
            gettimeofday(&tv_begin, NULL);
            if (0 != xrecv(sock, rxhead, sizeof(recv_buff) - sizeof(xhead_t), psender_config->recv_toms))
            {
                gettimeofday(&tv_end, NULL);
                uint32_t time_cost_ms = (tv_end.tv_sec - tv_begin.tv_sec)*1000 + (tv_end.tv_usec - tv_begin.tv_usec)/1000;
                // 对方没返回，应该重试
                ALARM("xrecv error log_id[%u] file_no[%u] block_id[%u] "
                        "channel[%s] qpath[%s] qfile[%s] host[%s] port[%u] event[%s] msglen[%u] msg[%s] time_cost_ms[%u] e[%m]",
                        basic_head.log_id, basic_head.file_no, basic_head.block_id,
                        psender_config->channel,
                        psender_config->qpath,
                        psender_config->qfile,
                        psender_config->host,
                        psender_config->port,
                        event_string, message_len, msgbuf, time_cost_ms);
                close(sock);
                sock = -1;
                // 防止日志打太多
                sleep(1);
                continue;
            }
            else
            {
                gettimeofday(&tv_end, NULL);
                uint32_t time_cost_ms = (tv_end.tv_sec - tv_begin.tv_sec)*1000 + (tv_end.tv_usec - tv_begin.tv_usec)/1000;
                if (rxhead->status == OK)
                {
                    // 成功了，保存进度
                    myflb.save_offset();
                    ROUTN("logid[%u] file_no[%u] block_id[%u] "
                            "channel[%s] qpath[%s] qfile[%s] host[%s] port[%u] event[%s] msglen[%u] time_cost_ms[%u]",
                            basic_head.log_id, basic_head.file_no, basic_head.block_id,
                            psender_config->channel,
                            psender_config->qpath,
                            psender_config->qfile,
                            psender_config->host,
                            psender_config->port,
                            event_string, message_len, time_cost_ms);
                    if (! psender_config->long_connect)
                    {
                        close(sock);
                        sock = -1;
                    }
                    break;
                }
                else if (rxhead->status == ROLL_BACK)
                {
                    // 对方要求回滚
                    ROUTN("log_id[%u] host[%s] port[%u] "
                            "channel[%s] qpath[%s] qfile[%s] roll back to file_no[%u] block_id[%u]",
                            basic_head.log_id, psender_config->host, psender_config->port,
                            psender_config->channel, 
                            psender_config->qpath,
                            psender_config->qfile,
                            rxhead->file_no, rxhead->block_id);
                    myflb.seek_message(rxhead->file_no, rxhead->block_id);
                    break;
                }
                else
                {
                    ALARM("status error log_id[%u] status[%u] errno[%u] file_no[%u] block_id[%u] "
                            "channel[%s] qpath[%s] qfile[%s] event[%s] msglen[%u] host[%s] port[%u] e[%m]",
                            basic_head.log_id, rxhead->status, rxhead->reserved, basic_head.file_no, basic_head.block_id,
                            psender_config->channel,
                            psender_config->qpath,
                            psender_config->qfile,
                            event_string, message_len,
                            psender_config->host,
                            psender_config->port);
                    // 防止日志打太多
                    sleep(1);
                    continue;
                }
            }
        }
    }
}
