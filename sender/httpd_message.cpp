#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <json/json.h>
#include <curl/curl.h>
//#include <openssl/ssl.h>
#include "sender.h"
#include "filelinkblock.h"
#include "MyException.h"
#include "mylog.h"

using namespace evcrawler;

void* httpd_message(void* arg)
{
	sender_config_t* psender_config = (sender_config_t*) arg;
	filelinkblock myflb(psender_config->qpath, psender_config->qfile, true);
	char channel[128];
	snprintf(channel, sizeof(channel), "%s.offset", psender_config->channel);
	myflb.set_channel(channel);
	myflb.seek_message();
	const uint32_t READ_BUFF_SIZE = 10*1024*1024;
	char*    msgbuf = (char*) malloc(READ_BUFF_SIZE);
	MyThrowAssert(msgbuf != NULL);
    char url_buff[1024];
    if ( 1 < strlen ( psender_config->url_suffix) )
    {
        snprintf(url_buff, sizeof(url_buff), "http://%s:%u%s",
                psender_config->host, psender_config->port, psender_config->url_suffix);
    }
    else
    {
        snprintf(url_buff, sizeof(url_buff), "http://%s:%u/",
                psender_config->host, psender_config->port);
    }

    char all_event_string[128];
    snprintf(all_event_string, sizeof(all_event_string), FORMAT_QUEUE_OP, 0, 0);

    curl_global_init(CURL_GLOBAL_DEFAULT);

    CURL *curl = NULL;
//    struct curl_httppost* formpost = NULL;
//    struct curl_httppost* lastptr = NULL;
    struct curl_slist*    headerlist = NULL;
    //headerlist = curl_slist_append(headerlist, "Content-type: application/json");
    headerlist = curl_slist_append(headerlist, "Content-type: text/xml");
    //headerlist = curl_slist_append(headerlist, "charsets: utf-8");

    while(1)
    {
        // read disk message
        flb_basic_head basic_head;
        uint32_t message_len = myflb.read_message(basic_head, msgbuf, READ_BUFF_SIZE);
        msgbuf[message_len] = '\0';
        MySuicideAssert(message_len > 0);

        // TODO
        // 这里可以监听摘要库的进度，防止索引进度领先于摘要库。
        // 即每次都读取摘要库的进度文件./offset/mongod_video_search_online
        // 与file_no和block_id进行比较即可。如果落后，则sleep0.5秒后继续查看摘要库进度
        // 这有个危险，这个摘要库进度文件一直被写，会不会读取到一个中间状态呢

        // 检查是否在监听的事件中
        //Json::Value root;
        //Json::Features features;
        //features.strictRoot_ = true; 
        //Json::Reader reader(features);

        //MySuicideAssert (reader.parse(msgbuf, root));

        char event_string[128];
        snprintf(event_string, sizeof(event_string), FORMAT_QUEUE_OP, basic_head.queue_type, basic_head.event_type);
        if ((psender_config->events_set.end() ==
                    psender_config->events_set.find(string(event_string)))
                && (psender_config->events_set.end() ==
                    psender_config->events_set.find(string(all_event_string))))
        {
            // 忽略既不是指定的消息也不是all all的消息
            DEBUG("channel[%s]: event[%s] is NOT in mylist. file_no[%u] block_id[%u]",
                    psender_config->channel, event_string, basic_head.file_no, basic_head.block_id);
            myflb.save_offset();
            continue;
        }

        //Json::Value body = root["__OPERATION_BODY__"];
        //string md5str;
        //if (body.isMember("_id"))
        //{
        //    md5str = body["_id"].asString();
        //}
        // 仅允许如下的key通过
        //set<string>::iterator sit;
        //Json::Value::Members keyNameString = body.getMemberNames();
        //for ( int i = 0; i < keyNameString.size(); i++ ) {
        //    string key = keyNameString[i];

        //    if ( psender_config->filter_set.end() == psender_config->filter_set.find(key))
        //    {
        //        body.removeMember(key);
        //    }
        //}

        Json::FastWriter fwriter;
        //Json::Value dstJson;
        //dstJson["doc"] = body;
        //Json::Value ddJson;
        //ddJson["add"] = dstJson;
        //string strbody = fwriter.write(ddJson);
        //string strbody = fwriter.write(body);
        //uint32_t bodysize = strbody.length();
        //MySuicideAssert(bodysize <= READ_BUFF_SIZE);
        // memmove(msgbuf, strbody.c_str(), bodysize);
        //if (bodysize <= 0)
        //{
        //    continue;
        //}

//        int ret = curl_formadd(&formpost, &lastptr, CURLFORM_COPYNAME, "add",
//                CURLFORM_PTRCONTENTS, strbody.c_str(),
//                CURLFORM_CONTENTSLENGTH, bodysize,
//                //CURLFORM_PTRCONTENTS, root["__OPERATION_BODY__"].asCString(),
//                //CURLFORM_CONTENTSLENGTH, root["__OPERATION_BODY__"].size(),
//                CURLFORM_END);
//        if (ret != 0)
//        {
//            ALARM("curl_formadd Fail. ret[%d] op[%s] file_no[%u] block_id[%u]",
//                    ret, root["__OPERATION__"].asCString(), file_no, block_id);
//            MySuicideAssert(ret == 0);
//        }

        while(1)
        {
            if (NULL == curl)
            {
                curl = curl_easy_init();
                MySuicideAssert( NULL != curl );
                MySuicideAssert( CURLE_OK == curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, psender_config->conn_toms));
                MySuicideAssert( CURLE_OK == curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, psender_config->recv_toms));
                MySuicideAssert( CURLE_OK == curl_easy_setopt(curl, CURLOPT_URL, url_buff));
                //curl_easy_setopt(curl, CURLOPT_VERBOSE, 1);
                MySuicideAssert( CURLE_OK == curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerlist));
                MySuicideAssert( CURLE_OK == curl_easy_setopt(curl, CURLOPT_POSTFIELDS, msgbuf));
                MySuicideAssert( CURLE_OK == curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, message_len));
            }
            //            MySuicideAssert( CURLE_OK == curl_easy_setopt(curl, CURLOPT_HTTPPOST, formpost));

            CURLcode res = curl_easy_perform(curl);
            int ct = 0;
            if (res == CURLE_OK)
            {
                curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &ct);
            }

            if(res == CURLE_OK && ct == 200)
            {
                // 成功了，保存进度
                myflb.save_offset();
                /* then cleanup the formpost chain */
//                curl_formfree(formpost);
//                formpost = NULL;
//                lastptr = NULL;
                ROUTN("logid[%u] file_no[%u] block_id[%u] "
                        "channel[%s] qpath[%s] qfile[%s] host[%s] port[%u] event[%s] msg[%s] msglen[%u]",
                        basic_head.log_id, basic_head.file_no, basic_head.block_id,
                        psender_config->channel,
                        psender_config->qpath,
                        psender_config->qfile,
                        psender_config->host,
                        psender_config->port,
                        event_string, msgbuf, message_len);
                break;
            }
            else
            {
                ALARM("curl_easy_perform error dst[%s] log_id[%u] file_no[%u] block_id[%u] "
                        "channel[%s] qpath[%s] qfile[%s] host[%s] port[%u] event[%s] "
                        "ct[%d] msglen[%u] msg[%s] e[%s]",
                        url_buff,
                        basic_head.log_id, basic_head.file_no, basic_head.block_id,
                        psender_config->channel,
                        psender_config->qpath,
                        psender_config->qfile,
                        psender_config->host,
                        psender_config->port,
                        event_string, ct, message_len, msgbuf, curl_easy_strerror(res));
                /* always cleanup */
                curl_easy_cleanup(curl);
                curl = NULL;
                // 防止日志打太多
                sleep(1);
                continue;
            }
        }
    }

    curl_slist_free_all(headerlist);
    headerlist = NULL;
    free(msgbuf);
    msgbuf = NULL;
}
