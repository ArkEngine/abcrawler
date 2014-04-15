#include <stdio.h>
#include <getopt.h>
#include <stdlib.h>
#include <pthread.h>
#include <json/json.h>
#include <iostream>
#include <fstream>
#include <signal.h>
#include <string.h>
#include "mylog.h"
#include "MyException.h"
#include "sender.h"

using namespace std;
using namespace evcrawler;

void* xhead_message(void*);
//void* httpd_message(void*);
//void* mongo_message(void*);

void PrintHelp(void)
{
    printf("\nUsage:\n");
    printf("%s <options>\n", PROJNAME);
    printf("  options:\n");
    printf("    -c:  #conf path\n");
    printf("    -v:  #version\n");
    printf("    -h:  #This page\n");
    printf("\n\n");
}

void PrintVersion(void)
{
    printf("Project    :  %s\n", PROJNAME);
    printf("Version    :  %s\n", _VERSION);
    printf("BuildDate  :  %s - %s\n", __DATE__, __TIME__);
}

int main(int argc, char** argv)
{
    signal(SIGPIPE, SIG_IGN);
    const char * strConfigPath = "./conf/"PROJNAME".config.json";
    char configPath[128];
    snprintf(configPath, sizeof(configPath), "%s", strConfigPath);

    char c = '\0';
    while ((c = getopt(argc, argv, "c:vh?")) != -1)
    {
        switch (c)
        {
            case 'c':
                snprintf(configPath, sizeof(configPath), "%s", optarg);
                strConfigPath = configPath;
                break;
            case 'v':
                PrintVersion();
                return 0;
            case 'h':
            case '?':
                PrintHelp();
                return 0;
            default:
                break;
        }
    }

    Json::Value root;
    Json::Features features;
    features.strictRoot_ = true;
    Json::Reader reader(features);

    ifstream in(strConfigPath);
    if (! reader.parse(in, root))
    {
        FATAL("file[%s] json format error.", strConfigPath);
        MyToolThrow("json format error.");
    }

    uint32_t    loglevel = root.isMember("LogLevel") ? root["LogLevel"].asInt() : 2;
    const char* logname  = root.isMember("LogPath")  ? root["LogPath"].asCString() : PROJNAME;
    SETLOG(loglevel, logname);
    ROUTN( "=====================================================================");

    Json::Value follower_array = root["followers"];
    if (0 >= follower_array.size())
    {
        MyThrowAssert("no followers.");
    }
    sender_config_t* psender_list = new sender_config_t[follower_array.size()];
    Json::Value::const_iterator iter;
    iter = follower_array.begin();

    uint32_t follower_real_count = 0;

    set<string> channel_set;

    for (uint32_t i=0; i<follower_array.size(); i++)
    {
        Json::Value mysender = follower_array[i];
        if (0 == mysender["enable"].asInt())
        {
            ROUTN("channel[%s] skip.", mysender["name"].asCString());
            continue;
        }
        psender_list[follower_real_count].enable = mysender["enable"].asInt();
        psender_list[follower_real_count].sender_id = follower_real_count;
        // 设置消息队列的路径和名称
        snprintf (psender_list[follower_real_count].qpath,
                sizeof(psender_list[follower_real_count].qpath),
                "%s", mysender["queue_path"].asCString());
        snprintf (psender_list[follower_real_count].qfile,
                sizeof(psender_list[follower_real_count].qfile),
                "%s", mysender["queue_file"].asCString());

        snprintf (psender_list[follower_real_count].channel,
                sizeof(psender_list[follower_real_count].channel),
                "%s", mysender["name"].asCString());
        if (channel_set.end() == channel_set.find(string(psender_list[follower_real_count].channel)))
        {
            channel_set.insert(string((psender_list[follower_real_count].channel)));
        }
        else
        {
            FATAL("dup channel[%s]", psender_list[follower_real_count].channel);
            MyThrowAssert(0);
        }
        snprintf (psender_list[follower_real_count].host,
                sizeof(psender_list[follower_real_count].host),
                "%s", mysender["host"].asCString());
        psender_list[follower_real_count].port = mysender["port"].asInt();

        //uint32_t json_list_size = mysender["filter_keys"].size();
        //for (uint32_t ii=0; ii<json_list_size; ii++)
        //{
            //string strkey = mysender["filter_keys"][ii].asCString();
            //psender_list[follower_real_count].filter_set.insert(strkey);
        //}

        const char* mode =  mysender["mode"].asCString();
        if (0 == strcmp(mode, "xhead"))
        {
            snprintf(psender_list[follower_real_count].mode,
                    sizeof(psender_list[follower_real_count].mode),
                    "%s", mode);
            psender_list[follower_real_count].long_connect = mysender["long_connect"].asInt();
            psender_list[follower_real_count].send_toms = mysender["send_toms"].asInt();
            psender_list[follower_real_count].recv_toms = mysender["recv_toms"].asInt();
            psender_list[follower_real_count].conn_toms = mysender["conn_toms"].asInt();
            ROUTN("xhead consumer[%s] qpath[%s] qfile[%s] host[%s] port[%d] long_connect[%d] "
                    "send_toms[%u] recv_toms[%u] conn_toms[%u]",
                    psender_list[follower_real_count].channel,
                    psender_list[follower_real_count].qpath,
                    psender_list[follower_real_count].qfile,
                    psender_list[follower_real_count].host,
                    psender_list[follower_real_count].port,
                    psender_list[follower_real_count].long_connect,
                    psender_list[follower_real_count].send_toms,
                    psender_list[follower_real_count].recv_toms,
                    psender_list[follower_real_count].conn_toms);
        }
        //else if(0 == strcmp(mode, "mongo"))
        //{
        //    snprintf(psender_list[follower_real_count].mode,
        //            sizeof(psender_list[follower_real_count].mode),
        //            "%s", mode);

        //    psender_list[follower_real_count].insert_mode = (1 == mysender["insert_mode"].asInt());
        //    psender_list[follower_real_count].sleep_interval_us = 0;
        //    if (mysender.isMember("sleep_interval_us"))
        //    {
        //        psender_list[follower_real_count].sleep_interval_us = mysender["sleep_interval_us"].asInt();
        //    }
        //    snprintf(psender_list[follower_real_count].database,
        //            sizeof(psender_list[follower_real_count].database),
        //            "%s", mysender["database"].asCString());
        //    snprintf(psender_list[follower_real_count].collection,
        //            sizeof(psender_list[follower_real_count].collection),
        //            "%s", mysender["collection"].asCString());
        //    snprintf(psender_list[follower_real_count].indexkey,
        //            sizeof(psender_list[follower_real_count].indexkey),
        //            "%s", mysender["indexkey"].asCString());

        //    json_list_size = mysender["int2date_keys"].size();
        //    for (uint32_t ii=0; ii<json_list_size; ii++)
        //    {
        //        string strkey = mysender["int2date_keys"][ii].asCString();
        //        psender_list[follower_real_count].int2date_set.insert(strkey);
        //    }

        //    Json::Value need_default_dict = mysender["need_default"];
        //    Json::Value::Members keyNameString = need_default_dict.getMemberNames();
        //    json_list_size = mysender["need_default"].size();
        //    for (uint32_t ii=0; ii<json_list_size; ii++)
        //    {
        //        string strkey = keyNameString[ii];
        //        uint32_t type = need_default_dict[strkey].asUInt();
        //        MyThrowAssert(type < 3);
        //        psender_list[follower_real_count].need_default_map[strkey] = type;
        //    }

        //    ROUTN("mongo consumer[%s] qpath[%s] qfile[%s] host[%s] port[%d] "
        //            "database[%s] collection[%s] indexkey[%s]",
        //            psender_list[follower_real_count].channel,
        //            psender_list[follower_real_count].qpath,
        //            psender_list[follower_real_count].qfile,
        //            psender_list[follower_real_count].host,
        //            psender_list[follower_real_count].port,
        //            psender_list[follower_real_count].database,
        //            psender_list[follower_real_count].collection,
        //            psender_list[follower_real_count].indexkey);
        //}
        else if(0 == strcmp(mode, "httpd"))
        {
            snprintf(psender_list[follower_real_count].mode,
                    sizeof(psender_list[follower_real_count].mode),
                    "%s", mode);
            psender_list[follower_real_count].recv_toms = mysender["recv_toms"].asInt();
            psender_list[follower_real_count].conn_toms = mysender["conn_toms"].asInt();
            snprintf(psender_list[follower_real_count].url_suffix,
                    sizeof(psender_list[follower_real_count].url_suffix),
                    "%s", mysender["url_suffix"].asCString());
            MySuicideAssert('/' == psender_list[follower_real_count].url_suffix[0]);
            ROUTN("xhead consumer[%s] qpath[%s] qfile[%s] host[%s] port[%d] url_suffix[%s] "
                    "recv_toms[%u] conn_toms[%u]",
                    psender_list[follower_real_count].channel,
                    psender_list[follower_real_count].qpath,
                    psender_list[follower_real_count].qfile,
                    psender_list[follower_real_count].host,
                    psender_list[follower_real_count].port,
                    psender_list[follower_real_count].url_suffix,
                    psender_list[follower_real_count].recv_toms,
                    psender_list[follower_real_count].conn_toms);
        }
        else
        {
            FATAL("mode[%s] unknown", mode);
            MySuicideAssert(0);
        }

        Json::Value events_list = mysender["events"];
        if (0 >= events_list.size())
        {
            MyThrowAssert("sender has no events.");
        }

        for (uint32_t k=0; k<events_list.size(); k++)
        {
            Json::Value event = events_list[k];
            char event_string[128];
            snprintf(event_string, sizeof(event_string), FORMAT_QUEUE_OP,
                    events_list[k]["queue_type"].asUInt(), events_list[k]["event_type"].asUInt());
            ROUTN("channel[%s] event[%s]", psender_list[follower_real_count].channel, event_string);
            psender_list[follower_real_count].events_set.insert(string(event_string));
        }
        iter++;
        follower_real_count ++;
    }

    MySuicideAssert (follower_real_count > 0);

    pthread_t* thread_id_list = new pthread_t[follower_real_count];
    // 生成发送线程，使用sender_config_t作为配置
    for (uint32_t i=0; i<follower_real_count; i++)
    {
        if (0 == strcmp(psender_list[i].mode, "xhead"))
        {
            MyThrowAssert( 0 == pthread_create(&thread_id_list[i],
                        NULL, xhead_message, &psender_list[i]));
        }
        //else if (0 == strcmp(psender_list[i].mode, "mongo"))
        //{
        //    MyThrowAssert( 0 == pthread_create(&thread_id_list[i],
        //                NULL, mongo_message, &psender_list[i]));
        //}
        //else if (0 == strcmp(psender_list[i].mode, "httpd"))
        //{
        //    MyThrowAssert( 0 == pthread_create(&thread_id_list[i],
        //                NULL, httpd_message, &psender_list[i]));
        //}
    }

    for (uint32_t i=0; i<follower_real_count; i++)
    {
        pthread_join(thread_id_list[i], NULL);
    }

    delete [] psender_list;
    delete [] thread_id_list;
}
