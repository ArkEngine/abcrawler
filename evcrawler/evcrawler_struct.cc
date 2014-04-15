#include <string.h>
#include <string>
#include "evcrawler_struct.h"

//const char* LINK_FIELD_URL      = "url";
//const char* LINK_FIELD_REFER    = "refer";
//const char* LINK_FIELD_URL_NO   = "url_no";
//const char* LINK_FIELD_SHARD_NO = "host_no";
//const char* LINK_FIELD_SHARD_NO = "shard_no";

link_info_t* link_info_new()
{
    link_info_t* tmp = (link_info_t*)calloc(sizeof(link_info_t), 1);
    if (NULL != tmp)
    {
        tmp->url = NULL;
        tmp->host = NULL;
        tmp->refer = NULL;
        tmp->retry_count = 0;
    }
    return tmp;
}

void link_info_free(link_info_t* plink_info)
{
    if (plink_info->url)
    {
        free(plink_info->url);
        plink_info->url = NULL;
    }

    if (plink_info->host)
    {
        free(plink_info->host);
        plink_info->host = NULL;
    }

    if (plink_info->path)
    {
        free(plink_info->path);
        plink_info->path = NULL;
    }

    if (plink_info->refer)
    {
        free(plink_info->refer);
        plink_info->refer = NULL;
    }

    free(plink_info);
}

bool process_link_item(const Json::Value& link_item, link_info_t* plink_info)
{
    if (link_item.isMember(LINK_FIELD_URL) && link_item[LINK_FIELD_URL].isString())
    {
        plink_info->url = strdup(link_item[LINK_FIELD_URL].asCString());
        string   host;
        string   path;
        uint16_t port;
        if (url_parser(plink_info->url, host, path, port))
        {
            plink_info->host = strdup(host.c_str());
            plink_info->path = strdup(path.c_str());
            plink_info->port = port;
        }
        else
        {
            ALARM("url_parser fail. url[%s]", plink_info->url);
            return false;
        }
    }
    else
    {
        ALARM("process field [%s] fail.", LINK_FIELD_URL);
        return false;
    }

    if (link_item.isMember(LINK_FIELD_REFER) && link_item[LINK_FIELD_REFER].isString())
    {
        plink_info->refer = strdup(link_item[LINK_FIELD_REFER].asCString());
    }

    if (link_item.isMember(LINK_FIELD_URL_NO) && link_item[LINK_FIELD_URL_NO].isInt())
    {
        plink_info->url_no = link_item[LINK_FIELD_URL_NO].asUInt();
    }
    else
    {
        ALARM("process field [%s] fail.", LINK_FIELD_URL_NO);
        return false;
    }

    if (link_item.isMember(LINK_FIELD_HOST_NO) && link_item[LINK_FIELD_HOST_NO].isInt())
    {
        plink_info->host_no = link_item[LINK_FIELD_HOST_NO].asUInt();
    }
    else
    {
        ALARM("process field [%s] fail.", LINK_FIELD_HOST_NO);
        return false;
    }

    if (link_item.isMember(LINK_FIELD_SHARD_NO) && link_item[LINK_FIELD_SHARD_NO].isInt())
    {
        plink_info->shard_no = link_item[LINK_FIELD_SHARD_NO].asUInt();
    }
    else
    {
        ALARM("process field [%s] fail.", LINK_FIELD_SHARD_NO);
        return false;
    }

    return true;
}



timer_context_t* timer_context_new(
        const char* host,
        thread_context_t* thread_context_list,
        uint32_t dns_update_interval,
        evdns_base* dnsbase)
{
    timer_context_t* ptmp = (timer_context_t*) malloc(sizeof(timer_context_t));
    if (ptmp)
    {
        ptmp->host = strdup(host);
        ptmp->thread_context_list = thread_context_list;
        ptmp->last_dns_update = (uint32_t)time(NULL) - 2*dns_update_interval;
        ptmp->dns_update_interval = dns_update_interval;
        ptmp->dnsbase = dnsbase;
    }
    return ptmp;
}

void timer_context_free(timer_context_t* ptimer_context)
{
    free(ptimer_context->host);
    free(ptimer_context);
}

request_context_t* request_context_new(
        link_info_t* plink_info, evhttp_connection* my_evhttp_connection, event_base* base, evdns_base* dnsbase)
{
    request_context_t* my_request_context = (request_context_t*)malloc(sizeof(request_context_t));
    my_request_context->plink_info = plink_info;
    my_request_context->evconn = my_evhttp_connection;
    my_request_context->base = base;
    my_request_context->dnsbase = dnsbase;
    my_request_context->host = strdup(plink_info->host);

    return my_request_context;
}

void request_context_free(request_context_t* my_request_context)
{
    free(my_request_context->host);
    my_request_context->host = NULL;
    free(my_request_context);
}

socket_context_t* socket_context_new()
{
    socket_context_t* ptmp = (socket_context_t*)calloc(sizeof(socket_context_t), 1);
    if (ptmp != NULL)
    {
        ptmp->content = NULL;
        ptmp->status = READING_HEAD;
    }
    return ptmp;
}

void socket_context_free(socket_context_t* psocket_context)
{
    free(psocket_context->content);
    free(psocket_context);
}

bool url_parser(const string& url, string& host, string& path, uint16_t& port)
{
    port = 80;
    path = "/";
    char http_format[] = "http://";
    size_t http_start = url.find(http_format);
    if (http_start == string::npos)
    {
        return false;
    }

    size_t path_start = url.find('/', sizeof(http_format));
    if (path_start == string::npos)
    {
        path = "/";
        host = url.substr(http_start+sizeof(http_format)-1);
    }
    else
    {
        path = url.substr(path_start);

        host = url.substr(http_start+sizeof(http_format)-1, path_start-sizeof(http_format)+1);

        size_t port_start = host.find(':');
        if (port_start != string::npos)
        {
            string str_port = host.substr(port_start+1);
            port = atoi(str_port.c_str());
            host = host.substr(0, port_start);
        }
    }
    return true;
}
