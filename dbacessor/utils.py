# -*- coding:utf8 -*-
import gzip
import json
import re, sys, time

def get_links(page_content, host = None, inhost = True, base_url = None):
    #w3.org http://www.w3.org/TR/WD-html40-970708/htmlweb.html#relative-urls
    #<BASE href="http://www.barre.fr/fou/intro.html">
    #<base href="http://www.lingshikong.com/" />
    base_match = re.match(r'.*?<base\s+?href="(.*?)".*?>.*',page_content, flags=re.I)
    if base_match:
        base_url = base_match.group(1)
    base_url = base_url[0: base_url.rfind('/')] + '/'

    # 将正则表达式编译成Pattern对象
    filter_head = None
    if host: filter_head = 'http://%s' % host

    pattern = re.compile(r'<a.*?href=(.+?)["|\'][^>]*?>', flags=re.I | re.DOTALL)
    #pattern = re.compile(r'<a.+?href=(.+?)>\s*?(.*?)\s*?</a>', flags=re.I | re.DOTALL)
    #pattern = re.compile(r'<a.+?href=(.+?)>\s*?(.*?)\s*?</a>', flags=re.IGNORECASE | re.DOTALL | re.DEBUG)
    #follow_url_list = pattern.findall(page_content.replace('\n', ''))
    follow_url_list = pattern.findall(page_content.replace('\n', ''))
    url_set = set([])
    for y in range(len(follow_url_list)):
        # url 要么是http开头，要么是/开头
        #print follow_url_list[y][0]
        if follow_url_list[y].strip() == '' or follow_url_list[y].strip() == '\\':
            continue
        http_start = follow_url_list[y].find('http://')
        if http_start != -1:
            if http_start < 5:
                url = follow_url_list[y][http_start:]
                #print 'got http:', url
            else:
                continue
        elif follow_url_list[y].find('"') == 0:
            url = follow_url_list[y][1:]
        else:
            slash_start = follow_url_list[y].find('/')
            if slash_start != -1:
                if slash_start < 5:
                    url = follow_url_list[y][slash_start:]
                    #print 'got ////:', url
                else:
                    continue
        url = url.replace('\'', '').replace('"', '').split(' ')[0].split('?')[0].split('<')[0].split('#')[0]
        #print follow_url_list[y][0].strip(), url, len(url)
        if not is_valid_url(url):
            #print '-- same url skip', url
            continue
        if filter_head:
            if url.startswith('/'):
                url = filter_head + url
            if base_url and not url.startswith('/') and not url.startswith('http://'):
                url = base_url[:base_url.rfind('/')+1]+url

            if inhost and url[:len(filter_head)] != filter_head:
                #print '-- not begin head skip', url
                continue
            elif len(url) < len(filter_head):
                #print '-- too short skip', url
                continue
            elif url == filter_head + '/':
                #print '-- same url skip', url
                continue
        #print url, follow_url_list[y][1]
        if   host == 'www.baidu.com':
            continue 
        if   host == 'www.haodf.com':
            # haodf.com仅抓wenda
            if -1 != url.find('/hospital/') and -1 != url.find('.htm'):
                index = url.find('hospital')
                # 去掉http://www.haodf.com/hospital/DE4rO-XCoLUXyUaoRinOCoXXJe/zixun.htm等
                if 1 == url[index:].count('/'):
                    url_set.add(url)
            if -1 != url.find('/faculty/') and -1 != url.find('.htm'):
                index = url.find('faculty')
                if 1 == url[index:].count('/'):
                    url_set.add(url)
            if -1 != url.find('/doctor/') and -1 != url.find('.htm'):
                index = url.find('doctor')
                if 1 == url[index:].count('/'):
                    url_set.add(url)
        elif  host == 'www.lingshikong.com':
            url_set.add(url)
        #elif host == 'club.xywy.com':
        #    # club.xywy.com下，带juhe的不要
        #    if -1 != url.find('juhe') or -1 != url.find('yaopin'):
        #        continue
        #    if -1 != url.find('static') and -1 != url.find('target'):
        #        url = url.split('target')[0]
        #    url_set.add(url)
        #elif host == 'www.120ask.com':
        #    # 120ask.com, user不要
        #    if -1 != url.find('user'):
        #        pass
        #    elif -1 != url.find('list'):
        #        url_set.add(url)
        #    elif -1 != url.find('question'):
        #        if -1 != url.find('target'):
        #            url = url.split('target')[0]
        #        lst = url.split('/')
        #        # 120ask.com存在重复url的情况
        #        # http://www.120ask.com/question/重复的原因/39064609.htm
        #        if lst[-1].split('.')[0].isdigit():
        #            url_set.add('http://www.120ask.com/question/'+lst[-1])
        #else:
        #    # 默认全部加入扩展
        #    url_set.add(url)
        #    pass
 
        #print url
        #txt = follow_url_list[y][1]
        #print 'url:[%s], txt:[%s]' % (url, txt,)
    return url_set

def get_page_type(page_content, base_url):
    # 只能通过python3来执行这个函数，因为print要求加()。
    # 判断页面上是否存在连续的章节号
    # 这个函数需要python3才能执行
    # 0 表示默认值
    # 1 表示为索引页
    # 2 表示为详细页面
    # 3 表示为首页
    import chinese_number
    conv = chinese_number.chinese_number_convertor()
    # 将正则表达式编译成Pattern对象
    filter_head = 'http://%s' % get_host(base_url)

    href_pattern = re.compile(r'<a.+?href=(.+?)>(.+?)</a>')
    chsn_pattern = re.compile(r'第([一|二|三|四|五|六|七|八|九|十|零|百|千|两|万]+)章')
    digt_pattern = re.compile(r'第(\d+)章')
    follow_url_list = href_pattern.findall(page_content)
    number_set = set([])

    detail_sign_set = set(['上一章', '上一页', '上一章节', '下一章', '下一页', '下一章节'])
    detail_sign_count = 0

    for y in range(len(follow_url_list)):
        url = follow_url_list[y][0].replace('\'', '').replace('"', '').split(' ')[0].split('?')[0].split('<')[0].split('#')[0]
        if not is_valid_url(url):
            #print '-- same url skip', url
            continue
        if filter_head:
            if url.startswith('/'):
                url = filter_head + url
            if base_url and not url.startswith('/') and not url.startswith('http://'):
                url = base_url[:base_url.rfind('/')+1]+url
            if len(url) < len(filter_head):
                #print '-- too short skip', url
                continue
            elif url[:len(filter_head)] != filter_head:
                #print '-- not begin head skip', url
                continue
            elif url == filter_head + '/':
                #print '-- same url skip', url
                continue
        target_anchor = follow_url_list[y][1]
        #print ("url[%s] anchor[%s] #" % (url, target_anchor,))
        if target_anchor in detail_sign_set:
            detail_sign_count += 1
            #print('[%u]anchor[%s] in detail_sign_set' % (detail_sign_count,target_anchor))
        else:
            #print('anchor[%s] not in detail_sign_set' % (target_anchor))
            pass
        #target_anchor = '第 一百 章'
        chsn_list = chsn_pattern.findall(target_anchor)
        for ch in chsn_list:
            #print ('[%u] [%s] [%s]' % (conv.to_integer(ch), ch, target_anchor))
            number_set.add(conv.to_integer(ch))
        #target_anchor = '第 1 章'
        dgit_list = digt_pattern.findall(target_anchor)
        for dg in dgit_list:
            #print ('[%u] [%s] [%s]' % (int(dg), dg, target_anchor))
            number_set.add(int(dg))
            #print (dg),
        #print
        #print url
        #txt = follow_url_list[y][1]
        #print 'url:[%s], txt:[%s]' % (url, txt,)
    number_list = list(number_set)
    number_list.sort()
    #print (number_list)
    if len(number_list) > 1:
        step_length = 1
        prev_numb = number_list[0]
        #print ('%u' % prev_numb,)
        for n in number_list[1:]:
            if n == prev_numb + 1:
                step_length += 1
            else:
                step_length = 1
            prev_numb = n
            if step_length >= 10:
                #print ('got it [%u]' % n)
                return 1
            #print ('%u' % n,),
        #print
    if detail_sign_count > 0: return 2
    else: return 0

def zipData(content,):
    import StringIO, gzip, zlib
    zbuf = StringIO.StringIO()
    zfile = gzip.GzipFile(mode='wb', compresslevel=1, fileobj=zbuf)
    zfile.write(content)
    zfile.close()
    return zbuf

def unzipData(zipped_data):
    import StringIO, gzip, zlib
    uzfile = gzip.GzipFile(mode='rb', fileobj=StringIO.StringIO(zipped_data))
    content = uzfile.read()
    uzfile.close()
    return content

def load_config(config_path):
    f = file(config_path)
    content = ''
    for line in f:
        content += line.strip()
    return json.loads(content)

def is_valid_url(url):
    if -1 != url.find('javascript'): return False
    if 0 == len(url): return False
    if './' == url: return False
    d = {}
    d['url'] = url
    try:
        json.dumps(d)
        return True
    except:
        return False
    return True

def get_host(url):
    head = 'http://'
    if not url.startswith(head):
        return None
    host = url.replace(head, '')
    return host.split('/')[0].split(':')[0]

if __name__ == '__main__':
    f = open(sys.argv[1])
    content = ''
    for line in f:
        content += line
    base_url = sys.argv[2]
    content = content.replace('\n', '')
    #print (get_page_type(content, base_url))
    host = None
    inhost = True
    base_url = None
    if len(sys.argv) > 2: host = sys.argv[2]
    if len(sys.argv) > 3: inhost = (0 != int(sys.argv[3]))
    if len(sys.argv) > 4: base_url = sys.argv[4]

    url_set = get_links(content, host, inhost, base_url)
    for url in url_set:
        print url
        #print get_host(url)
    sys.exit(1)
    #b = time.time()
    #for x in xrange(1000):
    #    unzip_content = unzipData(content)
    #e = time.time()
    #for x in xrange(10):
    #    get_links(unzip_content)
    #print "time-cost: %.3f" % (e-b,)
