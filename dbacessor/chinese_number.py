# -*- coding:utf8 -*-
import sys
# 只能在python3下才能执行

class chinese_number_convertor(object):
    def __init__(self,):
        self.__key_map_list = {'万':10000, \
                               '千':1000, \
                               '百':100, \
                               '十':10, \
                               '一':1, \
                               '二':2, \
                               '两':2, \
                               '三':3, \
                               '四':4, \
                               '五':5, \
                               '六':6, \
                               '七':7, \
                               '八':8, \
                               '九':9, \
                               '零':0, \
                               '0' : '零', \
                               '1' : '一', \
                               '2' : '二', \
                               '3' : '三', \
                               '4' : '四', \
                               '5' : '五', \
                               '6' : '六', \
                               '7' : '七', \
                               '8' : '八', \
                               '9' : '九', \
	}
        self.__field_list = ['十', '百', '千', '万', ]
    def to_integer(self, chinese_number, ):
        #print(chinese_number, len(chinese_number))
        #assert len(chinese_number) % 3 == 0
        x = len(chinese_number)
        step = 0
        index = 0
        interval_finish = True
        num_list = [0,]
        while x > 0:
            #print chinese_number[step:step+3],
            cur_key = chinese_number[index:index+1]
            #print self.__key_map_list[cur_key]
            if interval_finish:
                # cur_key = '一二三四五六七八九十零'
                if cur_key == '零':
                    pass
                #elif cur_key == '十':
                #    num_list[0] += 10
                #    interval_finish = True
                elif x == 1: # 到了末尾了
                    num_list[0] += self.__key_map_list[cur_key]
                    interval_finish = True
                else:
                    if cur_key == '十':
                        #一千零十五 or 一千零一十五
                        #print chinese_number,
                        num_list[0] += self.__key_map_list[cur_key]
                        interval_finish = True
                    else:
                        interval_finish = False
                        num_list.append(self.__key_map_list[cur_key])
            else:
                if x == 1: # 到了末尾了
                    if len(num_list) == 2:
                        num_list[0] += num_list.pop()*self.__key_map_list[cur_key]
                    elif len(num_list) == 1:
                        num_list[0] += self.__key_map_list[cur_key]
                    interval_finish = True
                else:
                    interval_finish = True
                    num_list[0] += num_list.pop()*self.__key_map_list[cur_key]
            x -= 1
            index += 1
            step += 1
        assert(1 == len(num_list))
        return num_list[0]

    def to_chinese(self, number, ):
        str_number = str(number)
        length = len(str_number)
        assert length < 6;
        int_chinese = ''
        bool_zero = False
        loop_count = 0
        if   number % 10000 == 0: loop_count = length - 4
        elif number % 1000  == 0: loop_count = length - 3
        elif number % 100   == 0: loop_count = length - 2
        elif number % 10    == 0: loop_count = length - 1
        for x in str_number:
            if x == '0' and bool_zero:
                pass
            elif x == '0' and not bool_zero:
                int_chinese += self.__key_map_list[x]
                bool_zero = True
            else:
                int_chinese += self.__key_map_list[x]
                if length > 1:
                    int_chinese += self.__field_list[length - 2]
            length -= 1
            loop_count -= 1
            if loop_count == 0: break
        int_chinese_list = []
        int_chinese_list.append(int_chinese)
        if -1 != int_chinese.find('一十') and -1 == int_chinese.find('百一十'):
            int_chinese_list.append(int_chinese.replace('一十', '十'))
        elif -1 != int_chinese.find('二百'):
            int_chinese_list.append(int_chinese.replace('二百', '两百'))
        elif -1 != int_chinese.find('二千'):
            int_chinese_list.append(int_chinese.replace('二千', '两千'))
        elif -1 != int_chinese.find('二万'):
            int_chinese_list.append(int_chinese.replace('二万', '两万'))
        return int_chinese_list
if __name__ == '__main__':
    cnc = chinese_number_convertor()
    for line in open(sys.argv[1]):
        tlist = line.strip().split(' ')
        retnum = cnc.to_integer(tlist[0])
        if int(tlist[1]) != retnum:
            print ('line: %s **FAIL**' % (line.strip(),))
        else:
            print ('%8d   ==   %s' % (retnum, tlist[0], ))
    for line in open(sys.argv[1]):
        tlist = line.strip().split(' ')
        retstr_list = cnc.to_chinese(int(tlist[1]))
        assert tlist[0] in retstr_list
        for retstr in retstr_list:
            print ('%s   ==   %s  == %d' % (retstr, tlist[0], int(tlist[1])))
    print ("PASS ALL!")
