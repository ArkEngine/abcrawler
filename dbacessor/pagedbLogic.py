# -*- coding:utf8 -*-
import pymongo
import hashlib
import sys

class pagedbLogic(object):

    def __init__(self, mongodb_config):
        self.connection = pymongo.MongoClient(mongodb_config['host'], mongodb_config['port'])
        self.database   = self.connection[mongodb_config['database']]
        self.collection = None

    def update(self, doc_item):
        _id = self.get_primary_key(doc_item)
        if self.collection.find_one({'_id': _id}):
            # 存在则更新
            self.collection.update({'_id': _id}, {'$set':doc_item}, upsert=True)
        else:
            return False, 'primary_key [' + _id + '] NOT exists.'
        return True, 'OK'

    def delete(self, doc_item):
        # 找出主键
        _id = self.get_primary_key(doc_item,)
        # 判断主键是否存在
        if self.collection.find_one({'_id': _id}):
            # 存在则删除
            self.collection.remove({'_id': _id})
        else:
            return False, 'primary_key [' + _id + '] NOT exists.'
        return True, 'OK'

    def insert(self, doc_item, upsert = False):
        # 找出主键
        _id = self.get_primary_key(doc_item)
        # 判断主键是否存在
        if self.collection.find_one({'_id': _id}):
            # 存在则更新
            if upsert:
                self.collection.update({'_id': _id}, {'$set':doc_item}, upsert=True)
                return True, 'primary_key [' + _id + '] upsert OK.'
            else:
                return False, 'primary_key [' + _id + '] already exists.'
        else:
            doc_item['_id'] = _id
            self.collection.insert(doc_item)
        return True, 'OK'

    def select(self, doc_id):
        return self.collection.find_one({'_id': str(doc_id)})

    def get_primary_key(self, doc_item,):
        assert doc_item.has_key('url')
        #print '[%s]' % doc_item['url']
        _id = hashlib.md5(doc_item['url']).hexdigest()
        table_name = doc_item['host'].replace('.', '_').replace('-', '_')
        self.collection = self.database[table_name]
        return str(_id)
