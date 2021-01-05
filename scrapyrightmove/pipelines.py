# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from pymongo import MongoClient
import datetime

class ScrapyrightmovePipeline(object):

    collection_name = 'rightmove'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = MongoClient(self.mongo_uri, connectTimeoutMS=200, retryWrites=True)
        self.db = self.client[self.mongo_db]
        self.collection = self.db[self.collection_name]
        # self.collection.drop()

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        new_item = self.extract_item(item)
        date = new_item['rent'][0]['date']
        if self.collection.find_one({'url': new_item['url']}) == None:
            self.collection.update_one({'url': new_item['url']}, {"$set": dict(new_item)}, upsert=True)
        elif self.collection.find_one({'url': new_item['url'], 'rent.date': {"$gte": date}}):
            print("数据库日期已有该日期,更新其他数据")
            self.collection.update_one({'url': new_item['url']}, {"$set":{'maxprice':new_item['maxprice'],
                                                                          'minprice':new_item['minprice'],
                                                                          'letAgreed': new_item['letAgreed'],
                                                                          'postcode': new_item['postcode'],
                                                                          'location': new_item['location']
                                                                          # 'addTime': new_item['addTime']
                                                                          }})
        else:
            print("需要更新最新日期数据")
            self.collection.update_one({'url': new_item['url']}, {"$set":{'maxprice':new_item['maxprice'],
                                                                          'minprice':new_item['minprice'],
                                                                          'letAgreed': new_item['letAgreed'],
                                                                          'postcode': new_item['postcode'],
                                                                          'location': new_item['location']
                                                                          # 'addTime': new_item['addTime']
                                                                          },
                                                                  "$push": {'rent': new_item['rent'][0]}})
        return new_item

    def extract_item(self, item):
        # if isinstance(item['rent']['price'],str):       ### price价格为'Infinity'
        #     item['rent']['price'] = 0
        # item['rent']['price'] = int(item['rent']['price'])
        item['rent']['date'] = datetime.datetime.combine(item['rent']['date'], datetime.time())
        item['maxprice'] = self.maxprice_item(item)
        item['minprice'] = self.minprice_item(item)
        # new = item.copy()
        # del new['rent']
        temp = []
        temp.append(item['rent'])
        item['rent'] = temp
        return item



    def maxprice_item(self, item):
        data = self.collection.find_one({'url':item['url']})
        if data == None or 'maxprice' not in data:
            maxprice = item['rent']['price']
        else:
            maxprice = max(data['maxprice'], item['rent']['price'])
        return maxprice

    def minprice_item(self, item):
        data = self.collection.find_one({'url': item['url']})
        if data == None or 'minprice' not in data:
            minprice = item['rent']['price']
        else:
            minprice = min(data['minprice'], item['rent']['price'])
        return minprice