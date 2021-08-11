# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from pymongo import MongoClient
import mysql.connector
import datetime, time
import random


class ScrapyrightmovePipeline(object):
    # db_name = 'rightmove'
    db_name = 'test_scrapy'
    
    def __init__(self, host, user, passwd):
        self.mysql_host = host
        self.mysql_user = user
        self.mysql_passwd = passwd
        self.init_query_template()
        self.beginning_time = time.time()
        
        
    def init_query_template(self):
        # self.property_query = ("SELECT rightmove_id "
        #                 "FROM property_details "
        #                 "WHERE rightmove_id = %s ")
        
        self.property_query = ("SELECT rightmove_id "
                        "FROM property_details "
                        "WHERE rightmove_id = {} ")
        #
        # self.peak_rent_query = ("SELECT property_id, max, min "
        #                 "FROM peak_rent "
        #                 "WHERE property_id = %s ")

        # self.peak_rent_query = """SELECT property_id, max, min FROM peak_rent WHERE property_id = %s """

        self.peak_rent_query = """SELECT property_id, max, min FROM peak_rent WHERE property_id = {} """

        # self.insert_property = ("INSERT INTO property_details "
        #                 "(rightmove_id, url, agent, title, address, postcode, create_date, location, letAgreed, thumbnail) "
        #                 "VALUES (%s, %s, %s, %s, %s, %s, %s, POINT(%s,%s), %s, %s)")
 
        # self.insert_property = ("INSERT INTO property_details "
        #                 "(rightmove_id, url, agent, title, address, postcode, create_date, location, letAgreed, thumbnail) "
        #                 "VALUES ({id}, {url}, {agent}, {title}, {address}, {postcode}, {create_date}, POINT({latitude},{longitude}), {let_agreed}, {thumbnail})")
        #
        self.insert_property = ("INSERT INTO property_details "
                                "(rightmove_id, url, agent, title, address, postcode, create_date, location, letAgreed, thumbnail) "
                                "VALUES (%(id)s, %(url)s, %(agent)s, %(title)s, %(address)s, %(postcode)s, %(create_date)s, POINT(%(latitude)s,%(longitude)s), %(let_agreed)s, %(thumbnail)s)")
        
        
        # self.insert_daily_rent = ("INSERT INTO daily_rent "
        #                   "(property_id, rent, date) "
        #                   "VALUES (%s, %s, %s)")
        
        self.insert_daily_rent = ("INSERT INTO daily_rent "
                                  "(property_id, rent, date) "
                                  "VALUES (%(id)s, %(rent)s, %(date)s)")

        # self.insert_peak_rent = ("INSERT INTO peak_rent "
        #                   "(property_id, max, min) "
        #                   "VALUES (%s, %s, %s)")

        self.insert_peak_rent = ("INSERT INTO peak_rent "
                          "(property_id, max, min) "
                          "VALUES (%(id)s, %(rent)s, %(rent)s)")

        # self.update_max_peak = """ UPDATE peak_rent SET max = %s WHERE property_id = %s """
        
        self.update_max_peak = """ UPDATE peak_rent SET max = %(rent)s WHERE property_id = %(id)s """

        # self.update_min_peak = """ UPDATE peak_rent SET min = %s WHERE property_id = %s """

        self.update_min_peak = """ UPDATE peak_rent SET min = %(rent)s WHERE property_id = %(id)s """
        
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get('MYSQL_HOST'),
            user=crawler.settings.get('MYSQL_USER'),
            passwd=crawler.settings.get('MYSQL_PASSWD')
        )
    
    def open_spider(self, spider):
        self.db = mysql.connector.connect(
            user=self.mysql_user,
            host=self.mysql_host,
            password=self.mysql_passwd,
            database=self.db_name
        )
        self.query_cursor = self.db.cursor()
        self.handle_cursor = self.db.cursor()
        self.property_details_bulk = []
        self.peak_rent_bulk = []
        self.daily_rent_bulk = []
        # self.collection.drop()
    
    def close_spider(self, spider):
        self.save_to_dbs(spider)
        self.query_cursor.close()
        self.handle_cursor.close()
        self.db.close()
        print("sssss :", time.time() - self.beginning_time)
    
    def process_item(self, item, spider):
        # start_p = time.time()
        # self.query_cursor = self.db.cursor(buffered=True)
        # self.handle_cursor = self.db.cursor(buffered=True)
        # self.query_cursor.execute(self.property_query, (item['id'],))
        #
        # self.query_cursor.execute(self.peak_rent_query, (item['id'],))
        self.query_cursor.execute(self.peak_rent_query.format(item['id']))
        query_result = self.query_cursor.fetchone()

        # print(query_result)
        #
        if query_result is None:
            # insert_property_item = (
            #     item['id'], item['url'], item['agent'], item['title'], item['address'], item['postcode'],
            #     item['create_time'], item['location'][0], item['location'][1], item['let_agreed'], item['image'])
            self.property_details_bulk.append(item._values)
            # self.handle_cursor.execute(self.insert_property, item._values)
            # self.property_details_bulk.append(self.insert_property.format(**item))

            # insert_pr_item = (item['id'], item['rent'], item['rent'])
            # self.handle_cursor.execute(self.insert_peak_rent, item._values)
            self.peak_rent_bulk.append(item._values)
        else:
            self.handle_cursor.execute(self.update_max_peak, item._values) if float(query_result[1]) < float(item['rent']) else None
            self.handle_cursor.execute(self.update_min_peak, item._values) if float(query_result[2]) > float(item['rent']) else None
        daily_item = item._values.copy()
        daily_item['date'] = datetime.date.today()
        self.daily_rent_bulk.append(daily_item)
            # self.peak_rent_bulk.append(self.update_max_peak.format(**item._values)) if float(query_result[1]) < float(item['rent']) else None
            # self.peak_rent_bulk.append(self.update_min_peak.format(**item._values)) if float(
            #     query_result[2]) > float(item['rent']) else None
        # self.handle_cursor.execute(self.insert_daily_rent, (item['id'], item['rent'], datetime.date.today()))
        # print(self.insert_daily_rent.format(**item, date = datetime.date.today()))

        # self.daily_rent_bulk.append(item._values, date = datetime.date.today())

        if any(len(i) > 50 for i in (self.daily_rent_bulk, self.property_details_bulk, self.peak_rent_bulk)):
            # try:
            #     self.handle_cursor.executemany(self.insert_property, self.property_details_bulk)
            #     # a = self.handle_cursor.rowcount
            #     self.property_details_bulk = []
            #     self.handle_cursor.executemany(self.insert_peak_rent, self.peak_rent_bulk)
            #     # b = self.handle_cursor.rowcount
            #     self.peak_rent_bulk = []
            #     self.handle_cursor.executemany(self.insert_daily_rent, self.daily_rent_bulk)
            #     # d = self.handle_cursor.rowcount
            #     self.daily_rent_bulk = []
            # except Exception as e:
            #     spider.logger.error(str(e))
            # finally:
            #     self.db.commit()
            # print(self.daily_rent_bulk, self.peak_rent_bulk, self.property_details_bulk)
            # print(id(self.daily_rent_bulk), self.daily_rent_bulk)
            self.save_to_dbs(spider)
        # a = self.query_cursor.execute(select_query,(item['id'],))
        # # for i in a:
        # #     print(i)
        # # b = self.insert_cursor.execute(insert_property,inserted_item)
        # print("a:",self.query_cursor.fetchone())
        # # print("b:", b)
        # self.db.commit()
        # print(self.cursor.rowcount, "记录插入成功。")
        # self.query_cursor.close()
        # self.handle_cursor.close()
        # print("spend time:",time.time()-start_p)
        return item
        # new_item = self.extract_item(item)
        # date = new_item['rent'][0]['date']
        # if self.collection.find_one({'url': new_item['url']}) == None:
        #     self.collection.update_one({'url': new_item['url']}, {"$set": dict(new_item)}, upsert=True)
        # elif self.collection.find_one({'url': new_item['url'], 'rent.date': {"$gte": date}}):
        #     print("数据库日期已有该日期,更新其他数据")
        #     self.collection.update_one({'url': new_item['url']}, {"$set":{'maxprice':new_item['maxprice'],
        #                                                                   'minprice':new_item['minprice']
        #                                                                   # 'letAgreed': new_item['letAgreed'],
        #                                                                   # 'postcode': new_item['postcode'],
        #                                                                   # 'location': new_item['location']
        #                                                                   # 'addTime': new_item['addTime']
        #                                                                   }})
        # else:
        #     print("需要更新最新日期数据")
        #     self.collection.update_one({'url': new_item['url']}, {"$set":{'maxprice':new_item['maxprice'],
        #                                                                   'minprice':new_item['minprice']
        #                                                                   # 'letAgreed': new_item['letAgreed'],
        #                                                                   # 'postcode': new_item['postcode'],
        #                                                                   # 'location': new_item['location']
        #                                                                   # 'addTime': new_item['addTime']
        #                                                                   },
        #                                                           "$push": {'rent': new_item['rent'][0]}})
        # return new_item
    
    def save_to_dbs(self, spider):
        try:
            self.handle_cursor.executemany(self.insert_property, self.property_details_bulk)
            # a = self.handle_cursor.rowcount
            self.property_details_bulk = []
            self.handle_cursor.executemany(self.insert_peak_rent, self.peak_rent_bulk)
            # b = self.handle_cursor.rowcount
            self.peak_rent_bulk = []
            self.handle_cursor.executemany(self.insert_daily_rent, self.daily_rent_bulk)
            # d = self.handle_cursor.rowcount
            self.daily_rent_bulk = []
        except Exception as e:
            spider.logger.error(str(e))
        finally:
            self.db.commit()
            # print("成功插入property_details - [%s]条记录, peak_rent - [%s]条记录, daily_rent - [%s]条记录" %(a,b,d))
        
        # print("insert_peak_rent成功插入:", self.handle_cursor.rowcount, "记录")
        #
        # print("insert_daily_rent成功插入:", self.handle_cursor.rowcount, "记录")
        
    
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
        data = self.collection.find_one({'url': item['url']})
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
