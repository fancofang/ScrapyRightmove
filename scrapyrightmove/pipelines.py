# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import mysql.connector
import datetime


class ScrapyrightmovePipeline(object):
    db_name = 'rightmove'
    # db_name = 'test_scrapy'
    
    def __init__(self, host, user, passwd):
        self.mysql_host = host
        self.mysql_user = user
        self.mysql_passwd = passwd
        self.init_query_template()
        
        
    def init_query_template(self):
        self.property_query = ("SELECT rightmove_id "
                        "FROM property_details "
                        "WHERE rightmove_id = %s ")

        self.peak_rent_query = ("SELECT property_id, max, min FROM peak_rent WHERE property_id = %s ")

        self.daily_rent_query = ("SELECT * FROM daily_rent WHERE property_id = %s AND date = %s ")

        self.insert_property = ("INSERT INTO property_details "
                                "(rightmove_id, url, agent, title, address, postcode, create_date, location, letAgreed, thumbnail) "
                                "VALUES (%(id)s, %(url)s, %(agent)s, %(title)s, %(address)s, %(postcode)s, %(create_date)s, POINT(%(latitude)s,%(longitude)s), %(let_agreed)s, %(thumbnail)s)")
        
        self.insert_daily_rent = ("INSERT INTO daily_rent "
                                  "(property_id, rent, date) "
                                  "VALUES (%(id)s, %(rent)s, %(date)s)")

        self.insert_peak_rent = ("INSERT INTO peak_rent "
                          "(property_id, max, min) "
                          "VALUES (%(id)s, %(rent)s, %(rent)s)")
        
        self.update_max_peak = """ UPDATE peak_rent SET max = %(rent)s WHERE property_id = %(id)s """

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
    
    def process_item(self, item, spider):
        if item is None:
            return item
        if any(len(i) > 50 for i in (self.daily_rent_bulk, self.property_details_bulk, self.peak_rent_bulk)):
            self.save_to_dbs(spider)
        self.query_cursor.execute(self.peak_rent_query,(item['id'],))
        peak_rent_query_result = self.query_cursor.fetchone()
        
        self.query_cursor.execute(self.property_query,(item['id'],))
        property_rent_query_result = self.query_cursor.fetchone()
        
        if property_rent_query_result is None:
            self.property_details_bulk.append(item._values)
        if peak_rent_query_result is None:
            self.peak_rent_bulk.append(item._values)
        else:
            self.handle_cursor.execute(self.update_max_peak, item._values) if float(peak_rent_query_result[1]) < float(item['rent']) else None
            self.handle_cursor.execute(self.update_min_peak, item._values) if float(peak_rent_query_result[2]) > float(item['rent']) else None
        self.query_cursor.execute(self.daily_rent_query, (item['id'],datetime.date.today()))
        query_result = self.query_cursor.fetchone()
        if query_result is None:
            daily_item = item._values.copy()
            daily_item['date'] = datetime.date.today()
            self.daily_rent_bulk.append(daily_item)
        return item
    
    def save_to_dbs(self, spider):
        insert_property_amount,insert_peak_amount,insert_daily_amount = 0,0,0
        if self.property_details_bulk:
            try:
                self.handle_cursor.executemany(self.insert_property, self.property_details_bulk)
                self.db.commit()
                insert_property_amount = self.handle_cursor.rowcount
                self.property_details_bulk = []
            except mysql.connector.Error as err:
                spider.logger.error("save to dbs error - insert_property: %s"% str(err))
                print("error insert_property:",self.property_details_bulk[-1])
                self.db.rollback()
        if self.peak_rent_bulk:
            try:
                self.handle_cursor.executemany(self.insert_peak_rent, self.peak_rent_bulk)
                self.db.commit()
                insert_peak_amount = self.handle_cursor.rowcount
                self.peak_rent_bulk = []
            except mysql.connector.Error as err:
                spider.logger.error("save to dbs error - insert_peak: %s" % str(err))
                print("error insert_peak:", self.peak_rent_bulk[-1])
                self.db.rollback()
        if self.daily_rent_bulk:
            try:
                self.handle_cursor.executemany(self.insert_daily_rent, self.daily_rent_bulk)
                self.db.commit()
                insert_daily_amount = self.handle_cursor.rowcount
                self.daily_rent_bulk = []
            except mysql.connector.Error as err:
                spider.logger.error("save to dbs error - insert_daily: %s" % str(err))
                self.db.rollback()
        spider.logger.info("Succefully insert to dbs this time: property-[%s], peak-[%s], daily-[%s]" % (insert_property_amount, insert_peak_amount, insert_daily_amount))