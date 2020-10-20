# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ScrapyrightmoveItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    id = scrapy.Field()
    url = scrapy.Field()
    agent = scrapy.Field()
    addTime = scrapy.Field()
    title = scrapy.Field()
    address = scrapy.Field()
    rent = scrapy.Field()
    maxprice = scrapy.Field()
    minprice = scrapy.Field()
    letAgreed = scrapy.Field()
    postcode = scrapy.Field()
    location = scrapy.Field()
