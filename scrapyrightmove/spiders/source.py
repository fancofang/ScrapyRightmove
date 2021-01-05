import scrapy
import re
import json
from urllib.parse import urlparse, parse_qs, urlencode
from scrapyrightmove.items import ScrapyrightmoveItem
from datetime import datetime, date, time
import logging

class Myspider(scrapy.Spider):
    name = 'rightmove'

    start_urls = [
        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=1200&minPrice=100&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=1300&minPrice=1200&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=1400&minPrice=1300&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=1500&minPrice=1400&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=1750&minPrice=1500&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=2000&minPrice=1750&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=2500&minPrice=2000&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=3000&minPrice=2500&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=3500&minPrice=3000&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=5000&minPrice=3500&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E87490'
        '&maxPrice=20000&minPrice=5000&sortType=2&includeLetAgreed=false'
    ]

    custom_settings = {
        'LOG_LEVEL': 'WARNING'
    }

    def parse(self, response):

        self.logger.info('Parse function called on %s', response.url)
        urls = set()
        htmls = response.xpath('//a[contains(@href,"/property-to-rent/property")]/@href').getall()
        for i in htmls:
            urls.add(i)
            yield response.follow(i, callback=self.parse_details)
        parsed  = urlparse(response.url)
        querys = parse_qs(parsed.query)
        para = {k: v[0] for k, v in querys.items()}
        if 'index' not in para:
            para['index'] = '24'
        else:
            para['index'] = str(int(para['index']) + 24)
        data = urlencode(para)
        next_page = 'https://www.rightmove.co.uk/property-to-rent/find.html?' +data
        num = response.xpath('//span[@class="searchHeader-resultCount"]/text()').get()
        num = num.replace(',','')
        print("总共房源数:",num)
        print('目前index:', para['index'])

        if int(para['index']) < int(num) :
            print("准备下一页爬虫")
            yield response.follow(next_page, callback=self.parse)
            print(urls)
        # else:
        #     raise CloseSpider('完成爬虫,退出!')

    def parse_details(self, response):
        data = self.extract_details(response)
        
        try:
            id = self.extract_id(data)
            if id is None:
                print('Id does not find')
            
            title = self.extract_title(data)
            if title is None:
                print('Title does not find')
                
            address = self.extract_address(data)
            if address is None:
                print('Address does not find')
            
            agent = self.extract_agentdetails(data)
            if agent is None:
                print('Agent does not find')
                
            rent = self.extract_rent(data)
            if rent is None:
                print('Rent does not find')
            
            location = self.extract_location(data)
            if location is None:
                print('Location does not find')
            
            postcode = self.extract_postcode(data)
            if postcode is None:
                print('Postcode does not find')
                
            letagreed = self.extract_letagreed(data)
            if letagreed is None:
                print('Letagreed does not find')
                
            addtime = datetime.combine(date.today(), time(0))

            item = ScrapyrightmoveItem(id=id, url=response.url, agent=agent,
                                       addTime=addtime, title=title,
                                       address=address, rent={'date':date.today(),'price':rent},
                                       letAgreed = letagreed, postcode=postcode, location=location
            )
            yield item
            # print(item)
        except Exception as e:
            self.logger.error(str(e))

    def extract_id(self, response):
        id = response['propertyData']['id']
        return id

    def extract_title(self, response):
        title = response['propertyData']['text']['pageTitle']
        return title

    def extract_address(self, response):
        address = response['propertyData']['address']['displayAddress']
        return address
    
    def extract_agentdetails(self, response):
        data = response['propertyData']['customer']['branchDisplayName']
        agent = data.split(',')[0].strip()
        return agent
    
    def extract_rent(self, response):
        rent = response['propertyData']['prices']['primaryPrice']
        price_cursor = re.search(r'([0-9\,]+).+(\w$)', rent, re.M)
        price = int(price_cursor.group(1).replace(',', ''))
        if price_cursor.group(2) != 'm':
            price = round(price * 4.334, 0)
        rent = int(price)
        return rent
    
    def extract_location(self, response):
        data = response['propertyData']['location']
        location = {'latitude': data['latitude'], 'longitude': data['longitude']}
        return location
    
    def extract_postcode(self, response):
        data = response['propertyData']['propertyUrls']['nearbySoldPropertiesUrl']
        postcode_cursor = re.search(r'/([\w-]*).html', data)
        postcode = postcode_cursor.group(1).replace('-', ' ').upper()
        return postcode
    
    def extract_letagreed(self, response):
        letagreed = response['analyticsInfo']['analyticsProperty']['letAgreed']
        return letagreed

    def extract_details(self, response):
        pattern = re.compile(r"window\.PAGE_MODEL = (\{.*})",re.MULTILINE | re.DOTALL)
        data = response.xpath("//script[contains(., 'window.PAGE_MODEL')]/text()").re(pattern)[0]
        details = json.loads(data)
        return details
        