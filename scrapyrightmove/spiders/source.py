import scrapy
from urllib.parse import urlparse, parse_qs, urlencode
from scrapyrightmove.items import ScrapyrightmoveItem
from scrapyrightmove.utility import *

class Myspider(scrapy.Spider):
    name = 'rightmove'

    start_urls = [
        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=800&includeLetAgreed=false',
        
        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=1000&minPrice=800&includeLetAgreed=false',
    
        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=1200&minPrice=1000&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=1300&minPrice=1200&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=1400&minPrice=1300&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=1500&minPrice=1400&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=1750&minPrice=1500&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=2000&minPrice=1750&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=2500&minPrice=2000&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=3000&minPrice=2500&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=3500&minPrice=3000&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=5000&minPrice=3500&sortType=2&includeLetAgreed=false',

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&maxPrice=20000&minPrice=5000&sortType=2&includeLetAgreed=false'

        'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E93917'
        '&minPrice=20000&sortType=2&includeLetAgreed=false'
    ]

    custom_settings = {
        'LOG_LEVEL': 'WARNING'
    }

    def parse(self, response):
        self.logger.info('Parse function called on %s', response.url)
        seen = set()
        htmls = response.xpath('//a[contains(@href,"/property-to-rent/property")]/@href').getall()
        for i in htmls:
            if i not in seen:
                seen.add(i)
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
        # else:
        #     raise CloseSpider('完成爬虫,退出!')

    def parse_details(self, response):
        # print("url:",response.url)
        data = extract_details(response)
        
        try:
            id = extract_id(data)
            title = extract_title(data)
            address = extract_address(data)
            agent = extract_agentdetails(data)
            rent = extract_rent(data)
            location = extract_location(data)
            postcode = extract_postcode(data)
            # due to original url are filtered by letagree=False, so don't need to extract
            # letagreed = extract_letagreed(data)
            letagreed = False
            addtime = extract_addtime(data)

            item = ScrapyrightmoveItem(id=id, url=response.url, agent=agent,
                                       addTime=addtime, title=title,
                                       address=address, rent={'date':date.today(),'price':rent},
                                       letAgreed = letagreed, postcode=postcode, location=location
            )
            yield item
            # print(item)
        except Exception as e:
            self.logger.error(str(e))

    # def extract_id(self, response):
    #     id = response['propertyData']['id']
    #     return id
    #
    # def extract_title(self, response):
    #     title = response['propertyData']['text']['pageTitle']
    #     return title
    #
    # def extract_address(self, response):
    #     address = response['propertyData']['address']['displayAddress']
    #     return address
    #
    # def extract_agentdetails(self, response):
    #     data = response['propertyData']['customer']['branchDisplayName']
    #     agent = data.split(',')[0].strip()
    #     return agent
    #
    # def extract_rent(self, response):
    #     rent = response['propertyData']['prices']['primaryPrice']
    #     price_cursor = re.search(r'([0-9\,]+).+(\w$)', rent, re.M)
    #     price = int(price_cursor.group(1).replace(',', ''))
    #     if price_cursor.group(2) != 'm':
    #         price = round(price * 4.334, 0)
    #     rent = int(price)
    #     return rent
    #
    # def extract_location(self, response):
    #     data = response['propertyData']['location']
    #     location = {'latitude': data['latitude'], 'longitude': data['longitude']}
    #     return location
    #
    # def extract_postcode(self, response):
    #     data = response['propertyData']['propertyUrls']['nearbySoldPropertiesUrl']
    #     postcode_cursor = re.search(r'/([\w-]*).html', data)
    #     postcode = postcode_cursor.group(1).replace('-', ' ').upper()
    #     return postcode
    #
    # def extract_letagreed(self, response):
    #     letagreed = response['analyticsInfo']['analyticsProperty']['letAgreed']
    #     return letagreed
    #
    # def extract_details(self, response):
    #     pattern = re.compile(r"window\.PAGE_MODEL = (\{.*})",re.MULTILINE | re.DOTALL)
    #     data = response.xpath("//script[contains(., 'window.PAGE_MODEL')]/text()").re(pattern)[0]
    #     details = json.loads(data)
    #     return details
        