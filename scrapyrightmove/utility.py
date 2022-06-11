import re
import json
from datetime import datetime, date, time


def extract_details(response):
    pattern = re.compile(r"(?:window\.PAGE_MODEL)\s+=\s+(\{.*})", re.MULTILINE | re.DOTALL)
    # datas = response.xpath("//script[contains(text(), 'window.PAGE_MODEL')]/text()").re(pattern)
    xpath_result = response.xpath("//script[contains(text(), 'window.PAGE_MODEL')]/text()").get()
    raw_data ="r'" + xpath_result + "'"
    data =  pattern.findall(raw_data)
    if len(data) == 0:
        return False
    else:
        details = json.loads(data[0])
    return details

def extract_id(response):
    try:
        id = response['propertyData']['id']
    except:
        pattern = re.compile(r"/(\d*)$")
        id = pattern.findall(response.url)[0]
    return id

def extract_title(response):
    try:
        title = response['propertyData']['text']['pageTitle']
    except:
        title = None
    return title

def extract_address(response):
    try:
        address = response['propertyData']['address']['displayAddress']
    except:
        address = ""
    return address

def extract_agentdetails(response):
    try:
        data = response['propertyData']['customer']['branchDisplayName']
        agent = data.split(',')[0].strip()
    except:
        agent = None
    return agent

def extract_rent(response):
    try:
        rent = response['propertyData']['prices']['primaryPrice']
        price_cursor = re.search(r'([0-9\,]+).+(\w$)', rent, re.M)
        price = int(price_cursor.group(1).replace(',', ''))
        if price_cursor.group(2) != 'm':
            price = round(price * 4.334, 0)
        rent = int(price)
        if rent > 10000000:
            rent = 0
    except:
        rent = 0
    return rent

def extract_location(response):
    try:
        data = response['propertyData']['location']
        latitude = data['latitude']
        longitude = data['longitude']
    except:
        return None, None
    return latitude, longitude
    # try:
    #     data = response['propertyData']['location']
    #     location = {'latitude': data['latitude'], 'longitude': data['longitude']}
    # except:
    #     location = None
    # return location

def extract_postcode(response):
    try:
        data = response['propertyData']['propertyUrls']['nearbySoldPropertiesUrl']
        postcode_cursor = re.search(r'/([\w-]*).html', data)
        postcode = postcode_cursor.group(1).replace('-', ' ').upper()
    except:
        postcode = None
    return postcode

def extract_addtime(response):
    try:
        addtime = response['analyticsInfo']['analyticsProperty']['added']
    except:
        addtime = datetime.combine(date.today(), time(0))
    return addtime

def extract_letagreed(response):
    try:
        letagreed = response['analyticsInfo']['analyticsProperty']['letAgreed']
    except:
        letagreed = False
    return letagreed


def extract_image(response):
    try:
        image = response['propertyData']['images'][0]['url']
    except:
        image = None
    return image