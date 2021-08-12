import re
import json
from datetime import datetime, date, time


def extract_details(response):
    # pattern = re.compile(r"(?:window\.PAGE_MODEL) = (\{.*})", re.MULTILINE | re.DOTALL)
    pattern = re.compile(r"(?:window\.PAGE_MODEL)\s+=\s+(\{.*})", re.MULTILINE | re.DOTALL)
    data = response.xpath("//script[contains(text(), 'window.PAGE_MODEL')]/text()").re(pattern)
    if len(data) == 0 :
        print(response)
        print("11111111")
        print(response.text)
        print(response.status)
        print("2222222222222")
    # data = response.xpath("//script[contains(., 'window.PAGE_MODEL')]/text()").re(pattern)[0]
    details = json.loads(data)
    # print(type(details))
    return details

def extract_id(response):
    try:
        id = response['propertyData']['id']
    except:
        id = None
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
        address = None
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
    except:
        rent = None
    return rent

def extract_location(response):
    try:
        data = response['propertyData']['location']
        location = {'latitude': data['latitude'], 'longitude': data['longitude']}
    except:
        location = None
    return location

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
