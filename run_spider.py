from itertools import islice
import random

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapyrightmove.spiders.spider1 import Myspider as Spider1
from scrapyrightmove.spiders.spider2 import Myspider as Spider2
import time
# se = get_project_settings()
# for k,j in se.items():
#     print(k,j)
start = time.time()
process = CrawlerProcess(get_project_settings())

# def extract_code_from_excel():
#     import os
#     from openpyxl import load_workbook
#     path = os.getcwd()
#     excel_path = os.path.join(path, 'postcode', 'postcode-outcodes.xlsx')
#     outcode_list = []
#     wb = load_workbook(excel_path)
#     ws = wb['ran']
#     for row in range(2, ws.max_row + 1):
#         if ws['E' + str(row)].value != 0 and ws['F' + str(row)].value == 'OUTCODE':
#             outcode_list.append(ws['G' + str(row)].value)
#     return outcode_list

# outcode_list = extract_code_from_excel()
# random.shuffle(outcode_list)

# def random_chunk(li, min_chunk=100, max_chunk=500):
#     it = iter(li)
#     while True:
#         nxt = list(islice(it,random.randint(min_chunk,max_chunk)))
#         if nxt:
#             yield nxt
#         else:
#             break

# for i in random_chunk(outcode_list):
#     a = process.crawl(Myspider, outcode=i)
#     print(a)

process.crawl(Spider1)
process.crawl(Spider2)
# process.crawl(Myspider, outcode=outcode_list)
process.start()
print("--- %s seconds ---" % (time.time() - start))