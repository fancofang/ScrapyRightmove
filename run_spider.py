from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapyrightmove.spiders.spider1 import Myspider as Spider1
from scrapyrightmove.spiders.spider2 import Myspider as Spider2
import time
from dotenv import load_dotenv

load_dotenv()

start = time.time()
process = CrawlerProcess(get_project_settings())

process.crawl(Spider1)
process.crawl(Spider2)
process.start()

print("--- %s seconds ---" % (time.time() - start))