# from twisted.internet import pollreactor
# print(pollreactor)

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapyrightmove.spiders.spider1 import Myspider as Spider1
from scrapyrightmove.spiders.spider2 import Myspider as Spider2
from scrapyrightmove.spiders.spider3 import Myspider as Spider3
from scrapyrightmove.spiders.spider4 import Myspider as Spider4
from scrapyrightmove.spiders.spider5 import Myspider as Spider5
from scrapyrightmove.spiders.spider6 import Myspider as Spider6
from scrapyrightmove.spiders.spider7 import Myspider as Spider7
from scrapyrightmove.spiders.spider8 import Myspider as Spider8
from scrapyrightmove.spiders.spider9 import Myspider as Spider9
from scrapyrightmove.spiders.spider10 import Myspider as Spider10
from scrapyrightmove.spiders.spider11 import Myspider as Spider11
from scrapyrightmove.spiders.spider12 import Myspider as Spider12


import time
from dotenv import load_dotenv

load_dotenv()

if __name__ == '__main__':
    # pollreactor.install()
    start = time.time()
    process = CrawlerProcess(get_project_settings())

    process.crawl(Spider1)
    process.crawl(Spider2)
    process.crawl(Spider3)
    process.crawl(Spider4)
    process.crawl(Spider5)
    process.crawl(Spider6)
    process.crawl(Spider7)
    process.crawl(Spider8)
    process.crawl(Spider9)
    process.crawl(Spider10)
    process.crawl(Spider11)
    process.crawl(Spider12)

    process.start()

    print("--- %s seconds ---" % (time.time() - start))