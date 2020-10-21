#! /bin/sh
export PATH=$PATH:/usr/local/bin
cd /srv/ScrapyRightmove
source ./venv/bin/activate
nohup scrapy crawl rightmove >> rightmove.log 2>&1 &
