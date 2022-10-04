#! /bin/bash
export PATH=$PATH:/usr/local/bin
cd /srv/ScrapyRightmove
source ./venv/bin/activate
nohup python run_spider.py >> ./logs/daily_task.log 2>&1