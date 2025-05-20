# -*- encoding: UTF-8 -*-

import utils
import logging
import work_flow
import work_flow_new1
import settings
import schedule
import time
from pathlib import Path


def job():
    if utils.is_weekday():
        work_flow.prepare()
        work_flow_new1.prepare()


logging.basicConfig(format='%(asctime)s %(message)s', filename='sequoia.log')
logging.getLogger().setLevel(logging.INFO)
settings.init()

if settings.config['cron']:
    EXEC_TIME = "15:15"
    schedule.every().day.at(EXEC_TIME).do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
else:
    work_flow_new1.prepare()
    # work_flow.prepare()
