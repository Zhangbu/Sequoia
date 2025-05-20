# main.py
import utils
import logging
import work_flow_new
import settings
import schedule
import time
from pathlib import Path

# 配置日志 (这里是主文件自己的日志配置，与 work_flow_new.py 中的日志配置略有不同，但可以共存)
# 通常，更细致的日志应该在 work_flow_new.py 和策略文件中配置
logging.basicConfig(format='%(asctime)s %(message)s', filename='sequoia.log', level=logging.INFO)
# logging.getLogger().setLevel(logging.INFO) # 这行被上面的basicConfig包含了

settings.init() # 初始化你的设置

def job():
    if utils.is_weekday(): # 假设 utils.is_weekday() 能正确判断工作日
        logging.info("Running stock analysis job.", extra={'stock': 'NONE', 'strategy': '调度'})
        work_flow_new.prepare()
    else:
        logging.info("Today is not a weekday, skipping stock analysis job.", extra={'stock': 'NONE', 'strategy': '调度'})

if settings.config.get('cron', False): # 使用 .get() 避免 KeyError
    EXEC_TIME = "15:15" # 例如，每天15:15执行
    logging.info(f"Scheduling job to run daily at {EXEC_TIME}.", extra={'stock': 'NONE', 'strategy': '调度'})
    schedule.every().day.at(EXEC_TIME).do(job)

    while True:
        schedule.run_pending()
        time.sleep(1) # 每秒检查一次调度
else:
    logging.info("Cron mode is disabled. Running stock analysis job immediately.", extra={'stock': 'NONE', 'strategy': '调度'})
    work_flow_new.prepare()