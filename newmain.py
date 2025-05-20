# main.py
import utils
import logging
import work_flow_new
import settings
import schedule
import time
from pathlib import Path

# 配置日志
# 创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG) # 设置最低日志级别

# 创建一个文件处理器，用于写入日志文件
file_handler = logging.FileHandler('sequoia.log', encoding='utf-8')
file_handler.setLevel(logging.DEBUG) # 文件处理器只记录INFO及以上级别
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# 创建一个控制台处理器，用于输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO) # 控制台处理器记录INFO及以上级别
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# 将处理器添加到logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 确保不再重复添加处理器
# if not logger.handlers:
#     logger.addHandler(file_handler)
#     logger.addHandler(console_handler)


settings.init() # 初始化你的设置

def job():
    if utils.is_weekday():
        logger.info("Running stock analysis job.", extra={'stock': 'NONE', 'strategy': '调度'})
        work_flow_new.prepare()
    else:
        logger.info("Today is not a weekday, skipping stock analysis job.", extra={'stock': 'NONE', 'strategy': '调度'})

if settings.config.get('cron', False):
    EXEC_TIME = "15:15"
    logger.info(f"Scheduling job to run daily at {EXEC_TIME}.", extra={'stock': 'NONE', 'strategy': '调度'})
    schedule.every().day.at(EXEC_TIME).do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
else:
    logger.info("Cron mode is disabled. Running stock analysis job immediately.", extra={'stock': 'NONE', 'strategy': '调度'})
    work_flow_new.prepare()