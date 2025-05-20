import utils
import logging
import work_flow_new
import settings
import schedule
import time
from pathlib import Path
import sys

def configure_logging():
    """Configures the root logger to output to both file and console."""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG) # Set to DEBUG for development/debugging, INFO for production

    # Clear existing handlers to prevent duplicate logs if re-run in same session
    # This is crucial for environments like Jupyter notebooks or repeated runs
    if logger.handlers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    # File Handler
    file_handler = logging.FileHandler('sequoia.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO) # File logs INFO and above
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO) # Console logs INFO and above
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

# Call logging configuration at the very start
configure_logging()
logger = logging.getLogger(__name__) # Get the main logger for main.py's own use

# Initialize your settings AFTER logging is configured
settings.init()

def job():
    """The main job to be scheduled or run immediately."""
    if utils.is_weekday():
        logger.info("Running stock analysis job.", extra={'stock': 'NONE', 'strategy': '调度'})
        work_flow_new.prepare()
    else:
        logger.info("Today is not a weekday, skipping stock analysis job.", extra={'stock': 'NONE', 'strategy': '调度'})

# Access config using settings.get_config()
if settings.get_config().get('cron', False):
    EXEC_TIME = "15:15"
    logger.info(f"Scheduling job to run daily at {EXEC_TIME}.", extra={'stock': 'NONE', 'strategy': '调度'})
    schedule.every().day.at(EXEC_TIME).do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
else:
    logger.info("Cron mode is disabled. Running stock analysis job immediately.", extra={'stock': 'NONE', 'strategy': '调度'})
    job() # Call job directly if not in cron mode