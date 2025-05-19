# -*- encoding: UTF-8 -*-

import akshare as ak
import pandas as pd
import talib as tl
import logging
import concurrent.futures
import os
import time
from datetime import datetime, timedelta
from retrying import retry
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Semaphore to limit concurrent API calls
semaphore = threading.Semaphore(5)

# Cache directory
CACHE_DIR = './cache'
os.makedirs(CACHE_DIR, exist_ok=True)

# Function to generate cache file path
def get_cache_path(stock_code):
    return os.path.join(CACHE_DIR, f"{stock_code}_daily.csv")

# Retry decorator for API calls
@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def fetch_stock_data(symbol, start_date, end_date):
    with semaphore:
        logger.debug(f"Fetching data for {symbol}")
        data = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        time.sleep(0.1)  # Small delay per request
        return data

def fetch(code_name):
    stock_code, stock_name = code_name
    cache_path = get_cache_path(stock_code)
    
    # Check cache
    if os.path.exists(cache_path):
        try:
            data = pd.read_csv(cache_path)
            if not data.empty:
                logger.debug(f"Loaded cached data for {stock_code}")
                data['日期'] = pd.to_datetime(data['日期'])
                return data
        except Exception as e:
            logger.warning(f"Failed to read cache for {stock_code}: {e}")

    # Set date range (last 30 days)
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

    try:
        data = fetch_stock_data(stock_code, start_date, end_date)
        if data is None or data.empty:
            logger.debug(f"股票：{stock_code} 没有数据，略过...")
            return None

        # Calculate price change
        data['p_change'] = tl.ROC(data['收盘'], 1)

        # Save to cache
        data.to_csv(cache_path, index=False)
        logger.info(f"Cached data for {stock_code}")

        return data
    except Exception as e:
        logger.error(f"Failed to fetch data for {stock_code}: {e}")
        return None

def run(stocks, batch_size=50):
    stocks_data = {}
    total_stocks = len(stocks)
    logger.info(f"Processing {total_stocks} stocks")

    # Process in batches
    for i in range(0, total_stocks, batch_size):
        batch = stocks[i:i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} stocks)")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_stock = {executor.submit(fetch, stock): stock for stock in batch}
            for future in concurrent.futures.as_completed(future_to_stock):
                stock = future_to_stock[future]
                try:
                    data = future.result()
                    if data is not None:
                        data = data.astype({'成交量': 'double'})
                        stocks_data[stock] = data
                except Exception as exc:
                    logger.error(f"{stock[1]}({stock[0]}) generated an exception: {exc}")

        # Delay between batches
        if i + batch_size < total_stocks:
            logger.debug("Pausing between batches...")
            time.sleep(0.5)

    logger.info(f"Completed processing. Retrieved data for {len(stocks_data)} stocks")
    return stocks_data

if __name__ == "__main__":
    # Example: Fetch stock list (replace with actual stock list)
    stock_list = ak.stock_zh_a_spot_em()[['代码', '名称']].head(100).values.tolist()  # Top 100 stocks
    stocks_data = run(stock_list)
    print(f"Retrieved data for {len(stocks_data)} stocks")