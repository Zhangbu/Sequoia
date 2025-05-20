# -*- encoding: UTF-8 -*-
import akshare as ak
import logging
import talib as tl
import concurrent.futures
import time
import random
from functools import wraps
import requests
from typing import List, Tuple, Dict, Optional
import pandas as pd
from cachetools import TTLCache

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cache for API results (TTL = 1 hour to avoid redundant calls)
cache = TTLCache(maxsize=1000, ttl=3600)

# Rate limiting decorator
def rate_limited(max_requests: int, period: float):
    """Decorator to limit the rate of function calls."""
    calls = []
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_time = time.time()
            # Clean up old calls
            calls[:] = [t for t in calls if current_time - t < period]
            
            if len(calls) >= max_requests:
                sleep_time = period - (current_time - calls[0])
                if sleep_time > 0:
                    logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
            
            calls.append(time.time())
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Retry decorator for handling transient errors
def retry_on_failure(max_attempts: int = 3, base_delay: float = 1.0):
    """Decorator to retry function on failure with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"Failed after {max_attempts} attempts: {e}")
                        raise
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    logger.warning(f"Request failed: {e}, retrying after {delay:.2f} seconds")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

@retry_on_failure(max_attempts=3, base_delay=1.0)
@rate_limited(max_requests=10, period=60.0)  # Adjust based on API limits
def fetch(code_name: Tuple[str, str]) -> Optional[pd.DataFrame]:
    """Fetch stock data for a given stock code."""
    stock, name = code_name
    cache_key = f"{stock}_20250101_qfq"
    
    # Check cache first
    if cache_key in cache:
        logger.debug(f"Cache hit for stock: {stock}")
        return cache[cache_key]
    
    try:
        logger.debug(f"Fetching data for stock: {stock}")
        data = ak.stock_zh_a_hist(symbol=stock, period="daily", start_date="20250101", adjust="qfq")
        
        if data is None or data.empty:
            logger.warning(f"No data for stock: {stock}, skipping...")
            return None
        
        # Calculate price change
        data['p_change'] = tl.ROC(data['收盘'], 1)
        
        # Store in cache
        cache[cache_key] = data
        return data
    
    except Exception as e:
        logger.error(f"Error fetching data for {stock}: {e}")
        return None

def run(stocks: List[Tuple[str, str]], batch_size: int = 10, max_workers: int = 8) -> Dict[str, pd.DataFrame]:
    """
    Fetch stock data for a list of stocks in batches.
    
    Args:
        stocks: List of tuples containing (stock_code, stock_name)
        batch_size: Number of stocks to process in each batch
        max_workers: Number of concurrent threads
    """
    stocks_data = {}
    
    # Process stocks in batches
    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} stocks")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_stock = {executor.submit(fetch, stock): stock for stock in batch}
            for future in concurrent.futures.as_completed(future_to_stock):
                stock = future_to_stock[future]
                try:
                    data = future.result()
                    if data is not None:
                        data = data.astype({'成交量': 'double'})
                        stocks_data[stock] = data
                except Exception as exc:
                    logger.error(f"{stock[1]} ({stock[0]}) generated an exception: {exc}")
        
        # Small delay between batches to avoid overwhelming the server
        if i + batch_size < len(stocks):
            time.sleep(random.uniform(0.5, 1.5))
    
    return stocks_data