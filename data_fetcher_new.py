# -*- encoding: UTF-8 -*-
import akshare as ak
import logging
import talib as tl
import asyncio
import aiohttp
import time
import random
from functools import wraps
from cachetools import TTLCache
from typing import List, Tuple, Dict, Optional
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cache for API results (TTL = 1 hour)
cache = TTLCache(maxsize=1000, ttl=3600)

# Rate limiting configuration
MAX_REQUESTS = 20  # Increase based on API limits (adjust after testing)
PERIOD = 60.0  # Seconds
REQUEST_SEMAPHORE = asyncio.Semaphore(10)  # Concurrent requests limit

# Retry configuration
MAX_ATTEMPTS = 2  # Reduced to minimize latency
BASE_DELAY = 0.5  # Reduced base delay for faster retries

def rate_limited(max_requests: int, period: float):
    """Decorator to limit the rate of async function calls."""
    calls = []
    
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_time = time.time()
            calls[:] = [t for t in calls if current_time - t < period]
            
            if len(calls) >= max_requests:
                sleep_time = period - (current_time - calls[0])
                if sleep_time > 0:
                    logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                    await asyncio.sleep(sleep_time)
            
            calls.append(time.time())
            return await func(*args, **kwargs)
        return wrapper
    return decorator

async def retry_on_failure(func):
    """Async retry decorator with exponential backoff."""
    async def wrapper(*args, **kwargs):
        for attempt in range(MAX_ATTEMPTS):
            try:
                return await func(*args, **kwargs)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == MAX_ATTEMPTS - 1:
                    logger.error(f"Failed after {MAX_ATTEMPTS} attempts: {e}")
                    return None
                delay = BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.05)
                logger.warning(f"Request failed: {e}, retrying after {delay:.2f} seconds")
                await asyncio.sleep(delay)
        return None
    return wrapper

@retry_on_failure
@rate_limited(max_requests=MAX_REQUESTS, period=PERIOD)
async def fetch(code_name: Tuple[str, str], session: aiohttp.ClientSession) -> Optional[pd.DataFrame]:
    """Fetch stock data asynchronously for a given stock code."""
    stock, name = code_name
    cache_key = f"{stock}_20250101_qfq"
    
    # Check cache
    if cache_key in cache:
        logger.debug(f"Cache hit for stock: {stock}")
        return cache[cache_key]
    
    async with REQUEST_SEMAPHORE:
        try:
            logger.debug(f"Fetching data for stock: {stock}")
            # Since akshare is synchronous, run in default executor
            data = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: ak.stock_zh_a_hist(symbol=stock, period="daily", start_date="20250101", adjust="qfq")
            )
            
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

async def run_async(stocks: List[Tuple[str, str]], batch_size: int = 20) -> Dict[str, pd.DataFrame]:
    """
    Fetch stock data asynchronously in batches.
    
    Args:
        stocks: List of tuples containing (stock_code, stock_name)
        batch_size: Number of stocks to process in each batch
    """
    stocks_data = {}
    
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} stocks")
            
            # Fetch batch concurrently
            tasks = [fetch(stock, session) for stock in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for stock, data in zip(batch, results):
                if isinstance(data, Exception):
                    logger.error(f"{stock[1]} ({stock[0]}) generated an exception: {data}")
                elif data is not None:
                    try:
                        data = data.astype({'成交量': 'double'})
                        stocks_data[stock] = data
                    except Exception as e:
                        logger.error(f"Error processing data for {stock[1]} ({stock[0]}): {e}")
            
            # Minimal delay between batches
            if i + batch_size < len(stocks):
                await asyncio.sleep(random.uniform(0.1, 0.3))
    
    return stocks_data

def run(stocks: List[Tuple[str, str]], batch_size: int = 20) -> Dict[str, pd.DataFrame]:
    """Synchronous wrapper for async run function."""
    return asyncio.run(run_async(stocks, batch_size))
