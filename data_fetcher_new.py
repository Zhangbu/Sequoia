# data_fetcher_new.py
import akshare as ak
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
import datetime
from ratelimit import limits, sleep_and_retry
import sys
from tqdm import tqdm
import traceback

logger = logging.getLogger(__name__) # Get the shared logger

# Configuration for data caching - CACHE_DIR will be passed from settings now
# CACHE_DIR = "stock_data_cache" # This will be set by init()
CACHE_FORMAT = "parquet" # Or "csv" (parquet is generally better for DataFrames)

@sleep_and_retry
@limits(calls=5, period=60) # Limit AKShare calls to 5 per minute to avoid being blocked
def fetch_single_stock_data(stock_code, stock_name, start_date_str="20230101", cache_dir="stock_data_cache"):
    """
    Fetches historical daily stock data and manages caching (smarter update).
    """
    os.makedirs(cache_dir, exist_ok=True) # Ensure cache directory exists
    file_name = f"{stock_code}.{CACHE_FORMAT}"
    file_path = os.path.join(cache_dir, file_name)
    
    today = datetime.date.today()
    # Assume data is up-to-date if it includes yesterday's data or today's if market is open
    # This logic assumes end of day data is available after 3 PM
    latest_expected_date = today if datetime.datetime.now().hour >= 15 else today - datetime.timedelta(days=1)
    
    # Convert start_date_str to date object for comparison
    min_fetch_start_date = datetime.datetime.strptime(start_date_str, '%Y%m%d').date()

    cached_df = pd.DataFrame() 
    
    # Phase 1, Item 2: Smarter Cache Update - Try to load and append
    if os.path.exists(file_path):
        try:
            if CACHE_FORMAT == "parquet":
                cached_df = pd.read_parquet(file_path)
            elif CACHE_FORMAT == "csv":
                cached_df = pd.read_csv(file_path)
            
            # Ensure '日期' column is datetime and sorted
            cached_df['日期'] = pd.to_datetime(cached_df['日期'])
            cached_df = cached_df.sort_values(by='日期').reset_index(drop=True)

            if not cached_df.empty:
                last_cached_date = cached_df['日期'].max().date()
                
                # Check if cached data is already up-to-date or covers the full requested history
                if last_cached_date >= latest_expected_date and cached_df['日期'].min().date() <= min_fetch_start_date:
                    logger.debug(f"从缓存加载 {stock_name}({stock_code}) 数据，最新日期: {last_cached_date}", extra={'stock': stock_code, 'strategy': '数据获取'})
                    return cached_df
                elif last_cached_date >= latest_expected_date: # Data is up-to-date, but history might be shorter than requested
                    logger.info(f"缓存 {stock_name}({stock_code}) 最新日期 ({last_cached_date}) 已达最新，但起始日期 ({cached_df['日期'].min().strftime('%Y-%m-%d')}) 晚于请求的 {min_fetch_start_date.strftime('%Y-%m-%d')}。", extra={'stock': stock_code, 'strategy': '数据获取'})
                    # We still need to fetch from min_fetch_start_date to get the full history
                    start_date_str_for_fetch = min_fetch_start_date.strftime('%Y%m%d')
                else: # Cached data is outdated
                    logger.info(f"缓存 {stock_name}({stock_code}) 数据过期 ({last_cached_date})，将更新。", extra={'stock': stock_code, 'strategy': '数据获取'})
                    start_date_str_for_fetch = (last_cached_date + datetime.timedelta(days=1)).strftime('%Y%m%d')
                    logger.debug(f"Attempting to fetch new data from {start_date_str_for_fetch} for {stock_code}", extra={'stock': stock_code, 'strategy': '数据获取'})

            else: # Cached file exists but is empty/corrupted
                logger.warning(f"缓存文件 {file_path} 为空或损坏，将重新下载。", extra={'stock': stock_code, 'strategy': '数据获取'})
                if os.path.exists(file_path):
                    os.remove(file_path) # Remove corrupted file
                start_date_str_for_fetch = start_date_str # Fetch from original start_date

        except Exception as e:
            logger.warning(f"加载缓存文件 {file_path} 失败: {e}\n{traceback.format_exc()}，将重新下载。", extra={'stock': stock_code, 'strategy': '数据获取'})
            if os.path.exists(file_path):
                os.remove(file_path) # Remove problematic file
            start_date_str_for_fetch = start_date_str # Fetch from original start_date
    else: # No cached file exists
        start_date_str_for_fetch = start_date_str

    logger.info(f"从AKShare下载 {stock_name}({stock_code}) 数据 (从 {start_date_str_for_fetch} 开始)...", extra={'stock': stock_code, 'strategy': '数据获取'})
    try:
        new_data_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date_str_for_fetch, adjust="hfq")
        
        if new_data_df.empty:
            logger.warning(f"AKShare未能获取到 {stock_name}({stock_code}) 的历史数据 (从 {start_date_str_for_fetch} 开始)。", extra={'stock': stock_code, 'strategy': '数据获取'})
            return cached_df # Return existing cache if no new data was fetched (it might be old, but better than nothing)

        column_mapping = {
            '日期': '日期', '开盘': '开盘', '收盘': '收盘', '最高': '最高',
            '最低': '最低', '成交量': '成交量', '成交额': '成交额', '换手率': '换手率',
            '股票代码': '股票代码', # Ensure stock code is also mapped if present
        }
        # Filter and rename columns
        mapped_cols = {k:v for k,v in column_mapping.items() if k in new_data_df.columns}
        new_data_df = new_data_df.rename(columns=mapped_cols)[list(mapped_cols.values())].copy()
        
        # Ensure '日期' is datetime type
        new_data_df['日期'] = pd.to_datetime(new_data_df['日期'])
        new_data_df = new_data_df.sort_values(by='日期').reset_index(drop=True)

        # Phase 3, Item 7: More comprehensive data validation after fetch
        # Check for NaN in critical numeric columns
        numeric_cols_to_check = ['收盘', '开盘', '最高', '最低', '成交量', '成交额', '换手率']
        for col in numeric_cols_to_check:
            if col in new_data_df.columns and new_data_df[col].isnull().any():
                logger.warning(f"下载的 {stock_name}({stock_code}) 数据在列 '{col}' 包含NaN值。尝试填充。", extra={'stock': stock_code, 'strategy': '数据获取'})
                # Fill NaN with previous valid observation (Forward Fill), then backward fill for leading NaNs
                new_data_df[col].fillna(method='ffill', inplace=True)
                new_data_df[col].fillna(method='bfill', inplace=True) # For any leading NaNs
                # If still NaNs (e.g., all NaNs), fill with 0 or a very small number
                new_data_df[col].fillna(0, inplace=True)
                
        # Handle cases where data might be too short after cleaning or initially
        if new_data_df.empty or len(new_data_df) < 30: # Arbitrary minimum length for some common indicators (e.g., 20-period MA + buffer)
             logger.warning(f"下载的 {stock_name}({stock_code}) 数据清洗后过短或为空 ({len(new_data_df)}行)。可能无法用于复杂策略。", extra={'stock': stock_code, 'strategy': '数据获取'})
             # If new data is too short, return cached if valid, otherwise empty DF
             return cached_df if not cached_df.empty else pd.DataFrame()


        # Merge new data with cached data (if cached_df is not empty)
        if not cached_df.empty:
            # Use concat and drop_duplicates based on '日期' to handle overlaps and ensure unique dates
            combined_df = pd.concat([cached_df, new_data_df]).drop_duplicates(subset=['日期']).sort_values(by='日期').reset_index(drop=True)
            df_to_save = combined_df
            logger.info(f"成功更新 {stock_name}({stock_code}) 数据，总行数: {len(df_to_save)}。", extra={'stock': stock_code, 'strategy': '数据获取'})
        else:
            df_to_save = new_data_df
            logger.info(f"成功下载 {stock_name}({stock_code}) 数据，总行数: {len(df_to_save)}。", extra={'stock': stock_code, 'strategy': '数据获取'})

        # Cache data
        if CACHE_FORMAT == "parquet":
            df_to_save.to_parquet(file_path, index=False)
        elif CACHE_FORMAT == "csv":
            df_to_save.to_csv(file_path, index=False)
        
        return df_to_save

    except Exception as e:
        logger.error(f"下载或处理 {stock_name}({stock_code}) 数据失败: {e}\n{traceback.format_exc()}", extra={'stock': stock_code, 'strategy': '数据获取'})
        return cached_df if not cached_df.empty else pd.DataFrame() # Return existing cache or empty on failure

def run(stocks_list, start_date="20230101", cache_dir="stock_data_cache"):
    """Runs data fetching for a list of stocks using a thread pool."""
    all_stocks_data = {}
    
    # Ensure cache directory exists, based on the passed cache_dir
    os.makedirs(cache_dir, exist_ok=True)

    with ThreadPoolExecutor(max_workers=5) as executor: 
        future_to_stock = {
            executor.submit(fetch_single_stock_data, code, name, start_date, cache_dir): (code, name)
            for code, name in stocks_list
        }
        for future in tqdm(as_completed(future_to_stock), 
                           total=len(future_to_stock), 
                           desc="Fetching stock data", 
                           unit="stock",
                           file=sys.stdout # Ensure tqdm prints to stdout
                           ):
            code, name = future_to_stock[future]
            try:
                data = future.result()
                if not data.empty:
                    all_stocks_data[(code, name)] = data
            except Exception as exc:
                # Exception already logged in fetch_single_stock_data, just pass here
                pass 
    
    return all_stocks_data

if __name__ == '__main__':
    # This block is for independent testing of data_fetcher_new.py
    # If run standalone, ensure basic logging for standalone execution
    # This check ensures logging is only configured if it hasn't been by main.py
    if not logging.getLogger().handlers: 
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(name)s - [Stock: %(stock)s] - [Strategy: %(strategy)s] - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('data_fetcher_standalone.log', encoding='utf-8')
            ]
        )
    
    logger.info("开始示例数据获取...")
    sample_stocks = [
        ("000001", "平安银行"), ("600036", "招商银行"), ("000651", "格力电器"),
        ("600519", "贵州茅台"), ("002594", "比亚迪"), ("603193", "永和股份"),
        ("300001", "特锐德"), ("688001", "华兴源创"), # Example with ChiNext and STAR, will be filtered out by workflow
        ("000002", "万科A") # Another sample
    ]
    
    # Use a specific cache directory for standalone test if needed
    test_cache_dir = "test_stock_data_cache"
    fetched_data = run(sample_stocks, start_date="20240101", cache_dir=test_cache_dir)
    logger.info(f"示例数据获取完成，共获取 {len(fetched_data)} 支股票数据到 {test_cache_dir}。")
    
    if ("000001", "平安银行") in fetched_data:
        print("\n平安银行最新数据：")
        print(fetched_data[("000001", "平安银行")].tail())
    if ("603193", "永和股份") in fetched_data:
        print("\n永和股份最新数据：")
        print(fetched_data[("603193", "永和股份")].tail())
    if ("000002", "万科A") in fetched_data:
        print("\n万科A最新数据：")
        print(fetched_data[("000002", "万科A")].tail())