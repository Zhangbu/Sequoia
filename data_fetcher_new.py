# data_fetcher_new.py
import akshare as ak
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
import datetime
from ratelimit import limits, sleep_and_retry # Ensure ratelimit is installed
import sys
from tqdm import tqdm
import traceback

logger = logging.getLogger(__name__)

# --- Constants ---
DEFAULT_CACHE_FORMAT = "parquet" # Parquet is generally better for DataFrames
MIN_DATA_LENGTH_FOR_STRATEGY = 30 # Minimum rows for data to be considered useful
AKSHARE_CALLS_LIMIT = 5
AKSHARE_PERIOD_SECONDS = 60 # 5 calls per 60 seconds for AKShare

# --- Helper Function for Trading Dates ---
_trading_calendar = None

def get_trading_calendar(start_year=None, end_year=None):
    """
    Fetches and caches the A-share trading calendar from Akshare.
    """
    global _trading_calendar
    today = datetime.date.today()
    if start_year is None:
        start_year = today.year - 1 # Default to 5 years back
    if end_year is None:
        end_year = today.year + 1 # Default to next year

    # Simple in-memory cache for the calendar for the duration of the run
    if _trading_calendar is not None:
        # Check if the cached calendar covers the required range (simplified check)
        min_date_in_cache = pd.to_datetime(_trading_calendar['trade_date'].min()).year
        max_date_in_cache = pd.to_datetime(_trading_calendar['trade_date'].max()).year
        if min_date_in_cache <= start_year and max_date_in_cache >= end_year:
            return _trading_calendar

    try:
        logger.debug(f"Fetching trading calendar from Akshare for years {start_year}-{end_year}")
        # Fetch for a range of years to reduce calls
        # Note: Akshare's tool_trade_date_hist_sina returns date strings
        calendar_df_list = []
        for year in range(start_year, end_year + 1):
            try:
                # Using a more reliable source if available, or sticking to sina for simplicity
                # tool_trade_date_hist_df = ak.tool_trade_date_hist_ems() # Alternative
                tool_trade_date_hist_df = ak.tool_trade_date_hist_sina() # Date format YYYY-MM-DD
                tool_trade_date_hist_df['trade_date'] = pd.to_datetime(tool_trade_date_hist_df['trade_date'])
                calendar_df_list.append(tool_trade_date_hist_df[tool_trade_date_hist_df['trade_date'].dt.year == year])
            except Exception as e:
                logger.warning(f"Failed to fetch trading calendar for year {year} from Akshare: {e}. Trying to continue.")
        
        if not calendar_df_list:
            logger.error("Could not fetch any trading calendar data from Akshare.")
            return pd.DataFrame({'trade_date': []}) # Return empty if fetch fails

        _trading_calendar = pd.concat(calendar_df_list).drop_duplicates(subset=['trade_date']).sort_values('trade_date').reset_index(drop=True)
        _trading_calendar['trade_date'] = pd.to_datetime(_trading_calendar['trade_date']) # Ensure datetime
        return _trading_calendar
    except Exception as e:
        logger.error(f"Error fetching trading calendar: {e}\n{traceback.format_exc()}")
        return pd.DataFrame({'trade_date': []}) # Return empty on failure

def get_latest_trading_date(reference_date=None, calendar_df=None):
    """
    Gets the latest A-share trading date at or before the reference_date.
    If reference_date is None, uses today.
    If market is open (e.g., after 3 PM), it could be reference_date itself if it's a trading day.
    Otherwise, it's the most recent trading day before or on reference_date.
    """
    if reference_date is None:
        reference_date = datetime.date.today()
    else:
        if isinstance(reference_date, datetime.datetime):
            reference_date = reference_date.date()

    if calendar_df is None or calendar_df.empty:
        # Attempt to fetch calendar for a reasonable default range
        current_year = reference_date.year
        calendar_df = get_trading_calendar(start_year=current_year -1, end_year=current_year)

    if calendar_df.empty:
        logger.warning("Trading calendar is empty. Falling back to weekday logic for latest trading date.")
        # Fallback: if today is weekday, assume it's trading day, else Friday
        if reference_date.weekday() < 5: # Monday = 0, Friday = 4
            return reference_date
        else: # Weekend
            return reference_date - datetime.timedelta(days=reference_date.weekday() - 4) # Last Friday

    # Ensure 'trade_date' is datetime.date for comparison
    trading_dates = pd.to_datetime(calendar_df['trade_date']).dt.date
    
    # Filter dates at or before the reference_date
    past_or_current_trading_dates = trading_dates[trading_dates <= reference_date]
    
    if not past_or_current_trading_dates.empty:
        return past_or_current_trading_dates.max()
    else:
        # Should not happen if calendar is fetched correctly and reference_date is reasonable
        logger.warning(f"No trading dates found at or before {reference_date}. Using weekday fallback.")
        if reference_date.weekday() < 5: return reference_date
        else: return reference_date - datetime.timedelta(days=reference_date.weekday() - 4)


@sleep_and_retry
@limits(calls=AKSHARE_CALLS_LIMIT, period=AKSHARE_PERIOD_SECONDS)
def fetch_single_stock_data(stock_code, stock_name, start_date_config_str="20240101", cache_dir="stock_data_cache", cache_format=DEFAULT_CACHE_FORMAT, trading_calendar=None):
    """
    Fetches historical daily stock data and manages caching.
    start_date_config_str: The earliest date required by configuration (YYYYMMDD).
    cache_dir: Directory to store cached files.
    cache_format: 'parquet' or 'csv'.
    trading_calendar: Pre-fetched trading calendar DataFrame.
    """
    os.makedirs(cache_dir, exist_ok=True)
    file_name = f"{stock_code}.{cache_format}"
    file_path = os.path.join(cache_dir, file_name)

    # Determine the latest date for which data should ideally exist
    # If market is generally considered closed (e.g. before 3 PM or on non-trading day), expect data up to previous trading day.
    # If market is open/just closed (e.g. after 3 PM on a trading day), expect data up to today.
    now_dt = datetime.datetime.now()
    if now_dt.hour >= 15 : # After typical market close
        reference_date_for_latest = now_dt.date()
    else: # Before market close or during trading
        reference_date_for_latest = now_dt.date() - datetime.timedelta(days=1)
    
    latest_expected_data_date = get_latest_trading_date(reference_date_for_latest, trading_calendar)
    logger.debug(f"[{stock_name}({stock_code})] Latest expected data date: {latest_expected_data_date}", extra={'stock': stock_code, 'strategy': '数据获取'})


    min_fetch_start_date_obj = datetime.datetime.strptime(start_date_config_str, '%Y%m%d').date()

    cached_df = pd.DataFrame()
    needs_full_fetch = False # Flag to indicate if a full re-fetch from min_fetch_start_date_obj is needed

    if os.path.exists(file_path):
        try:
            if cache_format == "parquet":
                cached_df = pd.read_parquet(file_path)
            elif cache_format == "csv":
                cached_df = pd.read_csv(file_path, parse_dates=['日期']) # Ensure '日期' is parsed

            if not cached_df.empty:
                # Ensure '日期' column is datetime.date and sorted
                cached_df['日期'] = pd.to_datetime(cached_df['日期']).dt.date # Convert to date objects for comparison
                cached_df = cached_df.sort_values(by='日期').reset_index(drop=True)
                
                last_cached_date_obj = cached_df['日期'].max()
                first_cached_date_obj = cached_df['日期'].min()

                if last_cached_date_obj >= latest_expected_data_date:
                    if first_cached_date_obj <= min_fetch_start_date_obj:
                        logger.info(f"[{stock_name}({stock_code})] Data up-to-date and history complete in cache. Last cached: {last_cached_date_obj}", extra={'stock': stock_code, 'strategy': '数据获取'})
                        return cached_df # Cache is perfect
                    else:
                        logger.info(f"[{stock_name}({stock_code})] Cache up-to-date ({last_cached_date_obj}), but history starts late ({first_cached_date_obj} vs {min_fetch_start_date_obj}). Will fetch older data.", extra={'stock': stock_code, 'strategy': '数据获取'})
                        # We need to fetch from the configured start date up to the beginning of our cache
                        # to fill the gap, then merge. Or simpler: re-fetch all if logic is complex.
                        # For simplicity now, if history is incomplete, trigger a full re-fetch from configured start.
                        needs_full_fetch = True
                        start_date_for_api_fetch = min_fetch_start_date_obj
                else: # Cache is outdated
                    logger.info(f"[{stock_name}({stock_code})] Cache outdated. Last cached: {last_cached_date_obj}, expected: {latest_expected_data_date}. Will update.", extra={'stock': stock_code, 'strategy': '数据获取'})
                    # Fetch data from the day after the last cached date
                    start_date_for_api_fetch = last_cached_date_obj + datetime.timedelta(days=1)
            else: # Cached file exists but is empty
                logger.warning(f"Cache file {file_path} is empty. Triggering full download.", extra={'stock': stock_code, 'strategy': '数据获取'})
                needs_full_fetch = True
                start_date_for_api_fetch = min_fetch_start_date_obj
                cached_df = pd.DataFrame() # Ensure it's empty for merging logic
        except Exception as e:
            logger.warning(f"Failed to load or parse cache file {file_path}: {e}. Triggering full download.", extra={'stock': stock_code, 'strategy': '数据获取'})
            if os.path.exists(file_path):
                try: os.remove(file_path)
                except OSError as oe: logger.error(f"Error removing corrupted cache file {file_path}: {oe}")
            needs_full_fetch = True
            start_date_for_api_fetch = min_fetch_start_date_obj
            cached_df = pd.DataFrame()
    else: # No cache file
        logger.info(f"No cache file found for {stock_name}({stock_code}). Triggering full download.", extra={'stock': stock_code, 'strategy': '数据获取'})
        needs_full_fetch = True
        start_date_for_api_fetch = min_fetch_start_date_obj

    # If a full fetch is needed, existing cached_df is irrelevant for the API call, but might be used if API fails
    if needs_full_fetch:
        start_date_for_api_fetch = min_fetch_start_date_obj
        # if we are doing a full fetch, we should ideally clear the old cache for merging
        # but we'll rely on concat + drop_duplicates to handle it.
        # Or, if full fetch is triggered, consider `cached_df = pd.DataFrame()` so we don't merge with old partial history.
        # For now, if needs_full_fetch is true because cache was bad/empty, cached_df is already empty.
        # If it was due to incomplete history, we might want to discard the old cached_df before full fetch.
        # Let's simplify: if needs_full_fetch, we aim to replace cache.
        if os.path.exists(file_path) and needs_full_fetch : # Remove if doing a full new fetch
            logger.debug(f"[{stock_name}({stock_code})] Performing full fetch, removing old cache file if exists.", extra={'stock': stock_code, 'strategy': '数据获取'})
            # We will overwrite it anyway, but this makes the state cleaner if API fails mid-way
            # No, let's keep the old cache in case the new full fetch fails, then we can return the old one.
            # The merge logic will handle combining. If `needs_full_fetch` was due to incomplete history,
            # `cached_df` still holds the up-to-date part.

    # If start_date_for_api_fetch is later than today, no need to fetch
    if start_date_for_api_fetch.strftime('%Y%m%d') > datetime.date.today().strftime('%Y%m%d') :
        logger.info(f"[{stock_name}({stock_code})] Calculated API fetch start ({start_date_for_api_fetch}) is in the future. No new data to fetch.", extra={'stock': stock_code, 'strategy': '数据获取'})
        return cached_df # Return existing cache, it might be the most up-to-date available

    logger.info(f"Fetching data for {stock_name}({stock_code}) from Akshare (Start: {start_date_for_api_fetch.strftime('%Y%m%d')}, Adjust: qfq)...", extra={'stock': stock_code, 'strategy': '数据获取'})
    
    try:
        # AKShare expects YYYYMMDD string for dates
        new_data_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date_for_api_fetch.strftime('%Y%m%d'), adjust="qfq")
        
        if new_data_df.empty:
            # This can happen if start_date_for_api_fetch is a non-trading day or today with no data yet.
            logger.warning(f"Akshare returned no new data for {stock_name}({stock_code}) from {start_date_for_api_fetch.strftime('%Y%m%d')}.", extra={'stock': stock_code, 'strategy': '数据获取'})
            # If cache was up-to-date but history incomplete, and new fetch yields nothing, we still return original cache.
            return cached_df # Return whatever cache we had, it's the best we have.

        # Standardize columns (AKShare column names are usually stable but good practice)
        column_mapping = {
            '日期': '日期', '开盘': '开盘', '收盘': '收盘', '最高': '最高', '最低': '最低',
            '成交量': '成交量', '成交额': '成交额', '振幅': '振幅', '涨跌幅': '涨跌幅',
            '涨跌额': '涨跌额', '换手率': '换手率'
            # '股票代码' is not returned by stock_zh_a_hist, it's an input.
        }
        # Select and rename columns that are present in new_data_df
        cols_to_keep = [col for col in column_mapping.keys() if col in new_data_df.columns]
        new_data_df = new_data_df[cols_to_keep].rename(columns=column_mapping).copy()
        new_data_df['股票代码'] = stock_code # Add stock code column

        new_data_df['日期'] = pd.to_datetime(new_data_df['日期']).dt.date # Convert to date objects
        new_data_df = new_data_df.sort_values(by='日期').reset_index(drop=True)

        # Data Validation & Cleaning
        numeric_cols = ['开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        for col in numeric_cols:
            if col in new_data_df.columns:
                if new_data_df[col].isnull().any():
                    logger.debug(f"[{stock_name}({stock_code})] NaN found in downloaded data column '{col}'. Applying ffill/bfill/0.", extra={'stock': stock_code, 'strategy': '数据获取'})
                    new_data_df[col] = pd.to_numeric(new_data_df[col], errors='coerce') # Ensure numeric
                    new_data_df[col].fillna(method='ffill', inplace=True)
                    new_data_df[col].fillna(method='bfill', inplace=True)
                    new_data_df[col].fillna(0, inplace=True) # If all were NaN

        # Combine with existing cache if any
        if not cached_df.empty and not needs_full_fetch: # Only merge if not a full_fetch scenario where cache should be replaced
            # Ensure consistent date types before concat
            cached_df['日期'] = pd.to_datetime(cached_df['日期']).dt.date
            new_data_df['日期'] = pd.to_datetime(new_data_df['日期']).dt.date
            
            df_to_save = pd.concat([cached_df, new_data_df]).drop_duplicates(subset=['日期'], keep='last').sort_values(by='日期').reset_index(drop=True)
            logger.info(f"[{stock_name}({stock_code})] Successfully merged new data with cache. Total rows: {len(df_to_save)}", extra={'stock': stock_code, 'strategy': '数据获取'})
        else: # This is new data, or full fetch replacing old.
            df_to_save = new_data_df
            logger.info(f"[{stock_name}({stock_code})] Successfully fetched new data. Total rows: {len(df_to_save)}", extra={'stock': stock_code, 'strategy': '数据获取'})

        # Final check on combined data length
        if df_to_save.empty or len(df_to_save) < MIN_DATA_LENGTH_FOR_STRATEGY:
            logger.warning(f"[{stock_name}({stock_code})] Data after fetch/merge is too short ({len(df_to_save)} rows). May not be usable.", extra={'stock': stock_code, 'strategy': '数据获取'})
            # Decide: return this short data, or return old cache if it was better?
            # For now, return the newly processed (possibly short) data.
            if os.path.exists(file_path): # Save even if short, so we don't re-fetch same short data
                if cache_format == "parquet": df_to_save.to_parquet(file_path, index=False)
                else: df_to_save.to_csv(file_path, index=False)
            return df_to_save

        # Save to cache
        if cache_format == "parquet":
            df_to_save.to_parquet(file_path, index=False)
        elif cache_format == "csv":
            df_to_save.to_csv(file_path, index=False)
        
        return df_to_save

    except Exception as e:
        logger.error(f"Critical error during AKShare fetch or processing for {stock_name}({stock_code}): {e}\n{traceback.format_exc()}", extra={'stock': stock_code, 'strategy': '数据获取'})
        # On critical failure, return the original cached_df if it existed and was valid, otherwise empty
        return cached_df if not cached_df.empty else pd.DataFrame()


def run(stocks_list, start_date_config_str="20240101", cache_dir="stock_data_cache", cache_format=DEFAULT_CACHE_FORMAT, max_workers=5):
    """
    Runs data fetching for a list of stocks using a thread pool.
    stocks_list: list of (code, name) tuples.
    """
    all_stocks_data = {}
    os.makedirs(cache_dir, exist_ok=True)

    # Fetch trading calendar once for all threads
    current_year = datetime.date.today().year
    # Fetch calendar for a slightly wider range to be safe, e.g., configured start year to current year + 1
    try:
        config_start_year = datetime.datetime.strptime(start_date_config_str, "%Y%m%d").year
    except ValueError:
        config_start_year = current_year - 5 # Fallback if start_date_config_str is weird
        
    trading_cal = get_trading_calendar(start_year=min(config_start_year, current_year - 5), end_year=current_year + 1)
    if trading_cal.empty:
        logger.error("Failed to get trading calendar, data fetching accuracy for 'latest_expected_date' might be reduced.")


    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_stock = {
            executor.submit(fetch_single_stock_data, code, name, start_date_config_str, cache_dir, cache_format, trading_cal): (code, name)
            for code, name in stocks_list
        }
        
        progress_bar = tqdm(as_completed(future_to_stock), total=len(future_to_stock), desc="Fetching stock data", unit="stock", file=sys.stdout, ncols=100)
        
        for future in progress_bar:
            code, name = future_to_stock[future]
            progress_bar.set_postfix_str(f"{name}({code})")
            try:
                data = future.result()
                if data is not None and not data.empty: # Ensure data is not None
                    all_stocks_data[(code, name)] = data
                elif data is None: # Explicitly None might mean an issue not returning DataFrame
                     logger.error(f"Fetching returned None for {name}({code}).", extra={'stock': code, 'strategy': '数据获取'})
                # Empty DataFrame case already logged inside fetch_single_stock_data
            except Exception as exc:
                # This catches exceptions from future.result() if not caught inside the thread func
                logger.error(f"Exception for {name}({code}) in thread execution: {exc}\n{traceback.format_exc()}", extra={'stock': stock_code, 'strategy': '数据获取'})
    
    return all_stocks_data

# --- Main for Standalone Testing ---
if __name__ == '__main__':
    # Setup basic logging if run as a script
    if not logging.getLogger().handlers:
        log_format = '%(asctime)s - %(levelname)s - %(name)s - [%(stock)s:%(strategy)s] - %(message)s'
        # Define a filter that adds default values for 'stock' and 'strategy' if they are missing
        class ContextFilter(logging.Filter):
            def filter(self, record):
                if not hasattr(record, 'stock'):
                    record.stock = 'NONE'
                if not hasattr(record, 'strategy'):
                    record.strategy = 'N/A'
                return True
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(log_format))
        console_handler.addFilter(ContextFilter())
        
        file_handler = logging.FileHandler('data_fetcher_standalone.log', encoding='utf-8', mode='w')
        file_handler.setFormatter(logging.Formatter(log_format))
        file_handler.addFilter(ContextFilter())
        
        logging.basicConfig(level=logging.DEBUG, handlers=[console_handler, file_handler])

    logger.info("Starting standalone data fetching example...")
    
    sample_stocks = [
        ("000001", "平安银行"), ("600036", "招商银行"), ("000651", "格力电器"),
        ("600519", "贵州茅台"), ("002594", "比亚迪"),  ("000002", "万科A"),
        ("sh600000", "浦发银行"), # Test with sh/sz prefix, akshare handles it
        ("sz002415", "海康威视"),
        ("603193", "永和股份"), # Test for a stock that might have less history
        ("000004", "国农科技"), # Example stock that might be ST or delisted to test robustness
        ("600004", "白云机场") # Another one
    ]
    
    test_cache_dir = os.path.join(os.path.dirname(__file__), "test_stock_data_cache_new") # Place cache in script dir
    start_date_to_fetch = "20240101" # Fetch data from this date onwards

    fetched_data_map = run(sample_stocks, start_date_config_str=start_date_to_fetch, cache_dir=test_cache_dir, max_workers=3)
    
    logger.info(f"Standalone data fetching complete. Fetched {len(fetched_data_map)} stocks into {test_cache_dir}.")
    
    for (code, name), df in fetched_data_map.items():
        if df is not None and not df.empty:
            logger.info(f"Stock: {name}({code}), Data rows: {len(df)}, Last date: {df['日期'].max()}, First date: {df['日期'].min()}")
            # print(f"\n--- {name} ({code}) ---")
            # print(df.tail(3))
        else:
            logger.warning(f"No data returned for {name}({code}).")

    # Example: Check a specific stock
    if ("000001", "平安银行") in fetched_data_map:
        pa_bank_data = fetched_data_map[("000001", "平安银行")]
        if pa_bank_data is not None and not pa_bank_data.empty:
            print("\n平安银行 (000001) - Last 5 days from fetched data:")
            print(pa_bank_data.tail())