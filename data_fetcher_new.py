# data_fetcher_new.py
import akshare as ak
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
import datetime
from ratelimit import limits, sleep_and_retry
import sys # 导入sys模块
from tqdm import tqdm # 导入tqdm

logger = logging.getLogger(__name__)

# 配置日志 (确保同时输出到文件和控制台)
if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [Stock: %(stock)s] - [Strategy: %(strategy)s] - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# 配置数据缓存路径和格式
CACHE_DIR = "stock_data_cache"
CACHE_FORMAT = "parquet" # 或者 "csv"，根据你的偏好和库安装情况
os.makedirs(CACHE_DIR, exist_ok=True)

@sleep_and_retry
@limits(calls=5, period=60) 
def fetch_single_stock_data(stock_code, stock_name, start_date_str="20230101"):
    file_name = f"{stock_code}.{CACHE_FORMAT}"
    file_path = os.path.join(CACHE_DIR, file_name)
    
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    min_fetch_start_date = datetime.datetime.strptime(start_date_str, '%Y%m%d').date()

    df = pd.DataFrame()

    if os.path.exists(file_path):
        try:
            if CACHE_FORMAT == "parquet":
                df = pd.read_parquet(file_path)
            elif CACHE_FORMAT == "csv":
                df = pd.read_csv(file_path)
            
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values(by='日期').reset_index(drop=True)

            if not df.empty and df['日期'].max().date() >= yesterday:
                if df['日期'].min().date() <= min_fetch_start_date:
                    logger.debug(f"从缓存加载 {stock_name}({stock_code}) 数据。", extra={'stock': stock_code, 'strategy': '数据获取'})
                    return df
                else:
                    logger.info(f"缓存 {stock_name}({stock_code}) 数据起始日期 ({df['日期'].min().strftime('%Y-%m-%d')}) 早于请求的 {min_fetch_start_date.strftime('%Y-%m-%d')}，尝试更新。", extra={'stock': stock_code, 'strategy': '数据获取'})
            else:
                logger.info(f"缓存 {stock_name}({stock_code}) 数据过期或不完整，将重新下载。", extra={'stock': stock_code, 'strategy': '数据获取'})
        except Exception as e:
            logger.warning(f"加载缓存文件 {file_path} 失败: {e}，将重新下载。", extra={'stock': stock_code, 'strategy': '数据获取'})
            if os.path.exists(file_path):
                os.remove(file_path)

    logger.info(f"从AKShare下载 {stock_name}({stock_code}) 数据...", extra={'stock': stock_code, 'strategy': '数据获取'})
    try:
        new_data_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date_str, adjust="hfq")
        
        if new_data_df.empty:
            logger.warning(f"AKShare未能获取到 {stock_name}({stock_code}) 的历史数据。", extra={'stock': stock_code, 'strategy': '数据获取'})
            return pd.DataFrame()

        column_mapping = {
            '日期': '日期', '开盘': '开盘', '收盘': '收盘', '最高': '最高',
            '最低': '最低', '成交量': '成交量', '成交额': '成交额', '换手率': '换手率'
        }
        new_data_df = new_data_df.rename(columns=column_mapping)
        required_cols = list(column_mapping.values())
        new_data_df = new_data_df[required_cols].copy()

        new_data_df['日期'] = pd.to_datetime(new_data_df['日期'])
        new_data_df = new_data_df.sort_values(by='日期').reset_index(drop=True)

        if not df.empty:
            combined_df = pd.concat([df, new_data_df]).drop_duplicates(subset=['日期']).sort_values(by='日期').reset_index(drop=True)
            df = combined_df
            logger.info(f"成功更新 {stock_name}({stock_code}) 数据，总行数: {len(df)}。", extra={'stock': stock_code, 'strategy': '数据获取'})
        else:
            df = new_data_df
            logger.info(f"成功下载 {stock_name}({stock_code}) 数据，总行数: {len(df)}。", extra={'stock': stock_code, 'strategy': '数据获取'})

        if CACHE_FORMAT == "parquet":
            df.to_parquet(file_path, index=False)
        elif CACHE_FORMAT == "csv":
            df.to_csv(file_path, index=False)
        
        return df

    except Exception as e:
        logger.error(f"下载或处理 {stock_name}({stock_code}) 数据失败: {e}", extra={'stock': stock_code, 'strategy': '数据获取'})
        return pd.DataFrame()

def run(stocks_list, start_date="20230101"):
    all_stocks_data = {}
    
    with ThreadPoolExecutor(max_workers=5) as executor: 
        future_to_stock = {
            executor.submit(fetch_single_stock_data, code, name, start_date): (code, name)
            for code, name in stocks_list
        }
        # 使用 tqdm 包裹 as_completed，显示数据下载进度条
        for future in tqdm(as_completed(future_to_stock), 
                           total=len(future_to_stock), 
                           desc="Fetching stock data", 
                           unit="stock"):
            code, name = future_to_stock[future]
            try:
                data = future.result()
                if not data.empty:
                    all_stocks_data[(code, name)] = data
            except Exception as exc:
                # 异常已经在 fetch_single_stock_data 中记录，这里只需略过
                pass 
    
    return all_stocks_data

if __name__ == '__main__':
    # 为了在独立运行 data_fetcher_new.py 时也能看到日志，重新配置一下
    # 注意：在主流程中，这个basicConfig会被主logger的配置覆盖
    # 但是在独立运行此文件时，它会生效
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler) # 避免重复配置
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [Stock: %(stock)s] - [Strategy: %(strategy)s] - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout), # 输出到控制台
            # logging.FileHandler('data_fetcher.log', encoding='utf-8') # 如果需要独立的日志文件
        ]
    )

    logger.info("开始示例数据获取...")
    sample_stocks = [
        ("000001", "平安银行"), ("600036", "招商银行"), ("000651", "格力电器"),
        ("600519", "贵州茅台"), ("002594", "比亚迪"), ("603193", "永和股份"),
        ("300001", "特锐德"), ("688001", "华兴源创"),
    ]
    
    fetched_data = run(sample_stocks, start_date="20240101")
    logger.info(f"示例数据获取完成，共获取 {len(fetched_data)} 支股票数据。")
    
    if ("000001", "平安银行") in fetched_data:
        print("\n平安银行最新数据：")
        print(fetched_data[("000001", "平安银行")].tail())
    if ("603193", "永和股份") in fetched_data:
        print("\n永和股份最新数据：")
        print(fetched_data[("603193", "永和股份")].tail())