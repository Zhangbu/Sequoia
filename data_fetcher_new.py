# -*- encoding: UTF-8 -*-

import akshare as ak
import logging
import talib as tl
import concurrent.futures
import time
import os
import pandas as pd
from datetime import datetime, timedelta
import random # 用于随机延迟

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 缓存配置 ---
CACHE_DIR = "stock_data_cache"
CACHE_EXPIRATION_DAYS = 7 # 缓存有效期，例如7天

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def get_cache_path(stock_code):
    return os.path.join(CACHE_DIR, f"{stock_code}.parquet")

# --- fetch 函数：核心数据获取与处理 ---
def fetch(code_name):
    stock = code_name[0]
    cache_path = get_cache_path(stock)

    # 1. 检查本地缓存
    if os.path.exists(cache_path):
        try:
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(cache_path))
            if datetime.now() - file_mod_time < timedelta(days=CACHE_EXPIRATION_DAYS):
                cached_data = pd.read_parquet(cache_path)
                logging.info(f"从缓存加载数据：{stock} - {code_name[1]}")
                return cached_data
            else:
                logging.info(f"缓存过期，将重新获取数据：{stock} - {code_name[1]}")
        except Exception as e:
            logging.warning(f"加载缓存文件 {cache_path} 失败: {e}，将重新获取数据。")

    # 2. 从网络获取数据 (如果缓存无效或不存在)
    # 引入随机延迟，防止IP被封
    sleep_time = random.uniform(0.5, 2.0) # 每次请求间隔0.5到2秒
    logging.info(f"准备获取数据：{stock} - {code_name[1]} (延迟 {sleep_time:.2f}秒)")
    time.sleep(sleep_time)

    try:
        data = ak.stock_zh_a_hist(symbol=stock, period="daily", start_date="20250101", adjust="qfq")
    except Exception as e:
        logging.error(f"获取股票 {stock} - {code_name[1]} 数据失败: {e}")
        return None

    if data is None or data.empty:
        logging.debug(f"股票：{stock} - {code_name[1]} 没有数据，略过...")
        return None

    # 3. 数据处理
    data['p_change'] = tl.ROC(data['收盘'], 1)
    data = data.astype({'成交量': 'double'}) # 在这里完成类型转换

    # 4. 保存到缓存
    try:
        data.to_parquet(cache_path, index=False)
        logging.info(f"数据保存到缓存：{stock} - {code_name[1]}")
    except Exception as e:
        logging.error(f"保存缓存文件 {cache_path} 失败: {e}")

    return data

# --- run 函数：并发调度 ---
def run(stocks):
    stocks_data = {}
    # 降低并发数，例如 5-10，具体数值根据测试结果调整
    max_workers_count = min(len(stocks), 5) # 限制最大并发，或者你希望的固定值
    logging.info(f"启动数据获取，最大并发数：{max_workers_count}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers_count) as executor:
        # 使用字典保持股票与Future的映射，方便获取结果时关联
        future_to_stock_name = {executor.submit(fetch, stock): stock for stock in stocks}

        for future in concurrent.futures.as_completed(future_to_stock_name):
            stock_code, stock_name = future_to_stock_name[future]
            try:
                data = future.result()
                if data is not None:
                    stocks_data[(stock_code, stock_name)] = data
            except Exception as exc:
                logging.error(f"处理股票 {stock_name}({stock_code}) 时发生异常: {exc}")

    logging.info(f"所有股票数据获取完成，成功获取 {len(stocks_data)} 支股票数据。")
    return stocks_data

# --- 示例用法 ---
if __name__ == "__main__":
    # 示例股票列表，通常这是从某个地方获取的
    # stock[0] 是代码，stock[1] 是名称
    sample_stocks = [
        ("000001", "平安银行"),
        ("600000", "浦发银行"),
        ("000002", "万科A"),
        ("600519", "贵州茅台"),
        ("000008", "神州高铁"),
        ("000009", "中国宝安"),
        ("600004", "白云机场"),
        ("600005", "武钢股份"),
        ("000010", "美丽生态"),
        ("600006", "东风汽车"),
        ("600007", "上海实业"),
        ("600008", "首创股份"),
        ("600009", "上海机场"),
        ("600010", "包钢股份"),
        # ... 更多股票 ...
    ]

    # 可以从某个文件或API获取完整的股票列表
    # try:
    #     all_stocks = ak.stock_zh_a_spot_em()[['代码', '名称']].values.tolist()
    # except Exception as e:
    #     logging.error(f"获取所有A股列表失败: {e}")
    #     all_stocks = [] # 或者使用一个预定义的列表

    # 运行数据获取
    result_data = run(sample_stocks) # 或者 run(all_stocks)
    print("\n--- 获取结果摘要 ---")
    for (code, name), df in result_data.items():
        print(f"股票: {name} ({code}), 数据行数: {len(df)}")
    print(f"总共获取到 {len(result_data)} 支股票的数据。")