# # -*- encoding: UTF-8 -*-

# import data_fetcher_new # 你的数据获取模块
# import settings # 你的设置模块
# import strategy.enter as enter # 原始的策略模块
# import newStrategy.enter as newEnter
# import newStrategy.keep_increasing as newKeep_increasing
# import newStrategy.parking_apron as newParking_apron
# import newStrategy.backtrace_ma250 as newBacktrace_ma250
# import newStrategy.breakthrough_platform as newBreakthrough_platform
# import newStrategy.low_backtrace_increase as newLow_backtrace_increase
# import newStrategy.turtle_trade as newTurtle_trade
# import newStrategy.high_tight_flag as newHigh_tight_flag
# import newStrategy.climax_limitdown as newClimax_limitdown
# import newStrategy.limit_up as limit_up
# import newStrategy.new as newStrategynew
# import newStrategy.my_short_term_strategy as my_short_term_strategy # 导入新的策略文件

# import akshare as ak
# import push # 你的推送模块
# import logging
# import datetime
# import pandas as pd
# import time # 用于随机延迟
# import random # 用于随机延迟
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from ratelimit import limits, sleep_and_retry

# # 配置日志
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - [Stock: %(stock)s] - [Strategy: %(strategy)s] - %(message)s'
# )
# logger = logging.getLogger(__name__) # 获取一个日志器实例

# def prepare():
#     titleMsg = ""
#     selected_limit_up_stocks = []
#     logger.info("Process start", extra={'stock': 'NONE', 'strategy': 'NONE'})
#     try:
#         all_data = ak.stock_zh_a_spot_em() # 获取实时行情
        
#         # 过滤掉科创板(688), 创业板(300), ST/*ST, 低市值
#         # 确保 all_data 中包含 '代码', '名称', '总市值'
#         if not {'代码', '名称', '总市值', '涨跌幅'}.issubset(all_data.columns):
#             logger.error("ak.stock_zh_a_spot_em() 返回的数据缺少必要列。", extra={'stock': 'NONE', 'strategy': 'NONE'})
#             return "", [] # 返回空值避免后续错误

#         filtered_subset = all_data[['代码', '名称', '总市值', '涨跌幅']]
        
#         subset1 = filtered_subset[
#             (~filtered_subset['代码'].str.startswith('688')) &
#             (~filtered_subset['代码'].str.startswith('300')) &
#             (~filtered_subset['名称'].str.contains('ST', case=False, na=False)) &
#             (filtered_subset['总市值'] >= 10_000_000_000) # 100亿
#         ]
#         subset = subset1[['代码', '名称']]
#         # 将 DataFrame 转换为 (代码, 名称) 元组的列表
#         stocks = [tuple(x) for x in subset.values]
        
#         # 市场统计
#         titleMsg = statistics(all_data, stocks)
        
#         # 定义所有要运行的策略
#         strategies = {
#             '东方财富短线策略': my_short_term_strategy.check_enter, # <-- 这里是关键！
#         }

#         # 周一特殊处理：可以启用或禁用特定策略
#         if datetime.datetime.now().weekday() == 0: # 周一
#             # 示例：周一可能更关注均线多头
#             # strategies['均线多头'] = newKeep_increasing.check # 这行已经是默认行为，如果想更强调可以保留
#             pass # 暂时不做特殊处理，所有策略都跑

#         # 执行所有策略并收集结果
#         titleMsg, selected_limit_up_stocks = process(stocks, strategies, titleMsg, selected_limit_up_stocks)

#         logger.info(f"符合涨停板次日溢价策略的stock：{len(selected_limit_up_stocks)} 只", extra={'stock': 'NONE', 'strategy': '涨停板次日溢价'})
        
#         # 如果是周一，并且有涨停板次日溢价的stock，则进行回测
#         if selected_limit_up_stocks and datetime.datetime.now().weekday() == 0:
#             logger.info("开始回测涨停板次日溢价策略", extra={'stock': 'NONE', 'strategy': '限价板回测'})
#             backtest_results = backtest_selected_stocks(selected_limit_up_stocks)
#             titleMsg += format_backtest_results(backtest_results)

#         # 推送消息
#         if titleMsg:
#             max_length = 4000
#             print(titleMsg) # 打印到控制台
#             if len(titleMsg) > max_length:
#                 chunks = [titleMsg[i:i+max_length] for i in range(0, len(titleMsg), max_length)]
#                 for chunk in chunks:
#                     push.strategy(chunk)
#             else:
#                 push.strategy(titleMsg)
#         else:
#             push.strategy("无符合条件的策略结果")

#     except Exception as e:
#         logger.error(f"程序执行失败: {e}", extra={'stock': 'NONE', 'strategy': 'NONE'})
#         push.strategy(f"程序执行失败: {e}")

#     logger.info("Process end", extra={'stock': 'NONE', 'strategy': 'NONE'})
#     return titleMsg, selected_limit_up_stocks

# # 限速装饰器，防止IP被封
# @sleep_and_retry
# @limits(calls=10, period=60) # 每60秒最多调用10次，根据akshare的限制调整
# def call_strategy_check(stock_info, strategy_func, end_date):
#     """
#     一个包装函数，用于在线程池中调用策略函数，并处理日志。
#     stock_info 是 (代码, 名称) 元组
#     """
#     stock_code, stock_name = stock_info[0], stock_info[1] # stock_info 是 (code, name, data)
#     stock_data_df = stock_info[2] # 真正的DataFrame数据
    
#     try:
#         # 你的策略函数现在接收 (code_tuple, data_df, end_date)
#         # 例如: my_short_term_strategy.check_enter(("000001", "平安银行"), df, end_date)
#         result = strategy_func((stock_code, stock_name), stock_data_df, end_date=end_date)
#         return (stock_code, stock_name), result
#     except Exception as e:
#         logger.error(f"策略函数执行失败: {e}", extra={'stock': stock_code, 'strategy': strategy_func.__module__})
#         return (stock_code, stock_name), False

# def process(stocks, strategies, titleMsg, selected_limit_up_stocks):
#     try:
#         # 获取所有stock的历史数据
#         # data_fetcher_new.run(stocks) 需要返回 {("代码", "名称"): DataFrame} 格式
#         # 且 DataFrame 必须包含 '日期', '收盘', '开盘', '最高', '最低', '成交量', '成交额', '换手率'
#         logger.info(f"开始获取 {len(stocks)} 支stock的历史数据...", extra={'stock': 'NONE', 'strategy': '数据获取'})
#         stocks_data = data_fetcher_new.run(stocks) # 假设 data_fetcher_new.run 已经优化并返回正确格式
#         logger.info(f"历史数据获取完成，成功获取 {len(stocks_data)} 支stock数据。", extra={'stock': 'NONE', 'strategy': '数据获取'})

#         # 确定分析的结束日期
#         end_date_str = settings.config.get('end_date', datetime.datetime.now().strftime('%Y-%m-%d'))
#         end_date_ts = pd.Timestamp(end_date_str)
#         logger.info(f"当前分析日期为: {end_date_ts.strftime('%Y-%m-%d')}", extra={'stock': 'NONE', 'strategy': '日期'})
        
#         for strategy_name, strategy_func in strategies.items():
#             logger.info(f"开始运行策略: {strategy_name}", extra={'stock': 'NONE', 'strategy': strategy_name})
            
#             # 存储符合当前策略的stock及数据
#             current_strategy_results = {}
            
#             # 使用ThreadPoolExecutor并发执行策略
#             # max_workers 数量可以根据系统资源和API限速进一步调整
#             # 考虑到akshare的调用限速，这里的并发数要保守
#             with ThreadPoolExecutor(max_workers=5) as executor: # 适当降低并发度
#                 # future_to_stock 映射: {Future对象: (stock代码, stock名称)}
#                 future_to_stock_info = {
#                     executor.submit(call_strategy_check, (code_name[0], code_name[1], data), strategy_func, end_date_ts): code_name
#                     for code_name, data in stocks_data.items() # stocks_data.items() 已经返回 (code, name): df
#                 }
                
#                 for future in as_completed(future_to_stock_info):
#                     original_stock_info = future_to_stock_info[future] # (code, name)
#                     try:
#                         (code, name), result = future.result() # 获取策略函数返回的结果
#                         if result: # 如果策略返回 True (符合条件)
#                             current_strategy_results[f"{code} {name}"] = stocks_data[(code, name)] # 存储stock代码和名称作为键，以及其数据
#                             logger.info(f"stock {name} ({code}) 符合策略 [{strategy_name}]", extra={'stock': code, 'strategy': strategy_name})
#                     except Exception as exc:
#                         logger.error(f"处理stock {original_stock_info[1]}({original_stock_info[0]}) 时发生异常: {exc}", extra={'stock': original_stock_info[0], 'strategy': strategy_name})

#             logger.info(f"策略 [{strategy_name}] 运行完成，找到 {len(current_strategy_results)} 支符合条件的stock。", extra={'stock': 'NONE', 'strategy': strategy_name})

#             if len(current_strategy_results) > 0:
#                 titleMsg += format_strategy_result(strategy_name, current_strategy_results)
#                 # 特殊处理涨停板次日溢价策略，收集stock用于回测
#                 if strategy_name == '涨停板次日溢价':
#                     # current_strategy_results 的键是 "代码 名称" 字符串，值是 DataFrame
#                     # build_selected_limit_up_stocks 需要 (代码, 名称, 数据) 元组列表
#                     selected_limit_up_stocks = build_selected_limit_up_stocks(current_strategy_results)
#                     # logger.info(f"符合涨停板次日溢价策略的stock：{len(selected_limit_up_stocks)} 只", extra={'stock': 'NONE', 'strategy': '涨停板次日溢价'})

#     except Exception as e:
#         logger.error(f"处理策略和stock数据过程中失败: {e}", extra={'stock': 'NONE', 'strategy': 'NONE'})
#     return titleMsg, selected_limit_up_stocks

# # --- 保持其他辅助函数不变 ---

# # check_enter函数（原代码中的check_enter，现在这个名字有点歧义，因为它是一个工厂函数）
# # 这里保持原代码结构，但要注意它是一个高阶函数，返回一个可调用对象
# def check_enter(end_date=None, strategy_fun=None): # strategy_fun现在应该直接传入实际的策略函数
#     def end_date_filter(stock_info_tuple): # 接收 (stock代码, stock数据DataFrame)
#         code, data = stock_info_tuple # 解包元组
        
#         try:
#             if end_date is not None:
#                 end_date_ts = pd.Timestamp(end_date)
#                 # 确保 data['日期'] 已经是 datetime 类型
#                 if not isinstance(data.iloc[0]['日期'], pd.Timestamp):
#                     data['日期'] = pd.to_datetime(data['日期']) # 再次确保转换
                
#                 first_date = data.iloc[0]['日期']
#                 if end_date_ts < first_date:
#                     logger.debug(f"在 {end_date} 时还未上市", extra={'stock': code[0], 'strategy': 'UNKNOWN'})
#                     return False
                
#                 # 筛选数据到指定日期
#                 data_filtered = data[data['日期'] <= end_date_ts].copy()
#                 if data_filtered.empty:
#                     logger.debug(f"stock {code[0]} 在 {end_date} 之前没有数据。", extra={'stock': code[0], 'strategy': 'UNKNOWN'})
#                     return False
#             else:
#                 data_filtered = data.copy()

#             # 调用实际的策略函数
#             # 这里的 strategy_fun 应该接收 (stock_code_tuple, stock_data_df, end_date)
#             return strategy_fun(code, data_filtered, end_date=end_date)
#         except ValueError as ve:
#             logger.error(f"日期解析错误或数据问题: {ve}", extra={'stock': code[0], 'strategy': 'UNKNOWN'})
#             return False
#         except IndexError as ie:
#             logger.error(f"数据索引错误 (可能数据太少): {ie}", extra={'stock': code[0], 'strategy': 'UNKNOWN'})
#             return False
#         except Exception as e:
#             logger.error(f"执行策略时发生意外错误: {e}", extra={'stock': code[0], 'strategy': 'UNKNOWN'})
#             return False
#     return end_date_filter


# def format_strategy_result(strategy, results):
#     """
#     格式化单个策略的筛选结果。
#     results 字典的键是 "代码 名称" 字符串。
#     """
#     stock_names_list = []
#     for code_name_str in results.keys():
#         parts = code_name_str.split(maxsplit=1)
#         if len(parts) == 2:
#             stock_names_list.append(f"{parts[1]}({parts[0]})")
#         else:
#             stock_names_list.append(code_name_str) # 如果格式不符，直接用原始字符串

#     return '\n**************"{0}"**************\n{1}\n'.format(strategy, ' '.join(stock_names_list))


# def build_selected_limit_up_stocks(results):
#     """
#     从筛选结果中构建涨停板次日溢价stock列表。
    
#     Args:
#         results (dict): 筛选结果，键为 code_name (格式: "代码 名称")，值为stock数据 (DataFrame)。
    
#     Returns:
#         list: 包含 (代码, 名称, 数据) 元组的列表。
#     """
#     selected_limit_up_stocks = []
#     # print("当前stock是:",results.items()) # 调试信息
#     for code_name_str, data in results.items():
#         # print("当前处理的stock是:",code_name_str) # 调试信息
#         try:
#             # 验证 code_name 格式
#             if not isinstance(code_name_str, str) or not code_name_str.strip():
#                 logger.warning(f"无效的 code_name_str: {code_name_str}，跳过", extra={'stock': 'NONE', 'strategy': '涨停板回测'})
#                 continue
                
#             # 分割 code_name_str，假设格式为 "代码 名称"
#             parts = code_name_str.strip().split(maxsplit=1)
#             if len(parts) < 2:
#                 logger.warning(f"code_name_str 格式错误: {code_name_str}，缺少名称部分，跳过", extra={'stock': 'NONE', 'strategy': '涨停板回测'})
#                 continue
                
#             code, name = parts[0], parts[1]
            
#             # 验证stock代码格式（例如，6 位数字）
#             if not (code.isdigit() and len(code) == 6):
#                 logger.warning(f"stock代码格式错误: {code}，跳过", extra={'stock': code, 'strategy': '涨停板回测'})
#                 continue
                
#             # 验证数据有效性
#             if data is None or (isinstance(data, pd.DataFrame) and data.empty):
#                 logger.warning(f"stock {code_name_str} 的数据为空，跳过", extra={'stock': code, 'strategy': '涨停板回测'})
#                 continue
                
#             # 添加到结果列表
#             selected_limit_up_stocks.append((code, name, data))
#             logger.info(f"添加涨停板回测stock: 代码={code}, 名称={name}, 数据行数={len(data)}", extra={'stock': code, 'strategy': '涨停板回测'})
            
#         except Exception as e:
#             logger.error(f"处理 {code_name_str} 失败: {e}", extra={'stock': code_name_str.split()[0] if code_name_str else 'UNKNOWN', 'strategy': '涨停板回测'})
#             continue
    
#     logger.info(f"共筛选出 {len(selected_limit_up_stocks)} 只涨停板次日溢价stock用于回测", extra={'stock': 'NONE', 'strategy': '涨停板回测'})
#     return selected_limit_up_stocks


# def format_backtest_results(backtest_results):
#     result = "\n************************ 涨停板次日溢价回测结果 ************************\n"
#     for code_name_str, stats in backtest_results.items():
#         result += f"\nstock: {code_name_str}\n" # code_name_str 已经是 "代码 名称" 字符串
#         result += f"总交易次数: {stats.get('总交易次数', 'N/A')}\n"
#         result += f"胜率: {stats.get('胜率', 0):.2%}\n"
#         result += f"平均收益率: {stats.get('平均收益率', 0):.2%}\n"
#         result += f"盈利交易次数: {stats.get('盈利交易次数', 'N/A')}\n"
#         result += f"亏损交易次数: {stats.get('亏损交易次数', 'N/A')}\n"
#     return result

# def backtest_selected_stocks(selected_stocks):
#     backtest_results = {}
#     # 回测日期范围，根据你的需求设置
#     start_date = '20240101'
#     end_date = datetime.datetime.now().strftime('%Y%m%d') # 回测到今天
    
#     for symbol, name, data in selected_stocks:
#         code_name_str = f"{symbol} {name}"
#         try:
#             # 假设 limit_up.backtest 接收 code_name_str, data, start_date, end_date
#             # 并且 data 是完整的历史数据，backtest函数会在内部根据日期筛选
#             stats = limit_up.backtest(code_name_str, data, start_date, end_date)
#             backtest_results[code_name_str] = stats
#             logger.info(f"回测 {code_name_str} 完成: 胜率={stats['胜率']:.2%}", extra={'stock': symbol, 'strategy': '涨停板回测'})
#             time.sleep(random.uniform(1, 3)) # 引入随机延迟
#         except Exception as e:
#             logger.error(f"回测 {code_name_str} 失败: {e}", extra={'stock': symbol, 'strategy': '涨停板回测'})
#     return backtest_results

# def statistics(all_data, stocks):
#     msg = "" # 使用局部变量
#     try:
#         limitup = len(all_data.loc[(all_data['涨跌幅'] >= 9.5)])
#         limitdown = len(all_data.loc[(all_data['涨跌幅'] <= -9.5)])
#         up5 = len(all_data.loc[(all_data['涨跌幅'] >= 5)])
#         down5 = len(all_data.loc[(all_data['涨跌幅'] <= -5)])
#         msg = "************************ 市场统计 ************************\n"
#         msg += f"涨停数：{limitup} 跌停数：{limitdown}\n涨幅大于5%数：{up5} 跌幅大于5%数：{down5}\n"
#         msg += "************************ 策略结果 ************************\n"
#     except Exception as e:
#         logger.error(f"统计数据失败: {e}", extra={'stock': 'NONE', 'strategy': 'NONE'})
#         msg = "************************ 市场统计 ************************\n统计数据失败\n"
#     return msg

# if __name__ == "__main__":
#     prepare()
# data_fetcher_new.py
import akshare as ak
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
import datetime
from ratelimit import limits, sleep_and_retry

logger = logging.getLogger(__name__)

# 配置数据缓存路径
CACHE_DIR = "stock_data_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# AKShare API 限速装饰器
# 根据AKShare的实际限制调整 calls 和 period
@sleep_and_retry
@limits(calls=5, period=60) # 示例：每60秒最多5次调用，防止被封IP
def fetch_single_stock_data(stock_code, stock_name, start_date="20230101"):
    """
    获取单个股票的历史日线数据，并进行缓存。
    """
    file_path = os.path.join(CACHE_DIR, f"{stock_code}.csv")
    
    # 检查缓存
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path, parse_dates=['日期'])
            if not df.empty and df['日期'].max() >= pd.to_datetime(datetime.date.today() - datetime.timedelta(days=1)):
                logger.debug(f"从缓存加载 {stock_name}({stock_code}) 数据。", extra={'stock': stock_code, 'strategy': '数据获取'})
                return df
        except Exception as e:
            logger.warning(f"加载缓存文件 {file_path} 失败: {e}，将重新下载。", extra={'stock': stock_code, 'strategy': '数据获取'})
            os.remove(file_path) # 清理损坏的缓存文件

    logger.info(f"从AKShare下载 {stock_name}({stock_code}) 数据...", extra={'stock': stock_code, 'strategy': '数据获取'})
    try:
        # 获取股票历史数据，包含前复权，确保所有必要字段
        df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, adjust="hfq")
        
        # 重命名列以匹配策略需求
        df.rename(columns={
            '日期': '日期',
            '开盘': '开盘',
            '收盘': '收盘',
            '最高': '最高',
            '最低': '最低',
            '成交量': '成交量',
            '成交额': '成交额',
            '换手率': '换手率'
        }, inplace=True)

        # 确保日期列是 datetime 类型
        df['日期'] = pd.to_datetime(df['日期'])

        # 缓存数据
        df.to_csv(file_path, index=False)
        logger.info(f"成功下载并缓存 {stock_name}({stock_code}) 数据。", extra={'stock': stock_code, 'strategy': '数据获取'})
        return df
    except Exception as e:
        logger.error(f"下载 {stock_name}({stock_code}) 数据失败: {e}", extra={'stock': stock_code, 'strategy': '数据获取'})
        return pd.DataFrame() # 返回空DataFrame表示失败

def run(stocks_list, start_date="20230101"):
    """
    并发获取多个股票的历史数据。
    :param stocks_list: 股票代码和名称的列表，例如 [("000001", "平安银行"), ...]
    :param start_date: 数据开始日期
    :return: 字典，键为 (股票代码, 股票名称) 元组，值为对应的DataFrame数据。
    """
    all_stocks_data = {}
    
    # 使用 ThreadPoolExecutor 并发下载数据
    # 根据你的网络和API限制，可以调整 max_workers
    with ThreadPoolExecutor(max_workers=5) as executor: 
        future_to_stock = {
            executor.submit(fetch_single_stock_data, code, name, start_date): (code, name)
            for code, name in stocks_list
        }

        for future in as_completed(future_to_stock):
            code, name = future_to_stock[future]
            try:
                data = future.result()
                if not data.empty:
                    all_stocks_data[(code, name)] = data
            except Exception as exc:
                logger.error(f"获取 {name}({code}) 数据时发生异常: {exc}", extra={'stock': code, 'strategy': '数据获取'})
    
    return all_stocks_data

if __name__ == '__main__':
    # 示例用法
    sample_stocks = [
        ("000001", "平安银行"),
        ("600036", "招商银行"),
        ("000651", "格力电器"),
        ("600519", "贵州茅台"),
        ("002594", "比亚迪"),
    ]
    
    # 清空日志处理器，避免重复输出
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [Stock: %(stock)s] - [Strategy: %(strategy)s] - %(message)s'
    )
    
    logger.info("开始示例数据获取...")
    fetched_data = run(sample_stocks, start_date="20240101")
    logger.info(f"示例数据获取完成，共获取 {len(fetched_data)} 支股票数据。")
    
    if ("000001", "平安银行") in fetched_data:
        print("\n平安银行最新数据：")
        print(fetched_data[("000001", "平安银行")].tail())