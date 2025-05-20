# work_flow_new1.py
# -*- encoding: UTF-8 -*-

import data_fetcher_new
import settings
import strategy.enter as enter
import newStrategy.my_short_term_strategy as my_short_term_strategy

import akshare as ak
import push
import logging
import datetime
import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from ratelimit import limits, sleep_and_retry
import sys # 导入sys模块
from tqdm import tqdm # 导入tqdm

# 配置日志 (确保同时输出到文件和控制台)
# 之前的basicConfig只输出到文件。为了同时输出到控制台，需要更明确的配置。
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # 设置logger的最低处理级别

# 检查是否已经存在StreamHandler，避免重复添加
if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
    console_handler = logging.StreamHandler(sys.stdout) # 输出到标准输出
    console_handler.setLevel(logging.INFO)
    # 使用与文件相同的formatter，或者根据需要自定义
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [Stock: %(stock)s] - [Strategy: %(strategy)s] - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# 确保文件处理器也存在 (如果主文件没有全局配置到这个logger)
# 更好的做法是在 main.py 中全局配置所有 logger，然后在这里只需要获取 logger 即可。
# 如果你希望 work_flow_new1.py 有自己独立的文件日志，可以再添加一个FileHandler
# 但通常，所有模块的日志都通过主 logger 汇总到同一个文件。
# 为了简化，我们假设 main.py 已经配置了文件日志，这里主要确保控制台输出。


def prepare():
    titleMsg = ""
    selected_limit_up_stocks = []
    logger.info("Process start", extra={'stock': 'NONE', 'strategy': 'NONE'})
    try:
        all_data = ak.stock_zh_a_spot_em() 
        
        required_cols = {'代码', '名称', '总市值', '涨跌幅', '成交额', '换手率', '最新价'}
        if not required_cols.issubset(all_data.columns):
            missing_cols = required_cols - set(all_data.columns)
            logger.error(f"ak.stock_zh_a_spot_em() 返回的数据缺少必要列: {missing_cols}。请检查AKShare数据源。", extra={'stock': 'NONE', 'strategy': 'NONE'})
            return "", []

        filtered_subset = all_data[['代码', '名称', '总市值', '涨跌幅', '成交额', '换手率', '最新价']]
        
        subset1 = filtered_subset[
            (~filtered_subset['代码'].str.startswith('688')) &
            (~filtered_subset['代码'].str.startswith('300')) &
            (~filtered_subset['名称'].str.contains('ST', case=False, na=False)) &
            (filtered_subset['总市值'] >= 10_000_000_000) &
            (filtered_subset['成交额'] >= 200_000_000) &
            (filtered_subset['换手率'] >= 1.0) &
            (filtered_subset['换手率'] <= 25.0) &
            (filtered_subset['最新价'] >= 5.0) &
            (filtered_subset['涨跌幅'] > -3.0) 
        ]
        
        subset = subset1[['代码', '名称']]
        stocks = [tuple(x) for x in subset.values]
        
        logger.info(f"初步筛选后，剩余 {len(stocks)} 只股票进入后续分析。", extra={'stock': 'NONE', 'strategy': '初步筛选'})
        
        titleMsg = statistics(all_data, stocks)
        
        strategies = {
            '东方财富短线策略': my_short_term_strategy.check_enter,
        }

        titleMsg, selected_limit_up_stocks = process(stocks, strategies, titleMsg, selected_limit_up_stocks)

        logger.info(f"符合涨停板次日溢价策略的股票：{len(selected_limit_up_stocks)} 只", extra={'stock': 'NONE', 'strategy': '涨停板次日溢价'})
        
        if selected_limit_up_stocks and datetime.datetime.now().weekday() == 0:
            logger.info("开始回测涨停板次日溢价策略", extra={'stock': 'NONE', 'strategy': '限价板回测'})
            backtest_results = backtest_selected_stocks(selected_limit_up_stocks)
            titleMsg += format_backtest_results(backtest_results)

        if titleMsg:
            max_length = 4000
            print(titleMsg)
            if len(titleMsg) > max_length:
                chunks = [titleMsg[i:i+max_length] for i in range(0, len(titleMsg), max_length)]
                for chunk in chunks:
                    push.strategy(chunk)
            else:
                push.strategy(titleMsg)
        else:
            push.strategy("无符合条件的策略结果")

    except Exception as e:
        logger.exception(f"程序执行失败: {e}", extra={'stock': 'NONE', 'strategy': 'NONE'}) # 使用 exception 打印完整的堆栈信息
        push.strategy(f"程序执行失败: {e}")

    logger.info("Process end", extra={'stock': 'NONE', 'strategy': 'NONE'})
    return titleMsg, selected_limit_up_stocks

@sleep_and_retry
@limits(calls=10, period=60)
def call_strategy_check(stock_info, strategy_func, end_date):
    stock_code, stock_name, stock_data_df = stock_info
    
    try:
        result = strategy_func((stock_code, stock_name), stock_data_df, end_date=end_date)
        return (stock_code, stock_name), result
    except Exception as e:
        logger.error(f"策略函数执行失败: {e}", extra={'stock': stock_code, 'strategy': strategy_func.__module__})
        return (stock_code, stock_name), False

def process(stocks, strategies, titleMsg, selected_limit_up_stocks):
    try:
        logger.info(f"开始获取 {len(stocks)} 支股票的历史数据...", extra={'stock': 'NONE', 'strategy': '数据获取'})
        stocks_data_dict = data_fetcher_new.run(stocks) 
        logger.info(f"历史数据获取完成，成功获取 {len(stocks_data_dict)} 支股票数据。", extra={'stock': 'NONE', 'strategy': '数据获取'})

        end_date_str = settings.config.get('end_date', datetime.datetime.now().strftime('%Y-%m-%d'))
        end_date_ts = pd.Timestamp(end_date_str)
        logger.info(f"当前分析日期为: {end_date_ts.strftime('%Y-%m-%d')}", extra={'stock': 'NONE', 'strategy': '日期'})
        
        for strategy_name, strategy_func in strategies.items():
            logger.info(f"开始运行策略: {strategy_name}", extra={'stock': 'NONE', 'strategy': strategy_name})
            
            current_strategy_results = {}
            
            with ThreadPoolExecutor(max_workers=5) as executor: 
                future_to_stock_info = {
                    executor.submit(call_strategy_check, (code_name[0], code_name[1], data), strategy_func, end_date_ts): code_name
                    for code_name, data in stocks_data_dict.items()
                }
                
                # 使用 tqdm 包裹 as_completed，显示进度条
                # desc 是进度条前缀，total 是总数，unit 是单位
                for future in tqdm(as_completed(future_to_stock_info), 
                                   total=len(future_to_stock_info), 
                                   desc=f"Running {strategy_name}", 
                                   unit="stock"):
                    original_code_name_tuple = future_to_stock_info[future]
                    try:
                        (code, name), result = future.result()
                        if result:
                            current_strategy_results[f"{code} {name}"] = stocks_data_dict[(code, name)]
                            logger.info(f"股票 {name} ({code}) 符合策略 [{strategy_name}]", extra={'stock': code, 'strategy': strategy_name})
                    except Exception as exc:
                        logger.error(f"处理股票 {original_code_name_tuple[1]}({original_code_name_tuple[0]}) 时发生异常: {exc}", extra={'stock': original_code_name_tuple[0], 'strategy': strategy_name})

            logger.info(f"策略 [{strategy_name}] 运行完成，找到 {len(current_strategy_results)} 支符合条件的股票。", extra={'stock': 'NONE', 'strategy': strategy_name})

            if len(current_strategy_results) > 0:
                titleMsg += format_strategy_result(strategy_name, current_strategy_results)
                if strategy_name == '涨停板次日溢价':
                    selected_limit_up_stocks = build_selected_limit_up_stocks(current_strategy_results)

    except Exception as e:
        logger.exception(f"处理策略和股票数据过程中失败: {e}", extra={'stock': 'NONE', 'strategy': 'NONE'}) # 同样使用 exception
    return titleMsg, selected_limit_up_stocks

# 限速装饰器
@sleep_and_retry
@limits(calls=10, period=60) # 每60秒最多调用10次，根据akshare的限制调整
def call_strategy_check(stock_info, strategy_func, end_date):
    """
    一个包装函数，用于在线程池中调用策略函数，并处理日志。
    stock_info 是 (股票代码, 股票名称, DataFrame) 元组
    """
    stock_code, stock_name, stock_data_df = stock_info
    
    try:
        # 你的策略函数现在接收 (code_tuple, data_df, end_date)
        result = strategy_func((stock_code, stock_name), stock_data_df, end_date=end_date)
        return (stock_code, stock_name), result
    except Exception as e:
        logger.error(f"策略函数执行失败: {e}", extra={'stock': stock_code, 'strategy': strategy_func.__module__})
        return (stock_code, stock_name), False

def process(stocks, strategies, titleMsg, selected_limit_up_stocks):
    try:
        # 获取所有股票的历史数据
        # data_fetcher_new.run(stocks) 需要返回 {("代码", "名称"): DataFrame} 格式
        # 且 DataFrame 必须包含 '日期', '收盘', '开盘', '最高', '最低', '成交量', '成交额', '换手率'
        logger.info(f"开始获取 {len(stocks)} 支股票的历史数据...", extra={'stock': 'NONE', 'strategy': '数据获取'})
        stocks_data_dict = data_fetcher_new.run(stocks) # 假设 data_fetcher_new.run 已经优化并返回正确格式
        logger.info(f"历史数据获取完成，成功获取 {len(stocks_data_dict)} 支股票数据。", extra={'stock': 'NONE', 'strategy': '数据获取'})

        # 确定分析的结束日期
        end_date_str = settings.config.get('end_date', datetime.datetime.now().strftime('%Y-%m-%d'))
        end_date_ts = pd.Timestamp(end_date_str)
        logger.info(f"当前分析日期为: {end_date_ts.strftime('%Y-%m-%d')}", extra={'stock': 'NONE', 'strategy': '日期'})
        
        for strategy_name, strategy_func in strategies.items():
            logger.info(f"开始运行策略: {strategy_name}", extra={'stock': 'NONE', 'strategy': strategy_name})
            
            current_strategy_results = {}
            
            with ThreadPoolExecutor(max_workers=5) as executor: # 适当降低并发度
                # 组装 (code, name, df) 元组传递给 call_strategy_check
                future_to_stock_info = {
                    executor.submit(call_strategy_check, (code_name[0], code_name[1], data), strategy_func, end_date_ts): code_name
                    for code_name, data in stocks_data_dict.items()
                }
                
                for future in as_completed(future_to_stock_info):
                    original_code_name_tuple = future_to_stock_info[future] # (code, name)
                    try:
                        (code, name), result = future.result()
                        if result:
                            current_strategy_results[f"{code} {name}"] = stocks_data_dict[(code, name)]
                            logger.info(f"股票 {name} ({code}) 符合策略 [{strategy_name}]", extra={'stock': code, 'strategy': strategy_name})
                    except Exception as exc:
                        logger.error(f"处理股票 {original_code_name_tuple[1]}({original_code_name_tuple[0]}) 时发生异常: {exc}", extra={'stock': original_code_name_tuple[0], 'strategy': strategy_name})

            logger.info(f"策略 [{strategy_name}] 运行完成，找到 {len(current_strategy_results)} 支符合条件的股票。", extra={'stock': 'NONE', 'strategy': strategy_name})

            if len(current_strategy_results) > 0:
                titleMsg += format_strategy_result(strategy_name, current_strategy_results)
                if strategy_name == '涨停板次日溢价':
                    selected_limit_up_stocks = build_selected_limit_up_stocks(current_strategy_results)

    except Exception as e:
        logger.error(f"处理策略和股票数据过程中失败: {e}", extra={'stock': 'NONE', 'strategy': 'NONE'})
    return titleMsg, selected_limit_up_stocks

# check_enter 函数 (原代码中的那个高阶函数)
def check_enter(end_date=None, strategy_fun=None):
    def end_date_filter(stock_info_tuple_for_adapter): # 接收 (股票代码元组 (code, name), 股票数据DataFrame)
        code_tuple, data = stock_info_tuple_for_adapter
        
        try:
            if end_date is not None:
                end_date_ts = pd.Timestamp(end_date)
                if not isinstance(data.iloc[0]['日期'], pd.Timestamp):
                    data['日期'] = pd.to_datetime(data['日期'])
                
                first_date = data.iloc[0]['日期']
                if end_date_ts < first_date:
                    logger.debug(f"在 {end_date} 时还未上市", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
                    return False
                
                data_filtered = data[data['日期'] <= end_date_ts].copy()
                if data_filtered.empty:
                    logger.debug(f"股票 {code_tuple[0]} 在 {end_date} 之前没有数据。", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
                    return False
            else:
                data_filtered = data.copy()

            # 调用实际的策略函数
            return strategy_fun(code_tuple, data_filtered, end_date=end_date)
        except ValueError as ve:
            logger.error(f"日期解析错误或数据问题: {ve}", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
            return False
        except IndexError as ie:
            logger.error(f"数据索引错误 (可能数据太少): {ie}", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
            return False
        except Exception as e:
            logger.error(f"执行策略时发生意外错误: {e}", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
            return False
    return end_date_filter


def format_strategy_result(strategy, results):
    stock_names_list = []
    for code_name_str in results.keys():
        parts = code_name_str.split(maxsplit=1)
        if len(parts) == 2:
            stock_names_list.append(f"{parts[1]}({parts[0]})")
        else:
            stock_names_list.append(code_name_str)

    return '\n**************"{0}"**************\n{1}\n'.format(strategy, ' '.join(stock_names_list))


def build_selected_limit_up_stocks(results):
    selected_limit_up_stocks = []
    for code_name_str, data in results.items():
        try:
            if not isinstance(code_name_str, str) or not code_name_str.strip():
                logger.warning(f"无效的 code_name_str: {code_name_str}，跳过", extra={'stock': 'NONE', 'strategy': '涨停板回测'})
                continue
                
            parts = code_name_str.strip().split(maxsplit=1)
            if len(parts) < 2:
                logger.warning(f"code_name_str 格式错误: {code_name_str}，缺少名称部分，跳过", extra={'stock': 'NONE', 'strategy': '涨停板回测'})
                continue
                
            code, name = parts[0], parts[1]
            
            if not (code.isdigit() and len(code) == 6):
                logger.warning(f"股票代码格式错误: {code}，跳过", extra={'stock': code, 'strategy': '涨停板回测'})
                continue
                
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                logger.warning(f"股票 {code_name_str} 的数据为空，跳过", extra={'stock': code, 'strategy': '涨停板回测'})
                continue
                
            selected_limit_up_stocks.append((code, name, data))
            logger.info(f"添加涨停板回测股票: 代码={code}, 名称={name}, 数据行数={len(data)}", extra={'stock': code, 'strategy': '涨停板回测'})
            
        except Exception as e:
            logger.error(f"处理 {code_name_str} 失败: {e}", extra={'stock': code_name_str.split()[0] if code_name_str else 'UNKNOWN', 'strategy': '涨停板回测'})
            continue
    
    logger.info(f"共筛选出 {len(selected_limit_up_stocks)} 只涨停板次日溢价股票用于回测", extra={'stock': 'NONE', 'strategy': '涨停板回测'})
    return selected_limit_up_stocks


def format_backtest_results(backtest_results):
    result = "\n************************ 涨停板次日溢价回测结果 ************************\n"
    for code_name_str, stats in backtest_results.items():
        result += f"\n股票: {code_name_str}\n"
        result += f"总交易次数: {stats.get('总交易次数', 'N/A')}\n"
        result += f"胜率: {stats.get('胜率', 0):.2%}\n"
        result += f"平均收益率: {stats.get('平均收益率', 0):.2%}\n"
        result += f"盈利交易次数: {stats.get('盈利交易次数', 'N/A')}\n"
        result += f"亏损交易次数: {stats.get('亏损交易次数', 'N/A')}\n"
    return result

def backtest_selected_stocks(selected_stocks):
    backtest_results = {}
    start_date = '20250101'
    end_date = datetime.datetime.now().strftime('%Y%m%d')
    
    for symbol, name, data in selected_stocks:
        code_name_str = f"{symbol} {name}"
        try:
            stats = limit_up.backtest(code_name_str, data, start_date, end_date)
            backtest_results[code_name_str] = stats
            logger.info(f"回测 {code_name_str} 完成: 胜率={stats['胜率']:.2%}", extra={'stock': symbol, 'strategy': '涨停板回测'})
            time.sleep(random.uniform(1, 3))
        except Exception as e:
            logger.error(f"回测 {code_name_str} 失败: {e}", extra={'stock': symbol, 'strategy': '涨停板回测'})
    return backtest_results

def statistics(all_data, stocks):
    msg = ""
    try:
        limitup = len(all_data.loc[(all_data['涨跌幅'] >= 9.5)])
        limitdown = len(all_data.loc[(all_data['涨跌幅'] <= -9.5)])
        up5 = len(all_data.loc[(all_data['涨跌幅'] >= 5)])
        down5 = len(all_data.loc[(all_data['涨跌幅'] <= -5)])
        msg = "************************ 市场统计 ************************\n"
        msg += f"涨停数：{limitup} 跌停数：{limitdown}\n涨幅大于5%数：{up5} 跌幅大于5%数：{down5}\n"
        msg += "************************ 策略结果 ************************\n"
    except Exception as e:
        logger.error(f"统计数据失败: {e}", extra={'stock': 'NONE', 'strategy': 'NONE'})
        msg = "************************ 市场统计 ************************\n统计数据失败\n"
    return msg