# # -*- encoding: UTF-8 -*-

# import data_fetcher
# import settings
# import strategy.enter as enter

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


# import akshare as ak
# import push
# import logging
# import time
# import datetime

# titleMsg = "************************ 新策略 ************************"

# def prepare():
#     global titleMsg  # 声明 titleMsg 为全局变量以便修改
#     logging.info("************************ process start ***************************************")
#     all_data = ak.stock_zh_a_spot_em()
#     # subset = all_data[['代码', '名称']]
#     # 选择需要的列
#     filtered_subset = all_data[['代码', '名称', '总市值']]

#     # 过滤条件
#     subset1 = filtered_subset[
#     # 过滤掉代码以 "688" 或 "300" 开头的
#     (~filtered_subset['代码'].str.startswith('688')) & 
#     (~filtered_subset['代码'].str.startswith('300')) &
#     # 过滤掉名称包含 "ST" 的
#     (~filtered_subset['名称'].str.contains('ST', case=False, na=False)) &
#     # 过滤掉总市值小于 100 亿（100亿 = 10000000000）
#     (filtered_subset['总市值'] >= 10000000000)
#     ]
#     subset = subset1[['代码', '名称']]
#     # 输出结果
#     stocks = [tuple(x) for x in subset.values]
#     statistics(all_data, stocks)
    
#     strategies = {
#         '涨停板次日溢价': limit_up.check_enter,
#         '放量上涨': newEnter.check_volume,
#         '均线多头': newKeep_increasing.check,
#         '停机坪': newParking_apron.check,
#         '回踩年线': newBacktrace_ma250.check,
#         '突破平台': newBreakthrough_platform.check,
#         '无大幅回撤': newLow_backtrace_increase.check,
#         '海龟交易法则': newTurtle_trade.check_enter,
#         '高而窄的旗形': newHigh_tight_flag.check,
#         '放量跌停': newClimax_limitdown.check,
#     }

#     if datetime.datetime.now().weekday() == 0:
#         strategies['均线多头'] = newKeep_increasing.check

#     process(stocks, strategies)

#     # 在程序结束时发送一次完整的 titleMsg
#     if titleMsg:
#         push.strategy(titleMsg)  # 使用 push.strategy 发送整合后的消息
#     else:
#         push.strategy("无符合条件的策略结果")
        
#     logging.info("************************ process   end ***************************************")

# def process(stocks, strategies):
#     stocks_data = data_fetcher.run(stocks)
#     for strategy, strategy_func in strategies.items():
#         check(stocks_data, strategy, strategy_func)
#         time.sleep(2)

# def check(stocks_data, strategy, strategy_func):
#     global titleMsg  # 声明 titleMsg 为全局变量以便追加
#     end = settings.config['end_date']
#     m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
#     results = dict(filter(m_filter, stocks_data.items()))
#     if len(results) > 0:
#         # 将策略结果追加到 titleMsg，而不是直接推送
#         titleMsg += '\n**************"{0}"**************\n{1}\n'.format(strategy, list(results.keys()))
#         # push.strategy('**************"{0}"**************\n{1}\n**************"{0}"**************\n'.format(strategy, list(results.keys())))


# def check_enter(end_date=None, strategy_fun=enter.check_volume):
#     def end_date_filter(stock_data):
#         if end_date is not None:
#             if end_date < stock_data[1].iloc[0].日期:  # 该股票在end_date时还未上市
#                 logging.debug("{}在{}时还未上市".format(stock_data[0], end_date))
#                 return False
#         return strategy_fun(stock_data[0], stock_data[1], end_date=end_date)


#     return end_date_filter


# # 统计数据
# def statistics(all_data, stocks):
#     global titleMsg  # 声明 titleMsg 为全局变量以便修改
#     limitup = len(all_data.loc[(all_data['涨跌幅'] >= 9.5)])
#     limitdown = len(all_data.loc[(all_data['涨跌幅'] <= -9.5)])

#     up5 = len(all_data.loc[(all_data['涨跌幅'] >= 5)])
#     down5 = len(all_data.loc[(all_data['涨跌幅'] <= -5)])

#     titleMsg += "涨停数：{}   跌停数：{}\n涨幅大于5%数：{}  跌幅大于5%数：{}".format(limitup, limitdown, up5, down5)
#     # push.statistics(msg)

# # -*- encoding: UTF-8 -*-

# import utils
# import logging
# import work_flow
# import work_flow_new
# import settings
# import schedule
# import time
# import datetime
# from pathlib import Path


# def job():
#     if utils.is_weekday():
#         work_flow.prepare()
#         work_flow_new.prepare()


# logging.basicConfig(format='%(asctime)s %(message)s', filename='sequoia.log')
# logging.getLogger().setLevel(logging.INFO)
# settings.init()

# if settings.config['cron']:
#     EXEC_TIME = "15:15"
#     schedule.every().day.at(EXEC_TIME).do(job)

#     while True:
#         schedule.run_pending()
#         time.sleep(1)
# else:
#     work_flow_new.prepare()
#     # work_flow.prepare()




# -*- encoding: UTF-8 -*-
# import data_fetcher
# import settings
# import strategy.enter as enter
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
# import akshare as ak
# import push
# import logging
# import time
# import datetime
# import random

# # 配置日志
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# titleMsg = ""

# def prepare():
#     global titleMsg
#     logging.info("************************ process start ***************************************")
#     try:
#         all_data = ak.stock_zh_a_spot_em()
#         filtered_subset = all_data[['代码', '名称', '总市值']]
#         subset1 = filtered_subset[
#             (~filtered_subset['代码'].str.startswith('688')) &
#             (~filtered_subset['代码'].str.startswith('300')) &
#             (~filtered_subset['名称'].str.contains('ST', case=False, na=False)) &
#             (filtered_subset['总市值'] >= 10_000_000_000)
#         ]
#         subset = subset1[['代码', '名称']]
#         stocks = [tuple(x) for x in subset.values]
#         statistics(all_data, stocks)

#         strategies = {
#             '涨停板次日溢价': limit_up.check_enter,
#             '放量上涨': newEnter.check_volume,
#             '均线多头': newKeep_increasing.check,
#             '停机坪': newParking_apron.check,
#             '回踩年线': newBacktrace_ma250.check,
#             '突破平台': newBreakthrough_platform.check,
#             '无大幅回撤': newLow_backtrace_increase.check,
#             '海龟交易法则': newTurtle_trade.check_enter,
#             '高而窄的旗形': newHigh_tight_flag.check,
#             '放量跌停': newClimax_limitdown.check,
#         }

#         if datetime.datetime.now().weekday() == 0:
#             strategies['均线多头'] = newKeep_increasing.check

#         process(stocks, strategies)
        
        

#         # 推送 titleMsg
#         if titleMsg:
#             max_length = 4000
#             if len(titleMsg) > max_length:
#                 chunks = [titleMsg[i:i+max_length] for i in range(0, len(titleMsg), max_length)]
#                 for chunk in chunks:
#                     push.strategy(chunk)
#                     time.sleep(1)
#             else:
#                 push.strategy(titleMsg)
#         else:
#             push.strategy("无符合条件的策略结果")

#     except Exception as e:
#         logging.error(f"程序执行失败: {e}")
#         push.strategy(f"程序执行失败: {e}")

#     logging.info("************************ process   end ***************************************")

# def process(stocks, strategies):
#     try:
#         stocks_data = data_fetcher.run(stocks)
#         for strategy, strategy_func in strategies.items():
#             try:
#                 check(stocks_data, strategy, strategy_func)
#                 time.sleep(random.uniform(1, 3))  # 随机延迟
#             except Exception as e:
#                 logging.error(f"策略 {strategy} 执行失败: {e}")
#     except Exception as e:
#         logging.error(f"获取股票数据失败: {e}")

# def check(stocks_data, strategy, strategy_func):
#     global titleMsg
#     try:
#         end = settings.config.get('end_date', datetime.now().strftime('%Y-%m-%d'))
#         m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
#         results = dict(filter(m_filter, stocks_data.items()))
#         if len(results) > 0:
#             titleMsg += format_strategy_result(strategy, results)
#     except Exception as e:
#         logging.error(f"检查策略 {strategy} 失败: {e}")

# def check_enter(end_date=None, strategy_fun=enter.check_volume):
#     def end_date_filter(stock_data):
#         try:
#             if end_date is not None:
#                 if end_date < stock_data[1].iloc[0].日期:
#                     logging.debug(f"{stock_data[0]} 在 {end_date} 时还未上市")
#                     return False
#             return strategy_fun(stock_data[0], stock_data[1], end_date=end_date)
#         except Exception as e:
#             logging.error(f"过滤 {stock_data[0]} 失败: {e}")
#             return False
#     return end_date_filter

# def format_strategy_result(strategy, results):
#     return '\n**************"{0}"**************\n{1}\n'.format(strategy, list(results.keys()))

# def statistics(all_data, stocks):
#     global titleMsg
#     try:
#         limitup = len(all_data.loc[(all_data['涨跌幅'] >= 9.5)])
#         limitdown = len(all_data.loc[(all_data['涨跌幅'] <= -9.5)])
#         up5 = len(all_data.loc[(all_data['涨跌幅'] >= 5)])
#         down5 = len(all_data.loc[(all_data['涨跌幅'] <= -5)])
#         titleMsg = "************************ 市场统计 ************************\n"
#         titleMsg += "涨停数：{}   跌停数：{}\n涨幅大于5%数：{} 跌幅大于5%数：{}\n".format(
#             limitup, limitdown, up5, down5)
#         titleMsg += "************************ 策略结果 ************************\n"
#     except Exception as e:
#         logging.error(f"统计数据失败: {e}")
#         titleMsg = "************************ 市场统计 ************************\n统计数据失败\n"

# if __name__ == "__main__":
#     prepare()



# -*- encoding: UTF-8 -*-
import data_fetcher
import settings
import strategy.enter as enter
import newStrategy.enter as newEnter
import newStrategy.keep_increasing as newKeep_increasing
import newStrategy.parking_apron as newParking_apron
import newStrategy.backtrace_ma250 as newBacktrace_ma250
import newStrategy.breakthrough_platform as newBreakthrough_platform
import newStrategy.low_backtrace_increase as newLow_backtrace_increase
import newStrategy.turtle_trade as newTurtle_trade
import newStrategy.high_tight_flag as newHigh_tight_flag
import newStrategy.climax_limitdown as newClimax_limitdown
import newStrategy.limit_up as limit_up
import akshare as ak
import push
import logging
import time
import datetime
import random
import pandas as pd

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

titleMsg = ""
selected_limit_up_stocks = []  # Store stocks passing limit_up.check_enter

def prepare():
    global titleMsg, selected_limit_up_stocks
    logging.info("************************ process start ***************************************")
    try:
        all_data = ak.stock_zh_a_spot_em()
        filtered_subset = all_data[['代码', '名称', '总市值']]
        subset1 = filtered_subset[
            (~filtered_subset['代码'].str.startswith('688')) &
            (~filtered_subset['代码'].str.startswith('300')) &
            (~filtered_subset['名称'].str.contains('ST', case=False, na=False)) &
            (filtered_subset['总市值'] >= 10_000_000_000)
        ]
        subset = subset1[['代码', '名称']]
        stocks = [tuple(x) for x in subset.values]
        statistics(all_data, stocks)

        strategies = {
            '涨停板次日溢价': limit_up.check_enter,
            # '放量上涨': newEnter.check_volume,
            # '均线多头': newKeep_increasing.check,
            # '停机坪': newParking_apron.check,
            # '回踩年线': newBacktrace_ma250.check,
            # '突破平台': newBreakthrough_platform.check,
            # '无大幅回撤': newLow_backtrace_increase.check,
            # '海龟交易法则': newTurtle_trade.check_enter,
            # '高而窄的旗形': newHigh_tight_flag.check,
            # '放量跌停': newClimax_limitdown.check,
        }

        if datetime.datetime.now().weekday() == 0:
            strategies['均线多头'] = newKeep_increasing.check

        process(stocks, strategies)

        print("测涨停板次日溢价策略的股票：", selected_limit_up_stocks)    
        # Backtest limit_up strategy for selected stocks (on Monday)
        if selected_limit_up_stocks:
            logging.info("开始回测涨停板次日溢价策略")
            backtest_results = backtest_selected_stocks(selected_limit_up_stocks)
            titleMsg += format_backtest_results(backtest_results)

        # 推送 titleMsg
        if titleMsg:
            max_length = 4000
            print(titleMsg)
            if len(titleMsg) > max_length:
                chunks = [titleMsg[i:i+max_length] for i in range(0, len(titleMsg), max_length)]
                for chunk in chunks:
                    push.strategy(chunk)
                    time.sleep(1)
            else:
                push.strategy(titleMsg)
        else:
            push.strategy("无符合条件的策略结果")

    except Exception as e:
        logging.error(f"程序执行失败: {e}")
        push.strategy(f"程序执行失败: {e}")

    logging.info("************************ process   end ***************************************")

def process(stocks, strategies):
    try:
        stocks_data = data_fetcher.run(stocks)
        for strategy, strategy_func in strategies.items():
            try:
                check(stocks_data, strategy, strategy_func)
                time.sleep(random.uniform(1, 3))
            except Exception as e:
                logging.error(f"策略 {strategy} 执行失败: {e}")
    except Exception as e:
        logging.error(f"获取股票数据失败: {e}")

def check(stocks_data, strategy, strategy_func):
    print("当前策略是:",strategy)
    global titleMsg, selected_limit_up_stocks
    try:
        end = settings.config.get('end_date', datetime.datetime.now().strftime('%Y-%m-%d'))
        m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
        results = dict(filter(m_filter, stocks_data.items()))
        if len(results) > 0:
            titleMsg += format_strategy_result(strategy, results)
            # Store stocks for limit_up backtesting
            if strategy == '涨停板次日溢价':
                # selected_limit_up_stocks = [(code_name.split()[0], code_name.split()[1], data)
                #                             for code_name, data in results.items()]
                selected_limit_up_stocks = build_selected_limit_up_stocks(results)
                print("符合涨停板次日溢价策略的股票：", selected_limit_up_stocks)
    except Exception as e:
        logging.error(f"检查策略 {strategy} 失败: {e}")

def check_enter(end_date=None, strategy_fun=enter.check_volume):
    def end_date_filter(stock_data):
        try:
            if end_date is not None:
                if end_date < stock_data[1].iloc[0].日期:
                    logging.debug(f"{stock_data[0]} 在 {end_date} 时还未上市")
                    return False
            return strategy_fun(stock_data[0], stock_data[1], end_date=end_date)
        except Exception as e:
            logging.error(f"过滤 {stock_data[0]} 失败: {e}")
            return False
    return end_date_filter

def format_strategy_result(strategy, results):
    return '\n**************"{0}"**************\n{1}\n'.format(strategy, list(results.keys()))

def build_selected_limit_up_stocks(results):
    """
    从筛选结果中构建涨停板次日溢价股票列表。
    
    Args:
        results (dict): 筛选结果，键为 code_name (格式: "代码 名称")，值为股票数据 (DataFrame)。
    
    Returns:
        list: 包含 (代码, 名称, 数据) 元组的列表。
    """
    selected_limit_up_stocks = []
    
    for code_name, data in results.items():
        print("当前股票是:",code_name)
        try:
            # 验证 code_name 格式
            if not isinstance(code_name, str) or not code_name.strip():
                logging.warning(f"无效的 code_name: {code_name}，跳过")
                continue
                
            # 分割 code_name，假设格式为 "代码 名称"
            parts = code_name.strip().split(maxsplit=1)
            if len(parts) < 2:
                logging.warning(f"code_name 格式错误: {code_name}，缺少名称部分，跳过")
                continue
                
            code, name = parts[0], parts[1]
            
            # 验证股票代码格式（例如，6 位数字）
            if not (code.isdigit() and len(code) == 6):
                logging.warning(f"股票代码格式错误: {code}，跳过")
                continue
                
            # 验证数据有效性
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                logging.warning(f"股票 {code_name} 的数据为空，跳过")
                continue
                
            # 添加到结果列表
            selected_limit_up_stocks.append((code, name, data))
            logging.info(f"添加股票: 代码={code}, 名称={name}, 数据行数={len(data)}")
            
        except Exception as e:
            logging.error(f"处理 {code_name} 失败: {e}")
            continue
    
    logging.info(f"共筛选出 {len(selected_limit_up_stocks)} 只涨停板次日溢价股票")
    return selected_limit_up_stocks

def format_backtest_results(backtest_results):
    result = "\n************************ 涨停板次日溢价回测结果 ************************\n"
    for code_name, stats in backtest_results.items():
        result += f"\n股票: {code_name}\n"
        result += f"总交易次数: {stats['总交易次数']}\n"
        result += f"胜率: {stats['胜率']:.2%}\n"
        result += f"平均收益率: {stats['平均收益率']:.2%}\n"
        result += f"盈利交易次数: {stats['盈利交易次数']}\n"
        result += f"亏损交易次数: {stats['亏损交易次数']}\n"
    return result

def backtest_selected_stocks(selected_stocks):
    backtest_results = {}
    start_date = '20240101'
    end_date = '20241231'
    for symbol, name, data in selected_stocks:
        code_name = f"{symbol} {name}"
        try:
            stats = limit_up.backtest(code_name, data, start_date, end_date)
            backtest_results[code_name] = stats
            logging.info(f"回测 {code_name} 完成: 胜率={stats['胜率']:.2%}")
            time.sleep(random.uniform(1, 3))
        except Exception as e:
            logging.error(f"回测 {code_name} 失败: {e}")
    return backtest_results

def statistics(all_data, stocks):
    global titleMsg
    try:
        limitup = len(all_data.loc[(all_data['涨跌幅'] >= 9.5)])
        limitdown = len(all_data.loc[(all_data['涨跌幅'] <= -9.5)])
        up5 = len(all_data.loc[(all_data['涨跌幅'] >= 5)])
        down5 = len(all_data.loc[(all_data['涨跌幅'] <= -5)])
        titleMsg = "************************ 市场统计 ************************\n"
        titleMsg += "涨停数：{}   跌停数：{}\n涨幅大于5%数：{} 跌幅大于5%数：{}\n".format(
            limitup, limitdown, up5, down5)
        titleMsg += "************************ 策略结果 ************************\n"
    except Exception as e:
        logging.error(f"统计数据失败: {e}")
        titleMsg = "************************ 市场统计 ************************\n统计数据失败\n"

if __name__ == "__main__":
    prepare()
