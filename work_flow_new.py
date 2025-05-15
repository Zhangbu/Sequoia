# -*- encoding: UTF-8 -*-

import data_fetcher
import settings
import strategy.enter as enter
from strategy import turtle_trade, climax_limitdown
from strategy import backtrace_ma250
from strategy import breakthrough_platform
from strategy import parking_apron
from strategy import low_backtrace_increase
from strategy import keep_increasing
from strategy import high_tight_flag

import newStrategy.enter as newEnter
import newStrategy.keep_increasing as newKeep_increasing
import newStrategy.parking_apron as newParking_apron
import newStrategy.backtrace_ma250 as newBacktrace_ma250
import newStrategy.breakthrough_platform as newBreakthrough_platform
import newStrategy.low_backtrace_increase as newLow_backtrace_increase
import newStrategy.turtle_trade as newTurtle_trade
import newStrategy.high_tight_flag as newHigh_tight_flag
import newStrategy.climax_limitdown as newClimax_limitdown


import akshare as ak
import push
import logging
import time
import datetime

titleMsg = "************************ 新策略 ************************"

def prepare():
    logging.info("************************ process start ***************************************")
    all_data = ak.stock_zh_a_spot_em()
    # subset = all_data[['代码', '名称']]
    # 选择需要的列
    filtered_subset = all_data[['代码', '名称', '总市值']]

    # 过滤条件
    subset1 = filtered_subset[
    # 过滤掉代码以 "688" 或 "300" 开头的
    (~filtered_subset['代码'].str.startswith('688')) & 
    (~filtered_subset['代码'].str.startswith('300')) &
    # 过滤掉名称包含 "ST" 的
    (~filtered_subset['名称'].str.contains('ST', case=False, na=False)) &
    # 过滤掉总市值小于 100 亿（100亿 = 10000000000）
    (filtered_subset['总市值'] >= 10000000000)
    ]
    subset = subset1[['代码', '名称']]
    # 输出结果
    stocks = [tuple(x) for x in subset.values]
    statistics(all_data, stocks)
    
    strategies = {
        '放量上涨': newEnter.check_volume,
        '均线多头': newKeep_increasing.check,
        '停机坪': newParking_apron.check,
        '回踩年线': newBacktrace_ma250.check,
        '突破平台': newBreakthrough_platform.check,
        '无大幅回撤': newLow_backtrace_increase.check,
        '海龟交易法则': newTurtle_trade.check_enter,
        '高而窄的旗形': newHigh_tight_flag.check,
        '放量跌停': newClimax_limitdown.check,
    }

    if datetime.datetime.now().weekday() == 0:
        strategies['均线多头'] = keep_increasing.check

    process(stocks, strategies)

    # 在程序结束时发送一次完整的 titleMsg
    if titleMsg:
        push.strategy(titleMsg)  # 使用 push.strategy 发送整合后的消息
    else:
        push.strategy("无符合条件的策略结果")
        
    logging.info("************************ process   end ***************************************")

def process(stocks, strategies):
    stocks_data = data_fetcher.run(stocks)
    for strategy, strategy_func in strategies.items():
        check(stocks_data, strategy, strategy_func)
        time.sleep(2)

def check(stocks_data, strategy, strategy_func):
    end = settings.config['end_date']
    m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
    results = dict(filter(m_filter, stocks_data.items()))
    if len(results) > 0:
        # 将策略结果追加到 titleMsg，而不是直接推送
        titleMsg += '\n**************"{0}"**************\n{1}\n'.format(strategy, list(results.keys()))
        # push.strategy('**************"{0}"**************\n{1}\n**************"{0}"**************\n'.format(strategy, list(results.keys())))


def check_enter(end_date=None, strategy_fun=enter.check_volume):
    def end_date_filter(stock_data):
        if end_date is not None:
            if end_date < stock_data[1].iloc[0].日期:  # 该股票在end_date时还未上市
                logging.debug("{}在{}时还未上市".format(stock_data[0], end_date))
                return False
        return strategy_fun(stock_data[0], stock_data[1], end_date=end_date)


    return end_date_filter


# 统计数据
def statistics(all_data, stocks):
    limitup = len(all_data.loc[(all_data['涨跌幅'] >= 9.5)])
    limitdown = len(all_data.loc[(all_data['涨跌幅'] <= -9.5)])

    up5 = len(all_data.loc[(all_data['涨跌幅'] >= 5)])
    down5 = len(all_data.loc[(all_data['涨跌幅'] <= -5)])

    titleMsg += "涨停数：{}   跌停数：{}\n涨幅大于5%数：{}  跌幅大于5%数：{}".format(limitup, limitdown, up5, down5)
    # push.statistics(msg)


