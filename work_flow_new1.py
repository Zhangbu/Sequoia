# -*- encoding: UTF-8 -*-
import data_fetcher
import settings
import strategy.enter as enter
import akshare as ak
import push
import logging
import time
import datetime
import random
import pandas as pd
import yaml
import importlib
import argparse

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

titleMsg = ""
selected_limit_up_stocks = []

def load_config(config_path='config.yaml'):
    """Load strategies from config.yaml and validate IDs."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        strategies = config.get('strategies', {})
        # Validate IDs
        ids = [info.get('id') for info in strategies.values()]
        if len(ids) != len(set(ids)):
            logging.error("配置文件中的策略ID不唯一")
            return {}
        for name, info in strategies.items():
            if not all(k in info for k in ['id', 'module', 'function']):
                logging.error(f"策略 {name} 配置缺少 id, module 或 function")
                return {}
        return strategies
    except Exception as e:
        logging.error(f"加载配置文件失败: {e}")
        return {}

def get_strategy_function(module_name, function_name):
    """Dynamically import strategy function."""
    try:
        module = importlib.import_module(module_name)
        return getattr(module, function_name)
    except (ImportError, AttributeError) as e:
        logging.error(f"导入策略 {module_name}.{function_name} 失败: {e}")
        return None

def parse_args(strategies_config):
    """Parse command-line arguments for strategy selection (numeric IDs)."""
    parser = argparse.ArgumentParser(description='Run stock selection strategies.')
    valid_ids = [str(info['id']) for info in strategies_config.values()]
    parser.add_argument('--strategies', nargs='+', choices=valid_ids,
                        help=f'Strategy IDs to run (space-separated, e.g., {" ".join(valid_ids)}). If omitted, run all.')
    args = parser.parse_args()
    return args.strategies

def prepare():
    global titleMsg, selected_limit_up_stocks
    logging.info("************************ process start ***************************************")
    try:
        # Load config
        strategies_config = load_config()
        if not strategies_config:
            raise ValueError("无可用策略配置")

        # Parse command-line arguments (numeric IDs)
        selected_strategy_ids = parse_args(strategies_config)
        if selected_strategy_ids:
            logging.info(f"运行指定策略ID: {selected_strategy_ids}")
            selected_strategy_names = [
                name for name, info in strategies_config.items()
                if str(info['id']) in selected_strategy_ids
            ]
        else:
            selected_strategy_names = strategies_config.keys()
            logging.info("未指定策略，运行所有策略")

        # Build strategies dictionary
        strategies = {}
        for name in selected_strategy_names:
            config = strategies_config.get(name)
            if config:
                func = get_strategy_function(config['module'], config['function'])
                if func:
                    strategies[name] = func
                else:
                    logging.warning(f"跳过策略 {name}，无法加载函数")
            else:
                logging.warning(f"策略 {name} 未在配置文件中定义")

        # Original stock filtering
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

        # Process strategies
        process(stocks, strategies)

        # Backtest limit_up strategy for selected stocks (on Monday)
        if datetime.datetime.now().weekday() == 0 and selected_limit_up_stocks and '涨停板次日溢价' in strategies:
            logging.info("开始回测涨停板次日溢价策略")
            backtest_results = backtest_selected_stocks(selected_limit_up_stocks)
            titleMsg += format_backtest_results(backtest_results)

        # Push titleMsg
        if titleMsg:
            max_length = 4000
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
    global titleMsg, selected_limit_up_stocks
    try:
        # Fix: Use datetime.datetime.now() instead of datetime.now()
        end = settings.config.get('end_date', datetime.datetime.now().strftime('%Y-%m-%d'))
        # Validate end_date format
        try:
            pd.to_datetime(end)
        except ValueError as ve:
            logging.error(f"无效的 end_date 格式: {end}")
            return
        m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
        results = dict(filter(m_filter, stocks_data.items()))
        if len(results) > 0:
            titleMsg += format_strategy_result(strategy, results)
            if strategy == '涨停板次日溢价':
                selected_limit_up_stocks = [(code_name.split()[0], code_name.split()[1], data)
                                            for code_name, data in results.items()]
    except AttributeError as ae:
        logging.error(f"日期处理错误，可能使用了错误的 datetime 方法: {ae}")
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
    import newStrategy.limit_up as limit_up
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