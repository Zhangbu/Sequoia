# -*- encoding: UTF-8 -*-
import pandas as pd
import logging
from datetime import datetime, timedelta
import akshare as ak
import random
import time

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 策略配置
BALANCE = 200_000  # 初始资金（保留，供未来仓位管理）
CONFIG = {
    'volume_increase_ratio': 1.5,  # 成交量放大倍数（对比前 5 日均量）
    'limit_up_threshold': 9.5,  # 涨停涨幅阈值（%）
    'profit_target': 0.03,  # 次日盈利目标 3%
    'stop_loss': -0.05,  # 次日止损 -5%
    'lookback_days': 5,  # 回溯天数（计算均量）
}

def check_enter(code_name, data, end_date=None, volume_lookback=5, limit_up_threshold=9.5, volume_ratio=1.5):
    """
    检查是否满足涨停板次日溢价策略入场条件：当天涨幅 ≥ 9.5% 且成交量放大。

    参数:
        code_name (str): 股票代码或名称，用于日志记录
        data (pd.DataFrame): 包含'日期', '收盘', '涨跌幅', '成交量'列的股票数据
        end_date (str or datetime, optional): 数据截止日期，格式如 '2023-12-31'
        volume_lookback (int): 成交量回溯天数，默认 5 天
        limit_up_threshold (float): 涨停涨幅阈值，默认 9.5%
        volume_ratio (float): 成交量放大倍数，默认 1.5 倍

    返回:
        bool: True（满足入场条件），False（不满足）
    """
    try:
        # 参数校验
        if not isinstance(data, pd.DataFrame) or data.empty:
            logger.warning(f"{code_name}: 提供的数据为空或格式错误")
            return False
        
        required_columns = {'日期', '收盘', '涨跌幅', '成交量'}
        if not required_columns.issubset(data.columns):
            logger.warning(f"{code_name}: 数据缺少必要列 {required_columns - set(data.columns)}")
            return False
        
        if volume_lookback <= 0 or limit_up_threshold <= 0 or volume_ratio <= 0:
            logger.warning(f"{code_name}: 参数必须为正数，当前 volume_lookback={volume_lookback}, "
                          f"limit_up_threshold={limit_up_threshold}, volume_ratio={volume_ratio}")
            return False

        # 日期筛选
        if end_date is not None:
            try:
                if isinstance(end_date, str):
                    end_date = pd.to_datetime(end_date)
                mask = (data['日期'] <= end_date)
                data = data.loc[mask].copy()
            except (ValueError, TypeError) as e:
                logger.warning(f"{code_name}: end_date格式错误，忽略日期过滤 - {e}")
                return False

        # 检查数据长度
        if len(data) < volume_lookback + 1:
            logger.warning(f"{code_name}: 数据长度{len(data)}不足volume_lookback={volume_lookback}+1")
            return False

        # 获取最后一天的数据
        last_row = data.iloc[-1]
        last_change = last_row['涨跌幅']
        last_volume = last_row['成交量']

        # 检查涨幅是否满足涨停条件
        if last_change < limit_up_threshold:
            return False

        # 计算前 volume_lookback 天的平均成交量
        prev_data = data.iloc[-volume_lookback-1:-1]
        avg_volume = prev_data['成交量'].mean()

        if pd.isna(avg_volume) or avg_volume == 0:
            logger.warning(f"{code_name}: 无法计算历史平均成交量")
            return False

        # 检查成交量是否放大
        if last_volume < volume_ratio * avg_volume:
            return False

        logger.info(f"{code_name}: 满足涨停入场条件，涨幅={last_change:.2f}%, "
                    f"成交量={last_volume} ({volume_ratio}x 平均成交量={avg_volume:.2f})")
        return True

    except Exception as e:
        logger.error(f"{code_name}: 处理过程中发生错误 - {e}")
        return False

def backtest(code_name, data, start_date, end_date):
    """
    回测涨停板次日溢价策略，计算单只股票的胜率和收益率。

    参数:
        code_name (str): 股票代码或名称
        data (pd.DataFrame): 包含'日期', '收盘', '涨跌幅', '成交量'列的股票数据
        start_date (str or datetime): 回测开始日期，格式 'YYYY-MM-DD'
        end_date (str or datetime): 回测结束日期，格式 'YYYY-MM-DD'

    返回:
        dict: 包含胜率、平均收益率、交易次数等统计信息
    """
    try:
        # 参数校验
        if not isinstance(data, pd.DataFrame) or data.empty:
            logger.warning(f"{code_name}: 提供的数据为空或格式错误")
            return {'总交易次数': 0, '胜率': 0.0, '平均收益率': 0.0, '盈利交易次数': 0, '亏损交易次数': 0}
        
        required_columns = {'日期', '收盘', '涨跌幅', '成交量'}
        if not required_columns.issubset(data.columns):
            logger.warning(f"{code_name}: 数据缺少必要列 {required_columns - set(data.columns)}")
            return {'总交易次数': 0, '胜率': 0.0, '平均收益率': 0.0, '盈利交易次数': 0, '亏损交易次数': 0}

        # 日期格式转换
        try:
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)
        except (ValueError, TypeError) as e:
            logger.error(f"{code_name}: 日期格式错误 - {e}")
            return {'总交易次数': 0, '胜率': 0.0, '平均收益率': 0.0, '盈利交易次数': 0, '亏损交易次数': 0}

        # 过滤数据到指定日期范围
        mask = (data['日期'] >= start_date) & (data['日期'] <= end_date)
        data = data.loc[mask].copy()

        if len(data) < 2:
            logger.warning(f"{code_name}: 数据长度{len(data)}不足，无法回测")
            return {'总交易次数': 0, '胜率': 0.0, '平均收益率': 0.0, '盈利交易次数': 0, '亏损交易次数': 0}

        total_trades = 0
        win_trades = 0
        total_return = 0.0
        trade_details = []

        # 遍历数据，检查每个交易日
        for i in range(len(data) - 1):  # 最后一天无次日数据
            trade_date = data.iloc[i]['日期']
            trade_data = data.iloc[:i+1]
            
            if check_enter(code_name, trade_data, end_date=trade_date,
                          volume_lookback=CONFIG['lookback_days'],
                          limit_up_threshold=CONFIG['limit_up_threshold'],
                          volume_ratio=CONFIG['volume_increase_ratio']):
                # 计算次日收益率
                today_close = data.iloc[i]['收盘']
                next_close = data.iloc[i + 1]['收盘']
                next_return = (next_close / today_close) - 1

                total_trades += 1
                is_win = next_return >= CONFIG['profit_target']
                if is_win:
                    win_trades += 1
                total_return += next_return

                trade_details.append({
                    '代码': code_name.split()[0],
                    '名称': code_name.split()[1] if len(code_name.split()) > 1 else '',
                    '交易日期': trade_date,
                    '收盘价': today_close,
                    '次日收益率': next_return,
                    '是否盈利': is_win
                })

        # 计算统计信息
        win_rate = win_trades / total_trades if total_trades > 0 else 0.0
        avg_return = total_return / total_trades if total_trades > 0 else 0.0
        stats = {
            '总交易次数': total_trades,
            '胜率': win_rate,
            '平均收益率': avg_return,
            '盈利交易次数': win_trades,
            '亏损交易次数': total_trades - win_trades
        }

        # 保存交易详情
        if trade_details:
            pd.DataFrame(trade_details).to_csv(f'backtest_{code_name.split()[0]}.csv', index=False, encoding='utf-8-sig')
            logger.info(f"{code_name}: 回测结果已保存到 backtest_{code_name.split()[0]}.csv")

        logger.info(f"{code_name}: 回测完成 - 总交易次数={total_trades}, 胜率={win_rate:.2%}, 平均收益率={avg_return:.2%}")
        return stats

    except Exception as e:
        logger.error(f"{code_name}: 回测过程中发生错误 - {e}")
        return {'总交易次数': 0, '胜率': 0.0, '平均收益率': 0.0, '盈利交易次数': 0, '亏损交易次数': 0}