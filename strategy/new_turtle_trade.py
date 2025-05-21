# -*- coding: UTF-8 -*-
import pandas as pd
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 总市值（保留，未来可用于仓位管理）
BALANCE = 200000

def check_enter(code_name, data, end_date=None, threshold=60):
    """
    检查是否满足海龟交易策略入场条件：最后一个交易日收盘价为指定周期内最高价。
    
    参数:
        code_name (str): 股票代码或名称，用于日志记录
        data (pd.DataFrame): 包含'日期'和'收盘'列的股票数据
        end_date (str or datetime, optional): 数据截止日期，格式如 '2023-12-31'
        threshold (int): 检查周期（天数），默认60天
    
    返回:
        bool: True（满足入场条件），False（不满足）
    """
    try:
        # 参数校验
        if not isinstance(data, pd.DataFrame) or data.empty:
            logger.warning(f"{code_name}: 提供的数据为空或格式错误")
            return False
        
        if not {'日期', '收盘'}.issubset(data.columns):
            logger.warning(f"{code_name}: 数据缺少'日期'或'收盘'列")
            return False
        
        if threshold <= 0:
            logger.warning(f"{code_name}: threshold必须为正整数，当前为{threshold}")
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
        if len(data) < threshold:
            logger.warning(f"{code_name}: 数据长度{len(data)}不足threshold={threshold}")
            return False

        # 获取最后threshold天的数据
        data = data.tail(n=threshold)

        # 计算周期内最高收盘价
        max_price = data['收盘'].max()

        # 获取最后一个交易日的收盘价
        last_close = data.iloc[-1]['收盘']

        # 判断是否满足入场条件
        if last_close >= max_price:
            logger.info(f"{code_name}: 满足入场条件，最后收盘价={last_close} 为{threshold}天内最高")
            return True
        else:
            # logger.info(f"{code_name}: 不满足入场条件，最后收盘价={last_close} < 最高价={max_price}")
            return False

    except Exception as e:
        logger.error(f"{code_name}: 处理过程中发生错误 - {e}")
        return False