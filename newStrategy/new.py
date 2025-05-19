# -*- encoding: UTF-8 -*-
import pandas as pd
import logging
from datetime import datetime, timedelta
import akshare as ak

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 策略配置
BALANCE = 200_000
CONFIG = {
    'volume_increase_ratio': 1.5,
    'limit_up_threshold': 9.5,
    'profit_target': 0.03,
    'stop_loss': -0.05,
    'lookback_days': 5,
}

def check_enter(code_name, data, end_date=None, volume_lookback=5, limit_up_threshold=9.5, volume_ratio=1.5):
    try:
        if not isinstance(data, pd.DataFrame) or data.empty:
            logger.warning(f"{code_name}: 提供的数据为空或格式错误")
            return False
        
        required_columns = {'日期', '收盘', '涨跌幅', '成交量'}
        if not required_columns.issubset(data.columns):
            logger.warning(f"{code_name}: 数据缺少必要列 {required_columns - set(data.columns)}")
            return False
        
        if volume_lookback <= 0 or limit_up_threshold <= 0 or volume_ratio <= 0:
            logger.warning(f"{code_name}: 参数必须为正数")
            return False

        if end_date is not None:
            try:
                if isinstance(end_date, str):
                    end_date = pd.to_datetime(end_date)
                mask = (data['日期'] <= end_date)
                data = data.loc[mask].copy()
            except (ValueError, TypeError) as e:
                logger.warning(f"{code_name}: end_date格式错误 - {e}")
                return False

        if len(data) < volume_lookback + 1:
            logger.warning(f"{code_name}: 数据长度不足")
            return False

        last_row = data.iloc[-1]
        last_change = last_row['涨跌幅']
        last_volume = last_row['成交量']

        if last_change < limit_up_threshold:
            return False

        prev_data = data.iloc[-volume_lookback-1:-1]
        avg_volume = prev_data['成交量'].mean()

        if pd.isna(avg_volume) or avg_volume == 0:
            logger.warning(f"{code_name}: 无法计算历史平均成交量")
            return False

        if last_volume < volume_ratio * avg_volume:
            return False

        logger.info(f"{code_name}: 满足涨停入场条件，涨幅={last_change:.2f}%, 成交量={last_volume}")
        return True

    except Exception as e:
        logger.error(f"{code_name}: 处理过程中发生错误 - {e}")
        return False