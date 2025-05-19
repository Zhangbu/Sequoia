# -*- encoding: UTF-8 -*-
import pandas as pd
import logging
import talib
from datetime import datetime, timedelta
import akshare as ak

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 策略配置
BALANCE = 200_000
CONFIG = {
    'rsi_lower': 30,
    'rsi_upper': 50,
    'volume_increase_min': 1.2,
    'volume_increase_max': 1.5,
    'lookback_days': 10,
    'kdj_j_upper': 80,
}

def check_enter(code_name, data, end_date=None, rsi_lower=30, rsi_upper=50, volume_lookback=10, volume_min_ratio=1.2, volume_max_ratio=1.5, kdj_j_upper=80):
    try:
        if not isinstance(data, pd.DataFrame) or data.empty:
            logger.warning(f"{code_name}: 提供的数据为空或格式错误")
            return False
        
        required_columns = {'日期', '收盘', '开盘', '最高', '最低', '成交量'}
        if not required_columns.issubset(data.columns):
            logger.warning(f"{code_name}: 数据缺少必要列 {required_columns - set(data.columns)}")
            return False
        
        if volume_lookback <= 0 or rsi_lower >= rsi_upper or volume_min_ratio <= 0 or volume_max_ratio <= volume_min_ratio:
            logger.warning(f"{code_name}: 参数设置错误")
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

        if len(data) < volume_lookback + 14:  # 确保足够数据计算RSI(14)和MACD
            logger.warning(f"{code_name}: 数据长度不足")
            return False

        # 计算技术指标
        close = data['收盘'].values
        high = data['最高'].values
        low = data['最低'].values

        # RSI (周期14)
        rsi = talib.RSI(close, timeperiod=14)
        last_rsi = rsi[-1]

        # MACD
        macd, signal, _ = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        last_diff = macd[-1]
        last_dea = signal[-1]
        prev_diff = macd[-2]
        prev_dea = signal[-2]

        # KDJ
        k, d = talib.STOCH(high, low, close, fastk_period=9, slowk_period=3, slowd_period=3)
        j = 3 * k - 2 * d
        last_k = k[-1]
        last_d = d[-1]
        last_j = j[-1]
        prev_k = k[-2]
        prev_d = d[-2]

        # 均线
        ma5 = talib.SMA(close, timeperiod=5)
        ma10 = talib.SMA(close, timeperiod=10)
        last_ma5 = ma5[-1]
        last_ma10 = ma10[-1]
        prev_ma5 = ma5[-2]
        prev_ma10 = ma10[-2]
        last_close = close[-1]

        # 成交量
        last_3d_volume = data['成交量'].iloc[-3:].mean()
        prev_volume = data['成交量'].iloc[-volume_lookback-3:-3].mean()

        if pd.isna(last_rsi) or pd.isna(last_diff) or pd.isna(last_k) or pd.isna(last_3d_volume):
            logger.warning(f"{code_name}: 技术指标计算失败")
            return False

        # RSI条件
        if not (rsi_lower <= last_rsi <= rsi_upper):
            return False

        # MACD金叉且接近零轴
        if not (prev_diff <= prev_dea and last_diff > last_dea and last_diff >= -0.1):
            return False

        # KDJ条件
        if not (prev_k <= prev_d and last_k > last_d and last_j < kdj_j_upper):
            return False

        # 均线条件
        if not (last_close > last_ma5 and prev_ma5 <= prev_ma10 and last_ma5 > last_ma10):
            return False

        # 成交量条件
        if not (volume_min_ratio * prev_volume <= last_3d_volume <= volume_max_ratio * prev_volume):
            return False

        logger.info(f"{code_name}: 满足入场条件，RSI={last_rsi:.2f}, MACD金叉, KDJ J={last_j:.2f}, 成交量={last_3d_volume:.2f}")
        return True

    except Exception as e:
        logger.error(f"{code_name}: 处理过程中发生错误 - {e}")
        return False