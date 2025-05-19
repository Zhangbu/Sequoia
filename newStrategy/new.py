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
    print(f"检查进入策略的stock: {code_name}")
    # print(f"检查进入策略的data: {data}")
    # print(f"检查进入策略的end_date: {end_date}")
    try:
        # 数据校验（同原代码）
        if not isinstance(data, pd.DataFrame) or data.empty or not {'日期', '收盘', '开盘', '最高', '最低', '成交量', '换手率'}.issubset(data.columns):
            logger.warning(f"{code_name}: 数据格式错误或缺少必要列")
            return False
        
        if volume_lookback <= 0 or rsi_lower >= rsi_upper or volume_min_ratio <= 0 or volume_max_ratio <= volume_min_ratio:
            logger.warning(f"{code_name}: 参数设置错误")
            return False

        if end_date is not None:
            try:
                end_date = pd.to_datetime(end_date)
                data = data[data['日期'] <= end_date].copy()
            except (ValueError, TypeError) as e:
                logger.warning(f"{code_name}: end_date格式错误 - {e}")
                return False

        if len(data) < volume_lookback + 14:
            logger.warning(f"{code_name}: 数据长度不足")
            return False

        # 计算技术指标
        close = data['收盘'].values
        high = data['最高'].values
        low = data['最低'].values

        rsi = talib.RSI(close, timeperiod=14)
        macd, signal, _ = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        k, d = talib.STOCH(high, low, close, fastk_period=9, slowk_period=3, slowd_period=3)
        j = 3 * k - 2 * d
        ma5 = talib.SMA(close, timeperiod=5)
        ma10 = talib.SMA(close, timeperiod=10)
        upper, _, _ = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)

        last_rsi = rsi[-1]
        last_diff = macd[-1]
        last_dea = signal[-1]
        prev_diff = macd[-2]
        prev_dea = signal[-2]
        last_k = k[-1]
        last_d = d[-1]
        last_j = j[-1]
        prev_k = k[-2]
        prev_d = d[-2]
        last_ma5 = ma5[-1]
        last_ma10 = ma10[-1]
        prev_ma5 = ma5[-2]
        prev_ma10 = ma10[-2]
        last_close = close[-1]
        last_3d_volume = data['成交量'].iloc[-3:].mean()
        prev_volume = data['成交量'].iloc[-volume_lookback-3:-3].mean()
        last_volume = data['成交量'].iloc[-1]

        # 条件检查
        if pd.isna(last_rsi) or pd.isna(last_diff) or pd.isna(last_k) or pd.isna(last_3d_volume):
            logger.warning(f"{code_name}: 技术指标计算失败")
            return False

        if not (rsi_lower <= last_rsi <= rsi_upper):
            logger.warning(f"{code_name}: RSI={last_rsi:.2f} 不满足 {rsi_lower}-{rsi_upper}")
            return False

        if not (prev_diff <= prev_dea and last_diff > last_dea and last_diff >= -0.1):
            logger.warning(f"{code_name}: MACD不满足金叉或零轴条件")
            return False

        if not (prev_k <= prev_d and last_k > last_d and last_j < kdj_j_upper):
            logger.warning(f"{code_name}: KDJ J={last_j:.2f} 不满足条件")
            return False

        if not (last_close > last_ma5 and prev_ma5 <= prev_ma10 and last_ma5 > last_ma10):
            logger.warning(f"{code_name}: 均线不满足多头排列")
            return False

        if not (volume_min_ratio * prev_volume <= last_3d_volume <= volume_max_ratio * prev_volume):
            logger.warning(f"{code_name}: 成交量不满足放大条件")
            return False

        if last_volume > prev_volume * 2:
            logger.warning(f"{code_name}: 单日成交量异常放量")
            return False

        if last_close > upper[-1] * 0.98:
            logger.warning(f"{code_name}: 股价接近布林带上轨")
            return False

        if data['换手率'].iloc[-1] < 8:
            logger.warning(f"{code_name}: 换手率={data['换手率'].iloc[-1]:.2f}% 低于8%")
            return False

        # 主力资金（示例，需实际数据支持）
        # fund_flow = ak.stock_individual_fund_flow(stock=code_name.split()[0], market="sh" if code_name.startswith("6") else "sz")
        # if fund_flow['主力净流入'].iloc[-3:].sum() < 50000000:
        #     logger.warning(f"{code_name}: 主力资金净流入不足")
        #     return False

        logger.info(f"{code_name}: 满足入场条件，RSI={last_rsi:.2f}, MACD金叉, KDJ J={last_j:.2f}, 成交量={last_3d_volume:.2f}")
        return True

    except Exception as e:
        logger.error(f"{code_name}: 处理过程中发生错误 - {e}")
        return False

# def check_enter(code_name, data, end_date=None, rsi_lower=30, rsi_upper=50, volume_lookback=10, volume_min_ratio=1.2, volume_max_ratio=1.5, kdj_j_upper=80):
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