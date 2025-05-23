# strategy/my_short_term_strategy.py
import pandas as pd
import logging
import talib
import numpy as np
import settings # Import settings to get global config

logger = logging.getLogger(__name__) # Get the shared logger

# Define a display name for the strategy
STRATEGY_NAME = "东方财富短线策略"

# --- Strategy Configuration Defaults ---
DEFAULT_STRATEGY_CONFIG = {
    'min_avg_daily_turnover_amount': 100_000_000,
    'avg_turnover_days': 20,
    'min_listed_days': 60,
    'ma5_cross_ma10_period': 3,
    'close_above_ma20': True,
    'macd_gold_cross_within_days': 3,
    'macd_dif_above_dea_and_zero': True,
    'volume_ratio_to_5day_avg_min': 1.5,
    'volume_ratio_to_5day_avg_max': 2.5,
    'volume_ratio_to_5day_avg_days': 5,
    'boll_break_middle_band': True,
    'rsi_period': 6,
    'rsi_cross_30': True,
    'rsi_lower_limit': 30,
    'rsi_upper_limit': 70,
    'kdj_gold_cross': True,
    'kdj_j_upper_limit': 50,
    'kdj_j_lower_limit': 20,
    'min_daily_turnover_rate': 3.0,
    'max_daily_turnover_rate': 25.0,
    'check_limit_up': False, # NEW: Add a configuration for checking limit up
    'limit_up_threshold': 9.5, # NEW: Threshold for limit up percentage
}

def get_strategy_config():
    """
    Fetches strategy-specific configuration from settings.
    Falls back to DEFAULT_STRATEGY_CONFIG if not found in global settings.
    """
    return settings.get_config().get('strategies', {}).get(STRATEGY_NAME, DEFAULT_STRATEGY_CONFIG)

def calculate_indicators(data: pd.DataFrame):
    """Calculates all necessary technical indicators for the strategy."""
    data['日期'] = pd.to_datetime(data['日期'])
    data = data.sort_values(by='日期').reset_index(drop=True)

    # --- Crucial Fix: Explicitly convert columns to float64 for TA-Lib ---
    # Include '涨跌幅' here to ensure it's numeric and handled for NaNs
    for col in ['收盘', '最高', '最低', '成交量', '成交额', '换手率', '涨跌幅']: # ADD '涨跌幅'
        data[col] = pd.to_numeric(data[col], errors='coerce')
        if col in ['收盘', '最高', '最低', '涨跌幅']: # For '涨跌幅' too, ffill is often appropriate
            data[col] = data[col].ffill()
        else:
            data[col] = data[col].fillna(0)

    close = data['收盘'].values.astype(np.float64)
    high = data['最高'].values.astype(np.float64)
    low = data['最低'].values.astype(np.float64)
    volume = data['成交量'].values.astype(np.float64)

    data['MA5'] = talib.SMA(close, timeperiod=5)
    data['MA10'] = talib.SMA(close, timeperiod=10)
    data['MA20'] = talib.SMA(close, timeperiod=20)

    data['MACD_DIF'], data['MACD_DEA'], data['MACD_HIST'] = talib.MACD(
        close, fastperiod=12, slowperiod=26, signalperiod=9
    )

    data['KDJ_K'], data['KDJ_D'] = talib.STOCH(
        high, low, close, fastk_period=9, slowk_period=3, slowd_period=3
    )
    data['KDJ_J'] = 3 * data['KDJ_K'] - 2 * data['KDJ_D']

    data['RSI'] = talib.RSI(close, timeperiod=get_strategy_config()['rsi_period'])

    data['BOLL_UPPER'], data['BOLL_MIDDLE'], data['BOLL_LOWER'] = talib.BBANDS(
        close, timeperiod=20, nbdevup=2, nbdevdn=2
    )

    data['VOL_MA5'] = talib.SMA(volume, timeperiod=get_strategy_config()['volume_ratio_to_5day_avg_days'])

    return data


def check_enter(stock_code_tuple, stock_data, end_date=None):
    """
    Checks if a stock meets the entry conditions for the '东方财富短线策略'.
    """
    code, name = stock_code_tuple
    config = get_strategy_config()

    logger.debug(f"[{name}({code})]: 开始检查东方财富App短线策略。", extra={'stock': code, 'strategy': STRATEGY_NAME})

    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty:
        logger.warning(f"[{name}({code})]: 策略收到空或非DataFrame数据，跳过。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    
    # Check for critical columns existence, including '涨跌幅'
    required_cols_for_strategy = {'日期', '收盘', '开盘', '最高', '最低', '成交量', '换手率', '成交额', '涨跌幅'} # ADD '涨跌幅'
    if not required_cols_for_strategy.issubset(stock_data.columns):
        missing_cols = required_cols_for_strategy - set(stock_data.columns)
        logger.warning(f"[{name}({code})]: 数据缺少策略所需关键列: {missing_cols}，跳过。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    stock_data['日期'] = pd.to_datetime(stock_data['日期'])
    stock_data_copy = stock_data.sort_values(by='日期').reset_index(drop=True).copy()

    if end_date:
        end_date = pd.to_datetime(end_date)
        data = stock_data_copy[stock_data_copy['日期'] <= end_date].copy()
    else:
        data = stock_data_copy.copy()

    max_ma_period = max(5, 10, 20)
    max_macd_period = 26 + 9
    max_stoch_period = 9 + 3 + 3
    max_rsi_period = config.get('rsi_period', 6)
    max_boll_period = 20
    max_vol_ma_period = config.get('volume_ratio_to_5day_avg_days', 5)
    max_turnover_days = config.get('avg_turnover_days', 20)
    
    min_required_len = max(
        max_ma_period, 
        max_macd_period, 
        max_stoch_period, 
        max_rsi_period, 
        max_boll_period, 
        max_vol_ma_period,
        max_turnover_days,
        config.get('min_listed_days', 60)
    ) + 5

    if len(data) < min_required_len:
        logger.debug(f"[{name}({code})]: 数据长度不足 {min_required_len} 天 ({len(data)}天)，无法计算所有指标，跳过。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    data = calculate_indicators(data) 

    if len(data) < 2 or data.iloc[-1].isnull().any() or data.iloc[-2].isnull().any():
        logger.debug(f"[{name}({code})]: 计算指标后数据不足两天或包含NaN值，无法进行前后日比较，跳过。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    latest_data = data.iloc[-1]
    prev_data = data.iloc[-2]

    # --- NEW: Check for Limit Up (涨停) condition ---
    if config['check_limit_up']:
        # Ensure '涨跌幅' is not NaN before checking
        if pd.isna(latest_data['涨跌幅']):
            logger.debug(f"[{name}({code})]: '涨跌幅'数据为NaN，无法判断涨停。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return False

        # For A-shares, 10% is the normal limit, but often 9.9% or 9.95% is used for a strict check
        # We use the configurable 'limit_up_threshold'
        if latest_data['涨跌幅'] < config['limit_up_threshold']:
            logger.debug(f"[{name}({code})]: 涨跌幅 ({latest_data['涨跌幅']:.2f}%) 未达到涨停阈值 ({config['limit_up_threshold']:.2f}%)。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return False

    # --- Basic Screening Conditions ---
    listed_days = len(data)
    if listed_days < config['min_listed_days']:
        logger.debug(f"[{name}({code})]: 上市天数不足 {config['min_listed_days']} 天 ({listed_days}天)。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    avg_daily_turnover_amount_series = data['成交额'].iloc[-config['avg_turnover_days']:]
    if avg_daily_turnover_amount_series.empty:
        logger.debug(f"[{name}({code})]: 近{config['avg_turnover_days']}天成交额数据不足，跳过。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    avg_daily_turnover_amount = avg_daily_turnover_amount_series.mean()

    if pd.isna(avg_daily_turnover_amount) or avg_daily_turnover_amount < config['min_avg_daily_turnover_amount']:
        logger.debug(f"[{name}({code})]: 日均成交额 ({avg_daily_turnover_amount/1_000_000:.2f}亿) 低于 {config['min_avg_daily_turnover_amount']/1_000_000:.2f}亿。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # --- Technical Indicator Screening (Core Logic) ---

    ma5_cross_ma10 = False
    lookback_start_idx = max(0, len(data) - config['ma5_cross_ma10_period'] - 1)
    for i in range(lookback_start_idx, len(data) - 1):
        if data['MA5'].iloc[i] <= data['MA10'].iloc[i] and data['MA5'].iloc[i+1] > data['MA10'].iloc[i+1]:
            ma5_cross_ma10 = True
            break
    if not ma5_cross_ma10:
        logger.debug(f"[{name}({code})]: 近{config['ma5_cross_ma10_period']}天未发生5日均线上穿10日均线。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    if config['close_above_ma20'] and not (latest_data['收盘'] > latest_data['MA20']):
        logger.debug(f"[{name}({code})]: 股价 ({latest_data['收盘']:.2f}) 未高于20日均线 ({latest_data['MA20']:.2f})。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    macd_gold_cross = False
    lookback_start_idx_macd = max(0, len(data) - config['macd_gold_cross_within_days'] - 1)
    for i in range(lookback_start_idx_macd, len(data) - 1):
        if data['MACD_DIF'].iloc[i] <= data['MACD_DEA'].iloc[i] and \
           data['MACD_DIF'].iloc[i+1] > data['MACD_DEA'].iloc[i+1]:
            macd_gold_cross = True
            break
    if not macd_gold_cross:
        logger.debug(f"[{name}({code})]: 近{config['macd_gold_cross_within_days']}天未发生MACD金叉。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    if config['macd_dif_above_dea_and_zero'] and not (latest_data['MACD_DIF'] > latest_data['MACD_DEA'] and latest_data['MACD_DIF'] > 0):
        logger.debug(f"[{name}({code})]: MACD不满足 DIF > DEA 且 DIF > 0。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False 
    
    if latest_data['VOL_MA5'] <= 0 or pd.isna(latest_data['VOL_MA5']):
        logger.debug(f"[{name}({code})]: 5日均量为零、负或NaN，无法计算放量比。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    
    volume_ratio = latest_data['成交量'] / latest_data['VOL_MA5']
    if not (config['volume_ratio_to_5day_avg_min'] <= volume_ratio <= config['volume_ratio_to_5day_avg_max']):
        logger.debug(f"[{name}({code})]: 成交量 ({latest_data['成交量']:.0f}) 不满足放量条件 ({volume_ratio:.2f}倍5日均量)，要求 {config['volume_ratio_to_5day_avg_min']} - {config['volume_ratio_to_5day_avg_max']} 倍。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    
    if config['boll_break_middle_band'] and not (
        not pd.isna(prev_data['收盘']) and not pd.isna(prev_data['BOLL_MIDDLE']) and \
        not pd.isna(latest_data['收盘']) and not pd.isna(latest_data['BOLL_MIDDLE']) and \
        prev_data['收盘'] <= prev_data['BOLL_MIDDLE'] and latest_data['收盘'] > latest_data['BOLL_MIDDLE']
    ):
        logger.debug(f"[{name}({code})]: 未上穿布林带中轨或布林带数据异常。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    if config['rsi_cross_30'] and (len(data) > 1) and not (
        not pd.isna(data['RSI'].iloc[-2]) and not pd.isna(latest_data['RSI']) and \
        data['RSI'].iloc[-2] <= config['rsi_lower_limit'] and latest_data['RSI'] > config['rsi_lower_limit']
    ):
        logger.debug(f"[{name}({code})]: RSI ({latest_data['RSI']:.2f}) 未上穿 {config['rsi_lower_limit']} 或RSI数据异常。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    if not (not pd.isna(latest_data['RSI']) and \
            config['rsi_lower_limit'] <= latest_data['RSI'] <= config['rsi_upper_limit']):
        logger.debug(f"[{name}({code})]: RSI ({latest_data['RSI']:.2f}) 不在 {config['rsi_lower_limit']}-{config['rsi_upper_limit']} 区间或RSI数据异常。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    if config['kdj_gold_cross'] and (len(data) > 1) and not (
        not pd.isna(prev_data['KDJ_K']) and not pd.isna(prev_data['KDJ_D']) and \
        not pd.isna(latest_data['KDJ_K']) and not pd.isna(latest_data['KDJ_D']) and \
        prev_data['KDJ_K'] <= prev_data['KDJ_D'] and latest_data['KDJ_K'] > latest_data['KDJ_D']
    ):
        logger.debug(f"[{name}({code})]: KDJ未金叉或KDJ数据异常。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    if not (not pd.isna(latest_data['KDJ_J']) and \
            config['kdj_j_lower_limit'] <= latest_data['KDJ_J'] < config['kdj_j_upper_limit']):
        logger.debug(f"[{name}({code})]: KDJ J值 ({latest_data['KDJ_J']:.2f}) 不在 {config['kdj_j_lower_limit']}-{config['kdj_j_upper_limit']} 区间或KDJ数据异常。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    if not (not pd.isna(latest_data['换手率']) and \
            config['min_daily_turnover_rate'] <= latest_data['换手率'] <= config['max_daily_turnover_rate']):
        logger.debug(f"[{name}({code})]: 换手率 ({latest_data['换手率']:.2f}%) 不在 {config['min_daily_turnover_rate']}-{config['max_daily_turnover_rate']}% 区间或换手率数据异常。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    logger.info(f"[{name}({code})]: ✨ 股票符合东方财富App短线策略所有入场条件！", extra={'stock': code, 'strategy': STRATEGY_NAME})
    return True