# strategy/oversold_breakout_strategy.py
import pandas as pd
import logging
import talib
import numpy as np
import settings # Import settings to get global config

# --- 使用你之前已修正的 get_strategy_config 和 _ensure_scalar_float ---
# (这里假设你已经将那两个函数放在一个公共模块或 settings.py 里，或者直接复制过来)
# 为了完整性，我将它们包含进来，并使用之前讨论的修正版本。

logger = logging.getLogger(__name__)

# Define a display name for the strategy
STRATEGY_NAME = "超跌反弹与趋势突破策略"

# --- Strategy Configuration Defaults ---
DEFAULT_STRATEGY_CONFIG = {
    # General Filters
    'min_avg_daily_turnover_amount': 50_000_000, # 较低的成交额门槛
    'avg_turnover_days': 20,
    'min_listed_days': 60,

    # Shared Indicator Params
    'rsi_period': 14, # 常用RSI周期
    'macd_fast_period': 12,
    'macd_slow_period': 26,
    'macd_signal_period': 9,
    'boll_period': 20,
    'boll_nbdev': 2,
    'volume_avg_days': 5,

    # --- Oversold Bounce Buy Conditions ---
    'buy_rsi_oversold_threshold': 25, # RSI进入此值以下为超卖
    'buy_rsi_oversold_cross_up': True, # 是否要求RSI上穿超卖线
    'buy_kdj_j_lower_limit': 10,     # KDJ J值低于此值为低位

    # --- Trend Breakout Buy Conditions ---
    'buy_breakout_ma_period': 20,        # 突破此均线
    'buy_breakout_volume_ratio_min': 1.5, # 突破时成交量至少为N日均量的X倍
    'buy_macd_cross_zero_or_gold': True, # MACD金叉或DIF上穿0轴

    # --- Exit Conditions ---
    'sell_rsi_overbought_threshold': 75, # RSI进入此值以上为超买
    'sell_rsi_overbought_cross_down': True, # 是否要求RSI下穿超买线
    'sell_macd_dead_cross': True,       # MACD死叉作为卖出信号
    'sell_price_below_ma_period': 10,   # 股价跌破此均线作为卖出信号
    'sell_stop_loss_pct': 0.07, # 7% 止损 (这个在简单check_exit中较难实现，除非传入买入价)
    'sell_take_profit_pct': 0.15 # 15% 止盈 (同上)
}

# --- 复用或调整你之前的配置加载和数据清理函数 ---
def _ensure_scalar_float(value_to_convert):
    if pd.isna(value_to_convert):
        raise ValueError("Cannot convert NaN or None to float for a required configuration value.")
    if isinstance(value_to_convert, str):
        cleaned_value = value_to_convert.replace('%', '').strip()
        if cleaned_value.endswith(','):
            cleaned_value = cleaned_value[:-1].strip()
        try:
            return float(cleaned_value)
        except ValueError as e:
            raise ValueError(f"Could not convert string '{value_to_convert}' (cleaned: '{cleaned_value}') to float: {e}")
    elif pd.api.types.is_list_like(value_to_convert) and not isinstance(value_to_convert, (str, bytes)):
        if len(value_to_convert) == 1:
            element = value_to_convert[0] if not isinstance(value_to_convert, pd.Series) else value_to_convert.iloc[0]
            return _ensure_scalar_float(element)
        else:
            raise ValueError(f"Expected a single value, got list-like with {len(value_to_convert)} elements: {value_to_convert}")
    try:
        return float(value_to_convert)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Could not convert value '{value_to_convert}' of type {type(value_to_convert)} to float: {e}")

def get_strategy_config_ob(): # Renamed to avoid conflict if in same file
    raw_config = settings.get_config().get('strategies', {}).get(STRATEGY_NAME, DEFAULT_STRATEGY_CONFIG)
    processed_config = raw_config.copy()
    
    numeric_keys = [
        'min_avg_daily_turnover_amount', 'avg_turnover_days', 'min_listed_days',
        'rsi_period', 'macd_fast_period', 'macd_slow_period', 'macd_signal_period',
        'boll_period', 'boll_nbdev', 'volume_avg_days',
        'buy_rsi_oversold_threshold', 'buy_kdj_j_lower_limit',
        'buy_breakout_ma_period', 'buy_breakout_volume_ratio_min',
        'sell_rsi_overbought_threshold', 'sell_price_below_ma_period',
        'sell_stop_loss_pct', 'sell_take_profit_pct'
    ]
    integer_keys = [ # Keys that must be integers
        'avg_turnover_days', 'min_listed_days', 'rsi_period', 'macd_fast_period',
        'macd_slow_period', 'macd_signal_period', 'boll_period', 'volume_avg_days',
        'buy_breakout_ma_period', 'sell_price_below_ma_period'
    ]
    
    for key in numeric_keys:
        if key in processed_config:
            try:
                processed_config[key] = _ensure_scalar_float(processed_config[key])
            except (ValueError, TypeError) as e:
                logger.error(f"Error converting config key '{key}' with value '{processed_config[key]}' for {STRATEGY_NAME}: {e}. Using default.")
                if key in DEFAULT_STRATEGY_CONFIG:
                    processed_config[key] = _ensure_scalar_float(DEFAULT_STRATEGY_CONFIG[key])
                else:
                    raise ValueError(f"Config value for '{key}' is invalid and no default for {STRATEGY_NAME}.")
        elif key in DEFAULT_STRATEGY_CONFIG:
            processed_config[key] = _ensure_scalar_float(DEFAULT_STRATEGY_CONFIG[key])

    for key in integer_keys:
        if key in processed_config and isinstance(processed_config[key], float):
            if processed_config[key].is_integer():
                processed_config[key] = int(processed_config[key])
            else:
                logger.warning(f"Config key '{key}' for {STRATEGY_NAME} has non-integer float '{processed_config[key]}'. Truncating.")
                processed_config[key] = int(processed_config[key])

    boolean_keys = [
        'buy_rsi_oversold_cross_up', 'buy_macd_cross_zero_or_gold',
        'sell_rsi_overbought_cross_down', 'sell_macd_dead_cross'
    ]
    for key in boolean_keys:
        if key in processed_config and not isinstance(processed_config[key], bool):
            if isinstance(processed_config[key], str):
                val_str = processed_config[key].strip()
                if val_str.endswith(','): val_str = val_str[:-1].strip()
                val_str_lower = val_str.lower()
                if val_str_lower == 'true': processed_config[key] = True
                elif val_str_lower == 'false': processed_config[key] = False
                else:
                    logger.warning(f"Config key '{key}' for {STRATEGY_NAME} has non-boolean string '{processed_config[key]}'. Using default.")
                    processed_config[key] = DEFAULT_STRATEGY_CONFIG[key]
            else:
                logger.warning(f"Config key '{key}' for {STRATEGY_NAME} is not bool. Using default.")
                processed_config[key] = DEFAULT_STRATEGY_CONFIG[key]
        elif key not in processed_config and key in DEFAULT_STRATEGY_CONFIG:
             processed_config[key] = DEFAULT_STRATEGY_CONFIG[key]
    return processed_config

def calculate_indicators_ob(data: pd.DataFrame, config): # Pass config for periods
    """Calculates all necessary technical indicators for the OB strategy."""
    data_copy = data.copy()
    data_copy['日期'] = pd.to_datetime(data_copy['日期'])
    data_copy = data_copy.sort_values(by='日期').reset_index(drop=True)

    cols_to_convert = ['收盘', '最高', '最低', '成交量', '成交额', '换手率', '涨跌幅', '开盘']
    for col in cols_to_convert:
        if col in data_copy.columns:
            if data_copy[col].dtype == 'object':
                 data_copy[col] = data_copy[col].astype(str).str.replace('%', '', regex=False).str.strip()
            data_copy[col] = pd.to_numeric(data_copy[col], errors='coerce')
            if col in ['收盘', '最高', '最低', '涨跌幅', '开盘']: 
                data_copy[col] = data_copy[col].ffill()
            elif col in ['成交量', '成交额', '换手率']:
                data_copy[col] = data_copy[col].fillna(0)
        else:
            data_copy[col] = np.nan

    close = data_copy['收盘'].values.astype(np.float64)
    high = data_copy['最高'].values.astype(np.float64)
    low = data_copy['最低'].values.astype(np.float64)
    volume = data_copy['成交量'].values.astype(np.float64)

    # MAs
    data_copy[f"MA{config['buy_breakout_ma_period']}"] = talib.SMA(close, timeperiod=config['buy_breakout_ma_period'])
    data_copy[f"MA{config['sell_price_below_ma_period']}"] = talib.SMA(close, timeperiod=config['sell_price_below_ma_period'])
    
    # RSI
    data_copy['RSI'] = talib.RSI(close, timeperiod=config['rsi_period'])

    # KDJ
    data_copy['KDJ_K'], data_copy['KDJ_D'] = talib.STOCH(
        high, low, close, fastk_period=9, slowk_period=3, slowd_period=3 # Standard KDJ params
    )
    data_copy['KDJ_J'] = 3 * data_copy['KDJ_K'] - 2 * data_copy['KDJ_D']

    # MACD
    data_copy['MACD_DIF'], data_copy['MACD_DEA'], data_copy['MACD_HIST'] = talib.MACD(
        close, fastperiod=config['macd_fast_period'], slowperiod=config['macd_slow_period'], signalperiod=config['macd_signal_period']
    )

    # Bollinger Bands
    data_copy['BOLL_UPPER'], data_copy['BOLL_MIDDLE'], data_copy['BOLL_LOWER'] = talib.BBANDS(
        close, timeperiod=config['boll_period'], nbdevup=config['boll_nbdev'], nbdevdn=config['boll_nbdev']
    )

    # Volume MA
    data_copy['VOL_MA'] = talib.SMA(volume, timeperiod=config['volume_avg_days'])
    
    return data_copy


def check_common_filters(stock_code_tuple, stock_data_with_indicators, config, strategy_name_log):
    """Common preliminary checks for any strategy."""
    code, name = stock_code_tuple
    
    if len(stock_data_with_indicators) < 2:
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 数据不足两日，跳过。", extra={'stock': code})
        return False, None, None # Signal failure, no latest/prev data

    latest_data = stock_data_with_indicators.iloc[-1]
    prev_data = stock_data_with_indicators.iloc[-2]

    # Basic NaN check for essential fields (can be expanded)
    essential_fields = ['收盘', 'RSI', 'KDJ_J', 'MACD_DIF', 'MACD_DEA', 'BOLL_LOWER', 'BOLL_UPPER', f"MA{config['buy_breakout_ma_period']}", 'VOL_MA']
    if latest_data[essential_fields].isnull().any():
        nan_fields = latest_data[essential_fields].index[latest_data[essential_fields].isnull()].tolist()
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 最新数据点指标存在NaN: {nan_fields}，跳过。", extra={'stock': code})
        return False, None, None
    if prev_data[essential_fields].isnull().any(): # Also check prev_data if used for crosses
        nan_fields_prev = prev_data[essential_fields].index[prev_data[essential_fields].isnull()].tolist()
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 前一数据点指标存在NaN: {nan_fields_prev}，跳过。", extra={'stock': code})
        return False, None, None


    # Listed days (using length of data after indicator calculation)
    if len(stock_data_with_indicators) < config['min_listed_days']:
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 上市天数 ({len(stock_data_with_indicators)}) 不足 {config['min_listed_days']} 天，跳过。", extra={'stock': code})
        return False, latest_data, prev_data

    # Avg daily turnover
    if len(stock_data_with_indicators['成交额']) < config['avg_turnover_days']:
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 成交额数据不足 {config['avg_turnover_days']} 天计算均值，跳过。", extra={'stock': code})
        return False, latest_data, prev_data
        
    avg_turnover_series = stock_data_with_indicators['成交额'].iloc[-config['avg_turnover_days']:]
    avg_daily_turnover = avg_turnover_series.mean()
    if pd.isna(avg_daily_turnover) or avg_daily_turnover < config['min_avg_daily_turnover_amount']:
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 日均成交额 ({avg_daily_turnover/1e8:.2f}亿) 低于阈值 ({config['min_avg_daily_turnover_amount']/1e8:.2f}亿)，跳过。", extra={'stock': code})
        return False, latest_data, prev_data
        
    return True, latest_data, prev_data # Passed common filters


def check_enter(stock_code_tuple, stock_data, end_date=None):
    code, name = stock_code_tuple
    config = get_strategy_config_ob()
    strategy_name_log = STRATEGY_NAME + " Enter"

    logger.debug(f"[{name}({code})]: 开始检查 [{STRATEGY_NAME}] 入场条件。", extra={'stock': code})

    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty:
        logger.warning(f"[{name}({code})][{strategy_name_log}]: 收到空或非DataFrame数据，跳过。", extra={'stock': code})
        return False

    data_for_period = stock_data.copy()
    if end_date:
        end_date_dt = pd.to_datetime(end_date)
        data_for_period = data_for_period[data_for_period['日期'] <= end_date_dt].copy()
    
    # Min length check before indicator calculation
    # Rough estimation, can be refined
    min_len_indic = max(config['rsi_period'], config['macd_slow_period'] + config['macd_signal_period'], 
                        config['boll_period'], config['volume_avg_days'], 
                        config['buy_breakout_ma_period'], config['min_listed_days']) + 5 
    if len(data_for_period) < min_len_indic:
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 原始数据长度不足 {min_len_indic}，跳过。", extra={'stock': code})
        return False

    data_with_indicators = calculate_indicators_ob(data_for_period, config)
    
    # Common filters (listed days, turnover, basic NaN)
    pass_common, latest_data, prev_data = check_common_filters(stock_code_tuple, data_with_indicators, config, strategy_name_log)
    if not pass_common:
        return False # Reason already logged in check_common_filters

    # --- Condition Group 1: Oversold Bounce ---
    oversold_bounce_signal = False
    if config['buy_rsi_oversold_cross_up']:
        rsi_crossed_up = prev_data['RSI'] <= config['buy_rsi_oversold_threshold'] and \
                         latest_data['RSI'] > config['buy_rsi_oversold_threshold']
    else: # Just needs to be above threshold if not requiring cross
        rsi_crossed_up = latest_data['RSI'] > config['buy_rsi_oversold_threshold'] 
        # This interpretation might need adjustment: usually "oversold bounce" implies it WAS oversold.
        # A better non-cross check might be: prev_data['RSI'] <= config['buy_rsi_oversold_threshold'] AND latest_data['RSI'] > prev_data['RSI']

    kdj_j_low = latest_data['KDJ_J'] < config['buy_kdj_j_lower_limit']
    
    # Price near or below Boll Lower and recovers
    price_near_boll_lower = latest_data['收盘'] <= latest_data['BOLL_LOWER'] * 1.01 # Within 1% of lower band or below
    recovered_above_boll_lower = prev_data['收盘'] < prev_data['BOLL_LOWER'] and latest_data['收盘'] > latest_data['BOLL_LOWER']


    if rsi_crossed_up and kdj_j_low: # Add more conditions like price_near_boll_lower if desired
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 符合超跌反弹条件 (RSI上穿, KDJ J低位)。RSI: {prev_data['RSI']:.2f}->{latest_data['RSI']:.2f}, J: {latest_data['KDJ_J']:.2f}", extra={'stock': code})
        oversold_bounce_signal = True


    # --- Condition Group 2: Trend Breakout ---
    trend_breakout_signal = False
    breakout_ma_col = f"MA{config['buy_breakout_ma_period']}"
    
    # Price breaks above MA
    price_breakout_ma = prev_data['收盘'] <= prev_data[breakout_ma_col] and \
                        latest_data['收盘'] > latest_data[breakout_ma_col]
    
    # Volume condition
    volume_confirms_breakout = False
    if latest_data['VOL_MA'] > 0 : # Avoid division by zero
        volume_ratio = latest_data['成交量'] / latest_data['VOL_MA']
        if volume_ratio >= config['buy_breakout_volume_ratio_min']:
            volume_confirms_breakout = True
    
    # MACD condition
    macd_confirms = False
    if config['buy_macd_cross_zero_or_gold']:
        macd_gold_cross = prev_data['MACD_DIF'] <= prev_data['MACD_DEA'] and \
                          latest_data['MACD_DIF'] > latest_data['MACD_DEA']
        dif_cross_zero = prev_data['MACD_DIF'] <= 0 and latest_data['MACD_DIF'] > 0
        if macd_gold_cross or dif_cross_zero:
            macd_confirms = True
            
    if price_breakout_ma and volume_confirms_breakout and macd_confirms:
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 符合趋势突破条件 (突破MA,放量,MACD确认)。Vol Ratio: {volume_ratio:.2f}", extra={'stock': code})
        trend_breakout_signal = True

    if oversold_bounce_signal or trend_breakout_signal:
        logger.info(f"[{name}({code})]: ✨ 符合 [{STRATEGY_NAME}] 入场条件！", extra={'stock': code})
        return True
    
    logger.debug(f"[{name}({code})][{strategy_name_log}]: 未符合任一入场条件组。", extra={'stock': code})
    return False


def check_exit_oversold_breakout(stock_code_tuple, stock_data, end_date=None, buy_price=None): # buy_price for stop_loss/take_profit
    code, name = stock_code_tuple
    config = get_strategy_config_ob()
    strategy_name_log = STRATEGY_NAME + " Exit"

    logger.debug(f"[{name}({code})]: 开始检查 [{STRATEGY_NAME}] 卖出条件。", extra={'stock': code})

    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty:
        # This function would typically be called with data for a stock you HOLD
        logger.warning(f"[{name}({code})][{strategy_name_log}]: 收到空或非DataFrame数据，跳过。", extra={'stock': code})
        return False # No action if no data

    data_for_period = stock_data.copy()
    if end_date:
        end_date_dt = pd.to_datetime(end_date)
        data_for_period = data_for_period[data_for_period['日期'] <= end_date_dt].copy()

    # Min length check before indicator calculation (can be less strict for exit)
    min_len_indic = max(config['rsi_period'], config['macd_slow_period'], config['sell_price_below_ma_period']) + 2
    if len(data_for_period) < min_len_indic:
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 原始数据长度不足 {min_len_indic} 计算退出指标，跳过。", extra={'stock': code})
        return False
        
    data_with_indicators = calculate_indicators_ob(data_for_period, config)

    if len(data_with_indicators) < 2: # Need at least current and previous for some checks
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 计算指标后数据不足两日，跳过。", extra={'stock': code})
        return False

    latest_data = data_with_indicators.iloc[-1]
    prev_data = data_with_indicators.iloc[-2] # Needed for cross checks

    essential_exit_fields = ['收盘', 'RSI', 'MACD_DIF', 'MACD_DEA', f"MA{config['sell_price_below_ma_period']}"]
    if latest_data[essential_exit_fields].isnull().any() or prev_data[essential_exit_fields].isnull().any():
        logger.debug(f"[{name}({code})][{strategy_name_log}]: 最新或前一日退出关键指标存在NaN，跳过。", extra={'stock': code})
        return False

    # --- Exit Condition 1: Stop Loss / Take Profit (if buy_price is provided) ---
    # This is a simplified implementation. A real system would track positions.
    if buy_price is not None:
        if latest_data['收盘'] <= buy_price * (1 - config['sell_stop_loss_pct']):
            logger.info(f"[{name}({code})][{strategy_name_log}]: 触发止损卖出。买入价: {buy_price:.2f}, 当前价: {latest_data['收盘']:.2f}", extra={'stock': code})
            return True
        if latest_data['收盘'] >= buy_price * (1 + config['sell_take_profit_pct']):
            logger.info(f"[{name}({code})][{strategy_name_log}]: 触发止盈卖出。买入价: {buy_price:.2f}, 当前价: {latest_data['收盘']:.2f}", extra={'stock': code})
            return True

    # --- Exit Condition 2: RSI Overbought and Crosses Down ---
    if config['sell_rsi_overbought_cross_down']:
        if prev_data['RSI'] >= config['sell_rsi_overbought_threshold'] and \
           latest_data['RSI'] < config['sell_rsi_overbought_threshold']:
            logger.info(f"[{name}({code})][{strategy_name_log}]: RSI超买后下穿卖出。RSI: {prev_data['RSI']:.2f}->{latest_data['RSI']:.2f}", extra={'stock': code})
            return True
    elif latest_data['RSI'] >= config['sell_rsi_overbought_threshold']: # Simpler: RSI just in overbought
        logger.info(f"[{name}({code})][{strategy_name_log}]: RSI处于超买区 ({latest_data['RSI']:.2f})，考虑卖出。", extra={'stock': code})
        # Depending on strictness, this alone might not be a sell signal for some.
        # return True # Uncomment if this is a sufficient sell signal

    # --- Exit Condition 3: MACD Dead Cross ---
    if config['sell_macd_dead_cross']:
        if prev_data['MACD_DIF'] >= prev_data['MACD_DEA'] and \
           latest_data['MACD_DIF'] < latest_data['MACD_DEA']:
            logger.info(f"[{name}({code})][{strategy_name_log}]: MACD死叉卖出。", extra={'stock': code})
            return True

    # --- Exit Condition 4: Price drops below MA ---
    sell_ma_col = f"MA{config['sell_price_below_ma_period']}"
    if latest_data['收盘'] < latest_data[sell_ma_col] and prev_data['收盘'] >= prev_data[sell_ma_col]: # Crossing down
        logger.info(f"[{name}({code})][{strategy_name_log}]: 股价跌破MA{config['sell_price_below_ma_period']}卖出。", extra={'stock': code})
        return True
        
    logger.debug(f"[{name}({code})][{strategy_name_log}]: 未符合卖出条件。", extra={'stock': code})
    return False

# Example of how you might integrate into your main loop (conceptual)
# if __name__ == '__main__':
#     # Assume 'settings' is configured and 'get_stock_data_somehow' exists
#     # And you have a list of stocks: stock_list = [('000001', '平安银行'), ...]
#     
#     # Mock settings for the example
#     class MockSettings:
#         def get_config(self):
#             return {
#                 'strategies': {
#                     STRATEGY_NAME: { # You can override defaults here
#                         'buy_rsi_oversold_threshold': 20 
#                     }
#                 }
#             }
#     settings = MockSettings() # Replace with your actual settings
#
#     today_date_str = "2023-10-27" # Example date
#
#     # In a real trading system, you'd also manage a portfolio of held stocks
#     # held_stocks_with_buy_price = { '000001': 10.50, ... } 
#
#     for stock_code, stock_name in stock_list:
#         stock_tuple = (stock_code, stock_name)
#         # stock_df = get_stock_data_somehow(stock_code, end_date=today_date_str, lookback_days=120) # Get enough data
#         stock_df = pd.DataFrame() # Placeholder for actual data loading
#
#         # Check for buy signals
#         # if stock_code not in held_stocks_with_buy_price: # Only check buy for stocks not held
#         if check_enter_oversold_breakout(stock_tuple, stock_df, end_date=today_date_str):
#             print(f"BUY SIGNAL for {stock_name} ({stock_code}) on {today_date_str}")
#             # Add to held_stocks_with_buy_price, record buy_price etc.
#
#         # Check for sell signals for held stocks
#         # elif stock_code in held_stocks_with_buy_price:
#         #    buy_price = held_stocks_with_buy_price[stock_code]
#         #    if check_exit_oversold_breakout(stock_tuple, stock_df, end_date=today_date_str, buy_price=buy_price):
#         #        print(f"SELL SIGNAL for {stock_name} ({stock_code}) on {today_date_str}")
#         #        # Remove from held_stocks_with_buy_price