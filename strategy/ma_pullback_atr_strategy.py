# strategy/ma_pullback_atr_strategy.py
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
STRATEGY_NAME = "均线多头回调与ATR止损策略" # MPA for MA Pullback ATR

# --- Strategy Configuration Defaults ---
DEFAULT_STRATEGY_CONFIG = {
    # Basic Filters (can be inherited or customized)
    'min_avg_daily_turnover_amount': 100_000_000,
    'avg_turnover_days': 20,
    'min_listed_days': 60,
    'min_daily_turnover_rate': 2.0,
    'max_daily_turnover_rate': 30.0,

    # MA Periods for Trend
    'ma_short_period': 5,
    'ma_medium_period': 20,
    'ma_long_period': 60,

    # Pullback Condition
    'pullback_to_ma_period': 20, # Price pulls back to this MA
    'rsi_period_pullback': 14,
    'rsi_pullback_lower_threshold': 40, # RSI pulls back to around this level
    'rsi_pullback_upper_threshold': 55, # And starts to turn up from below this

    # MACD Condition
    'macd_fast_period': 12,
    'macd_slow_period': 26,
    'macd_signal_period': 9,

    # ATR Stop Loss
    'atr_period': 14,
    'atr_multiplier_stop_loss': 2.0, # Stop loss = KeyLevel - ATR_Multiplier * ATR
    'atr_stop_loss_key_ma_period': 20, # Key MA to base ATR stop from (e.g., MA20)

    # Exit based on Trend Reversal
    'exit_ma_short_cross_medium_period': 5, # e.g. MA5
    # 'exit_ma_medium_cross_period': 20, # e.g. MA20 (crossed by MA5)

    # (Optional) Profit Target - for a full backtest system
    'profit_target_pct': None, # e.g., 0.20 for 20%
}


# --- 复用或调整你之前的配置加载和数据清理函数 ---
# (这里假设 settings.py 已经包含了 _ensure_scalar_float 和通用的 get_config)
# 我们需要为这个新策略写一个特定的 get_strategy_config_mpa
def get_strategy_config_mpa():
    """
    Fetches strategy-specific configuration for MPA strategy.
    Falls back to DEFAULT_STRATEGY_CONFIG if not found in global settings.
    """
    # This uses the _ensure_scalar_float and type conversion logic from your updated settings.py structure
    # For simplicity in this example, we'll assume settings.py handles the type conversion.
    # If not, you'd replicate the robust get_strategy_config logic here.

    # A simplified version for this example, assuming settings.py provides correctly typed values
    # In a real scenario, you'd call a generic config loader from settings.py and then apply defaults
    # like in your previous robust get_strategy_config.
    
    # For this example, let's assume your settings.py has a master get_config()
    # and we are using the version of get_strategy_config that you provided in the prompt.
    # So, we need to make sure settings.py can load config for STRATEGY_NAME
    
    # Let's use a structure similar to your original `get_strategy_config` but for MPA
    global_config = settings.get_config() # This should be the fully processed config from settings.init()
    strategy_specific_config = global_config.get('strategies', {}).get(STRATEGY_NAME, {})
    
    # Merge with defaults, specific config takes precedence
    # This is a shallow merge. For deep merge, use a utility like in your settings.py.
    final_config = DEFAULT_STRATEGY_CONFIG.copy()
    final_config.update(strategy_specific_config)

    # --- Ensure correct types after loading from YAML/JSON if settings.py doesn't handle it for all strategies ---
    # Example: if your settings.py's get_config() doesn't run _ensure_scalar_float and int conversions yet
    # For this example, we assume the values from settings.get_config() are already appropriately typed
    # by the robust config loading mechanism (like the one we discussed for settings.py).
    # If not, you'd need to repeat the numeric_keys, integer_keys, boolean_keys processing here.
    # For instance:
    numeric_keys = [
        'min_avg_daily_turnover_amount', 'avg_turnover_days', 'min_listed_days',
        'min_daily_turnover_rate', 'max_daily_turnover_rate', 'ma_short_period',
        'ma_medium_period', 'ma_long_period', 'pullback_to_ma_period',
        'rsi_period_pullback', 'rsi_pullback_lower_threshold', 'rsi_pullback_upper_threshold',
        'macd_fast_period', 'macd_slow_period', 'macd_signal_period', 'atr_period',
        'atr_multiplier_stop_loss', 'atr_stop_loss_key_ma_period',
        'exit_ma_short_cross_medium_period', 'profit_target_pct'
    ]
    integer_keys = [
        'avg_turnover_days', 'min_listed_days', 'ma_short_period', 'ma_medium_period',
        'ma_long_period', 'pullback_to_ma_period', 'rsi_period_pullback',
        'macd_fast_period', 'macd_slow_period', 'macd_signal_period', 'atr_period',
        'atr_stop_loss_key_ma_period', 'exit_ma_short_cross_medium_period'
    ]

    for key in numeric_keys:
        if key in final_config and final_config[key] is not None: # Check for None for optional profit_target_pct
            try:
                # Simplified: assume it's mostly numeric or bool from YAML. Real conversion needed if strings.
                # This part heavily relies on how settings.get_config() prepares the data.
                # Using a placeholder for robust conversion.
                # final_config[key] = robust_converter_function(final_config[key])
                if isinstance(final_config[key], str) and final_config[key].endswith(','):
                    final_config[key] = final_config[key][:-1] # Basic comma removal for demo
                final_config[key] = float(final_config[key])
            except ValueError:
                logger.error(f"Config Error: MPA strategy key '{key}' has invalid value '{final_config[key]}'. Using default: {DEFAULT_STRATEGY_CONFIG.get(key)}")
                final_config[key] = DEFAULT_STRATEGY_CONFIG.get(key)


    for key in integer_keys:
        if key in final_config and final_config[key] is not None:
            try:
                # final_config[key] = int(robust_converter_function(final_config[key]))
                if isinstance(final_config[key], str) and final_config[key].endswith(','):
                    final_config[key] = final_config[key][:-1]
                final_config[key] = int(float(final_config[key])) # Convert to float first then int
            except ValueError:
                logger.error(f"Config Error: MPA strategy key '{key}' needs int, got '{final_config[key]}'. Using default: {DEFAULT_STRATEGY_CONFIG.get(key)}")
                final_config[key] = DEFAULT_STRATEGY_CONFIG.get(key)
    # Boolean conversion would also be needed here if loaded as strings.
    # ...
    return final_config


def calculate_indicators_mpa(data: pd.DataFrame, config):
    """Calculates all necessary technical indicators for the MPA strategy."""
    # Ensure DataFrame is a copy to avoid SettingWithCopyWarning if `data` is a slice
    data_copy = data.copy()
    data_copy['日期'] = pd.to_datetime(data_copy['日期'])
    data_copy = data_copy.sort_values(by='日期').reset_index(drop=True)

    # --- Data type conversion for TA-Lib ---
    # (Assuming this is handled by a more robust data loader or already done before this function)
    # For safety, let's include a basic conversion here
    cols_to_convert_numeric = ['收盘', '最高', '最低', '成交量', '成交额', '换手率', '涨跌幅', '开盘']
    for col in cols_to_convert_numeric:
        if col in data_copy.columns:
            if data_copy[col].dtype == 'object': # If strings with '%' etc.
                 data_copy[col] = data_copy[col].astype(str).str.replace('%', '', regex=False).str.strip()
            data_copy[col] = pd.to_numeric(data_copy[col], errors='coerce')
            # Basic ffill for price/change, 0 for volume/turnover if NaNs appear after to_numeric
            if col in ['收盘', '最高', '最低', '涨跌幅', '开盘']: data_copy[col] = data_copy[col].ffill().bfill()
            else: data_copy[col] = data_copy[col].fillna(0)
        else: # If column is missing, create it as NaN so talib doesn't fail hard, but logs will show issues
            data_copy[col] = np.nan
            logger.warning(f"Column '{col}' missing in data for MPA indicators. Will be NaN.")


    close = data_copy['收盘'].values.astype(np.float64)
    high = data_copy['最高'].values.astype(np.float64)
    low =  data_copy['最低'].values.astype(np.float64)
    # volume = data_copy['成交量'].values.astype(np.float64) # Not directly used in this strategy's indicators yet

    # Moving Averages
    data_copy[f'MA{config["ma_short_period"]}'] = talib.SMA(close, timeperiod=config['ma_short_period'])
    data_copy[f'MA{config["ma_medium_period"]}'] = talib.SMA(close, timeperiod=config['ma_medium_period'])
    data_copy[f'MA{config["ma_long_period"]}'] = talib.SMA(close, timeperiod=config['ma_long_period'])
    data_copy[f'MA_Pullback_Target'] = talib.SMA(close, timeperiod=config['pullback_to_ma_period'])
    data_copy[f'MA_ATR_Stop_Ref'] = talib.SMA(close, timeperiod=config['atr_stop_loss_key_ma_period'])


    # RSI
    data_copy['RSI'] = talib.RSI(close, timeperiod=config['rsi_period_pullback'])

    # MACD
    data_copy['MACD_DIF'], data_copy['MACD_DEA'], data_copy['MACD_HIST'] = talib.MACD(
        close, fastperiod=config['macd_fast_period'],
        slowperiod=config['macd_slow_period'], signalperiod=config['macd_signal_period']
    )

    # ATR
    data_copy['ATR'] = talib.ATR(high, low, close, timeperiod=config['atr_period'])
    
    return data_copy

def check_common_mpa_filters(stock_code_tuple, data_with_indicators, config, strategy_name_log_suffix=""):
    """Common preliminary checks for the MPA strategy."""
    code, name = stock_code_tuple
    
    if len(data_with_indicators) < 2: # Need current and previous day for many checks
        logger.debug(f"[{name}({code})][MPA {strategy_name_log_suffix}]: Data less than 2 days, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False, None, None 

    latest_data = data_with_indicators.iloc[-1]
    prev_data = data_with_indicators.iloc[-2]

    # Check for NaNs in essential fields for the latest_data
    essential_latest_fields = [
        '收盘', '开盘', '最高', '最低',
        f'MA{config["ma_short_period"]}', f'MA{config["ma_medium_period"]}', f'MA{config["ma_long_period"]}',
        'MA_Pullback_Target', 'RSI', 'MACD_DIF', 'MACD_DEA', 'ATR', f'MA_ATR_Stop_Ref'
    ]
    if latest_data[essential_latest_fields].isnull().any():
        nan_fields = latest_data[essential_latest_fields].index[latest_data[essential_latest_fields].isnull()].tolist()
        logger.debug(f"[{name}({code})][MPA {strategy_name_log_suffix}]: Latest data indicators NaN: {nan_fields}, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False, None, None
    # Also check prev_data for fields used in cross-checks or prev_value comparisons
    essential_prev_fields = [f'MA{config["ma_short_period"]}', f'MA{config["ma_medium_period"]}'] # Example
    if prev_data[essential_prev_fields].isnull().any():
        nan_fields_prev = prev_data[essential_prev_fields].index[prev_data[essential_prev_fields].isnull()].tolist()
        logger.debug(f"[{name}({code})][MPA {strategy_name_log_suffix}]: Prev data indicators NaN: {nan_fields_prev}, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False, latest_data, prev_data


    # Basic Filters
    if len(data_with_indicators) < config['min_listed_days']:
        logger.debug(f"[{name}({code})][MPA {strategy_name_log_suffix}]: Listed days {len(data_with_indicators)} < {config['min_listed_days']}, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False, latest_data, prev_data

    if len(data_with_indicators['成交额']) < config['avg_turnover_days']:
        logger.debug(f"[{name}({code})][MPA {strategy_name_log_suffix}]: Turnover data < {config['avg_turnover_days']} days, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False, latest_data, prev_data
        
    avg_turnover_series = data_with_indicators['成交额'].iloc[-config['avg_turnover_days']:]
    avg_daily_turnover = avg_turnover_series.mean()
    if pd.isna(avg_daily_turnover) or avg_daily_turnover < config['min_avg_daily_turnover_amount']:
        logger.debug(f"[{name}({code})][MPA {strategy_name_log_suffix}]: Avg turnover {avg_daily_turnover/1e8:.2f}亿 < {config['min_avg_daily_turnover_amount']/1e8:.2f}亿, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False, latest_data, prev_data

    if not (config['min_daily_turnover_rate'] <= latest_data['换手率'] <= config['max_daily_turnover_rate']):
        logger.debug(f"[{name}({code})][MPA {strategy_name_log_suffix}]: Turnover rate {latest_data['换手率']:.2f}% not in range, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False, latest_data, prev_data

    return True, latest_data, prev_data


def check_enter(stock_code_tuple, stock_data, end_date=None):
    code, name = stock_code_tuple
    config = get_strategy_config_mpa() # Get MPA specific config

    logger.debug(f"[{name}({code})]: Start check [MPA Enter]", extra={'stock': code, 'strategy': STRATEGY_NAME})

    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty:
        logger.warning(f"[{name}({code})][MPA Enter]: Empty/invalid data, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    
    # Initial data length check
    # Needs to be long enough for the longest MA and other indicators
    min_hist_len = max(config['ma_long_period'], config['rsi_period_pullback'], config['atr_period'], config['macd_slow_period'] + config['macd_signal_period']) + 10 # Buffer
    if len(stock_data) < min_hist_len:
        logger.debug(f"[{name}({code})][MPA Enter]: Initial data length {len(stock_data)} < {min_hist_len}, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # Process data (filter by end_date, calculate indicators)
    data_for_period = stock_data.copy()
    if end_date:
        end_date_dt = pd.to_datetime(end_date)
        data_for_period = data_for_period[pd.to_datetime(data_for_period['日期']) <= end_date_dt].copy()
    
    if len(data_for_period) < min_hist_len: # Check again after date filtering
        logger.debug(f"[{name}({code})][MPA Enter]: Data length after date filter {len(data_for_period)} < {min_hist_len}, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    data_with_indicators = calculate_indicators_mpa(data_for_period, config)
    
    pass_common, latest_data, prev_data = check_common_mpa_filters(stock_code_tuple, data_with_indicators, config, "Enter")
    if not pass_common:
        return False # Reason logged in common_filters

    # --- Trend Condition: MA Crossover (Multi-Timeframe Alignment) ---
    ma_short = latest_data[f'MA{config["ma_short_period"]}']
    ma_medium = latest_data[f'MA{config["ma_medium_period"]}']
    ma_long = latest_data[f'MA{config["ma_long_period"]}']

    is_uptrend = ma_short > ma_medium and ma_medium > ma_long
    # Optional: Check if MAs are sloping upwards
    # ma_medium_sloping_up = ma_medium > prev_data[f'MA{config["ma_medium_period"]}']
    # if not (is_uptrend and ma_medium_sloping_up):
    if not is_uptrend:
        logger.debug(f"[{name}({code})][MPA Enter]: Not in MA uptrend alignment. MA_S:{ma_short:.2f}, MA_M:{ma_medium:.2f}, MA_L:{ma_long:.2f}", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # --- Pullback Condition ---
    ma_pullback_target_val = latest_data['MA_Pullback_Target']
    # Price touched or briefly dipped below the target MA and closed above/near it.
    # Condition: Low of the day was at/below target MA, and close is above/near it.
    price_pulled_back_to_ma = (latest_data['最低'] <= ma_pullback_target_val * 1.005) and \
                              (latest_data['收盘'] >= ma_pullback_target_val * 0.995) # Allow small tolerance
    
    # RSI condition for pullback
    rsi_in_pullback_zone = config['rsi_pullback_lower_threshold'] <= latest_data['RSI'] <= config['rsi_pullback_upper_threshold']
    # Optional: RSI turning up: latest_data['RSI'] > prev_data['RSI']
    
    if not (price_pulled_back_to_ma and rsi_in_pullback_zone):
        logger.debug(f"[{name}({code})][MPA Enter]: Pullback condition not met. PricePullback:{price_pulled_back_to_ma}, RSIZone:{rsi_in_pullback_zone} (RSI:{latest_data['RSI']:.2f})", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # --- MACD Confirmation ---
    macd_strong = latest_data['MACD_DIF'] > 0 and latest_data['MACD_DIF'] > latest_data['MACD_DEA']
    if not macd_strong:
        logger.debug(f"[{name}({code})][MPA Enter]: MACD not strong. DIF:{latest_data['MACD_DIF']:.2f}, DEA:{latest_data['MACD_DEA']:.2f}", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    logger.info(f"[{name}({code})][MPA Enter]: ✨ 符合买入条件! MA Trend OK, Pullback to MA{config['pullback_to_ma_period']} OK, RSI OK, MACD OK.", extra={'stock': code, 'strategy': STRATEGY_NAME})
    return True


def check_exit_mpa(stock_code_tuple, stock_data, end_date=None, buy_price=None): # buy_price for profit target
    code, name = stock_code_tuple
    config = get_strategy_config_mpa()

    logger.debug(f"[{name}({code})]: Start check [MPA Exit]", extra={'stock': code, 'strategy': STRATEGY_NAME})

    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty:
        logger.warning(f"[{name}({code})][MPA Exit]: Empty/invalid data, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False # Cannot determine exit

    # Data processing and indicator calculation
    min_hist_len_exit = max(config['atr_period'], config['ma_medium_period']) + 5 # Min length for exit indicators
    
    data_for_period = stock_data.copy()
    if end_date:
        end_date_dt = pd.to_datetime(end_date)
        data_for_period = data_for_period[pd.to_datetime(data_for_period['日期']) <= end_date_dt].copy()

    if len(data_for_period) < min_hist_len_exit:
        logger.debug(f"[{name}({code})][MPA Exit]: Data length {len(data_for_period)} < {min_hist_len_exit} for exit, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    data_with_indicators = calculate_indicators_mpa(data_for_period, config)

    # Common filters check is not strictly needed for exit if we assume basic data validity
    # but getting latest_data and prev_data is useful.
    if len(data_with_indicators) < 2:
        logger.debug(f"[{name}({code})][MPA Exit]: Data less than 2 days after indicators, skip.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
        
    latest_data = data_with_indicators.iloc[-1]
    prev_data = data_with_indicators.iloc[-2] # For cross checks

    # Ensure ATR and relevant MA are not NaN for stop loss calc
    if pd.isna(latest_data['ATR']) or pd.isna(latest_data[f'MA_ATR_Stop_Ref']):
        logger.debug(f"[{name}({code})][MPA Exit]: ATR or MA_ATR_Stop_Ref is NaN, cannot calculate ATR stop loss, skip exit check.", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False # Cannot determine this exit path

    # --- Exit Condition 1: ATR Stop Loss ---
    # Stop loss level: MA_ATR_Stop_Ref - ATR_Multiplier * ATR
    atr_stop_level = latest_data[f'MA_ATR_Stop_Ref'] - (config['atr_multiplier_stop_loss'] * latest_data['ATR'])
    if latest_data['收盘'] < atr_stop_level:
        logger.info(f"[{name}({code})][MPA Exit]: 触发ATR止损! Close:{latest_data['收盘']:.2f} < StopLevel:{atr_stop_level:.2f} (MA_Ref:{latest_data[f'MA_ATR_Stop_Ref']:.2f}, ATR:{latest_data['ATR']:.2f})", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return True

    # --- Exit Condition 2: Trend Reversal (MA Short cross below MA Medium) ---
    ma_short_exit = latest_data[f'MA{config["exit_ma_short_cross_medium_period"]}']
    ma_medium_ref_exit = latest_data[f'MA{config["ma_medium_period"]}'] # Using the main medium MA as reference
    
    prev_ma_short_exit = prev_data[f'MA{config["exit_ma_short_cross_medium_period"]}']
    prev_ma_medium_ref_exit = prev_data[f'MA{config["ma_medium_period"]}']

    if pd.notna(prev_ma_short_exit) and pd.notna(prev_ma_medium_ref_exit): # Ensure prev values are not NaN
        if prev_ma_short_exit >= prev_ma_medium_ref_exit and ma_short_exit < ma_medium_ref_exit:
            logger.info(f"[{name}({code})][MPA Exit]: MA{config['exit_ma_short_cross_medium_period']} 下穿 MA{config['ma_medium_period']}，趋势反转卖出。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return True
            
    # --- Exit Condition 3: Profit Target (Optional) ---
    if config.get('profit_target_pct') is not None and buy_price is not None:
        if latest_data['收盘'] >= buy_price * (1 + config['profit_target_pct']):
            logger.info(f"[{name}({code})][MPA Exit]: 达到止盈目标! Buy:{buy_price:.2f}, Close:{latest_data['收盘']:.2f}", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return True

    logger.debug(f"[{name}({code})][MPA Exit]: 未触发卖出条件。", extra={'stock': code, 'strategy': STRATEGY_NAME})
    return False

# To integrate this into your system:
# 1. Add DEFAULT_STRATEGY_CONFIG to settings.py's default_config.
# 2. Ensure your settings.py's config loading can handle this new strategy and its parameters,
#    performing type conversions (_ensure_scalar_float, int conversion, bool conversion) correctly.
# 3. In your main application logic or backtester, you would call:
#    - `get_strategy_config_mpa()` to get the config.
#    - `calculate_indicators_mpa(data, config)` to get indicators.
#    - `check_enter_mpa(...)` for buy signals.
#    - `check_exit_mpa(...)` for sell signals (passing `buy_price` if using profit targets).