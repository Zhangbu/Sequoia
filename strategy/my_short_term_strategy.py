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
    'check_limit_up': False, 
    'limit_up_threshold': 9.5, 
}

def _ensure_scalar_float(value_to_convert):
    """
    Robustly converts a value to a scalar float.
    Handles strings (e.g., "10.5%"), single-element list-like objects (e.g., np.array([10.5])), 
    and numeric types.
    """
    if pd.isna(value_to_convert):
        # Allow NaN to pass through for indicators that might be NaN,
        # but raise error if it's a config value that must be present.
        # For general conversion, returning NaN might be acceptable if handled later.
        # However, for config values, we usually expect them to be valid.
        # Let's assume for config, it should not be NaN.
        raise ValueError("Cannot convert NaN or None to float for a required configuration value.")
    
    if isinstance(value_to_convert, str):
        # Remove percentage sign and leading/trailing whitespace, then convert
        cleaned_value = value_to_convert.replace('%', '').strip()
        try:
            return float(cleaned_value)
        except ValueError as e:
            raise ValueError(f"Could not convert string '{value_to_convert}' (cleaned: '{cleaned_value}') to float: {e}")
    # Check if it's list-like (list, tuple, pandas Series, numpy array) but not string/bytes
    elif pd.api.types.is_list_like(value_to_convert) and not isinstance(value_to_convert, (str, bytes)):
        if len(value_to_convert) == 1:
            # If it's an iterable with one element, extract it and convert
            element = value_to_convert[0] if not isinstance(value_to_convert, pd.Series) else value_to_convert.iloc[0]
            # Recursively call to handle if the element itself needs cleaning (e.g. list containing "5%")
            return _ensure_scalar_float(element)
        else:
            raise ValueError(f"Expected a single value for conversion, but got a list-like object with {len(value_to_convert)} elements: {value_to_convert}")
    # For direct numbers (int, float, numpy scalars like np.float64)
    try:
        return float(value_to_convert)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Could not convert value '{value_to_convert}' of type {type(value_to_convert)} to float: {e}")

def get_strategy_config():
    """
    Fetches strategy-specific configuration from settings.
    Falls back to DEFAULT_STRATEGY_CONFIG if not found in global settings.
    Ensures numeric config values are floats.
    """
    raw_config = settings.get_config().get('strategies', {}).get(STRATEGY_NAME, DEFAULT_STRATEGY_CONFIG)
    
    processed_config = raw_config.copy() # Work on a copy
    
    # Define all keys that are expected to be numeric
    numeric_keys = [
        'min_avg_daily_turnover_amount', 'avg_turnover_days', 'min_listed_days',
        'ma5_cross_ma10_period', 'macd_gold_cross_within_days',
        'volume_ratio_to_5day_avg_min', 'volume_ratio_to_5day_avg_max',
        'volume_ratio_to_5day_avg_days', 'rsi_period', 'rsi_lower_limit',
        'rsi_upper_limit', 'kdj_j_upper_limit', 'kdj_j_lower_limit',
        'min_daily_turnover_rate', 'max_daily_turnover_rate', 'limit_up_threshold'
    ]
    
    for key in numeric_keys:
        if key in processed_config:
            try:
                processed_config[key] = _ensure_scalar_float(processed_config[key])
            except (ValueError, TypeError) as e:
                logger.error(f"Error converting config key '{key}' with value '{processed_config[key]}' to float: {e}. Using default if available.")
                if key in DEFAULT_STRATEGY_CONFIG:
                     # Ensure default is also processed if it wasn't already (e.g. if raw_config was empty)
                     processed_config[key] = _ensure_scalar_float(DEFAULT_STRATEGY_CONFIG[key])
                else:
                    # This case should ideally not happen if key is in numeric_keys derived from DEFAULT_STRATEGY_CONFIG
                    raise ValueError(f"Config value for '{key}' ('{processed_config[key]}') is invalid and no default available.")
        elif key in DEFAULT_STRATEGY_CONFIG: # Key missing in settings, but present in default
             processed_config[key] = _ensure_scalar_float(DEFAULT_STRATEGY_CONFIG[key])


    # Boolean keys don't need _ensure_scalar_float but ensure they are bool if loaded from settings
    boolean_keys = ['close_above_ma20', 'macd_dif_above_dea_and_zero', 'boll_break_middle_band',
                    'rsi_cross_30', 'kdj_gold_cross', 'check_limit_up']
    for key in boolean_keys:
        if key in processed_config and not isinstance(processed_config[key], bool):
            # Basic conversion for strings 'true'/'false' (case-insensitive)
            if isinstance(processed_config[key], str):
                if processed_config[key].lower() == 'true':
                    processed_config[key] = True
                elif processed_config[key].lower() == 'false':
                    processed_config[key] = False
                else:
                    logger.warning(f"Config key '{key}' has non-boolean string value '{processed_config[key]}'. Using default.")
                    processed_config[key] = DEFAULT_STRATEGY_CONFIG[key]
            else: # If not a recognizable string, fallback to default
                logger.warning(f"Config key '{key}' is not boolean ('{processed_config[key]}'). Using default.")
                processed_config[key] = DEFAULT_STRATEGY_CONFIG[key]
        elif key not in processed_config and key in DEFAULT_STRATEGY_CONFIG:
             processed_config[key] = DEFAULT_STRATEGY_CONFIG[key]


    return processed_config

def calculate_indicators(data: pd.DataFrame):
    """Calculates all necessary technical indicators for the strategy."""
    # Ensure DataFrame is a copy to avoid SettingWithCopyWarning if `data` is a slice
    data_copy = data.copy()
    data_copy['日期'] = pd.to_datetime(data_copy['日期'])
    data_copy = data_copy.sort_values(by='日期').reset_index(drop=True)

    # Crucial Fix: Explicitly convert columns to float64 for TA-Lib
    # Include '涨跌幅' here to ensure it's numeric and handled for NaNs
    cols_to_convert = ['收盘', '最高', '最低', '成交量', '成交额', '换手率', '涨跌幅', '开盘'] # Added '开盘' as it's often used
    
    for col in cols_to_convert:
        if col in data_copy.columns:
            # Attempt to clean if they are strings with '%'
            if data_copy[col].dtype == 'object': # Check if column is object type (often strings)
                 data_copy[col] = data_copy[col].astype(str).str.replace('%', '', regex=False).str.strip()

            data_copy[col] = pd.to_numeric(data_copy[col], errors='coerce')
            
            # Forward fill for price-related data and change percentages
            if col in ['收盘', '最高', '最低', '涨跌幅', '开盘']: 
                data_copy[col] = data_copy[col].ffill()
            # Fill with 0 for volume/turnover if appropriate, or consider ffill/bfill based on context
            elif col in ['成交量', '成交额', '换手率']:
                data_copy[col] = data_copy[col].fillna(0) # Or ffill() if more appropriate
        else:
            logger.warning(f"Column '{col}' not found in data for indicator calculation. It might be created as NaN.")
            data_copy[col] = np.nan # Create the column as NaN if it's missing

    # Ensure no NaN after ffill for critical TA-Lib inputs, replace with 0 or mean if necessary, or drop row
    # TA-Lib functions generally cannot handle NaNs in their direct input arrays.
    # A common strategy is to drop rows with NaNs in critical columns before passing to TA-Lib,
    # or ensure ffill/bfill covers all, or replace remaining NaNs with 0 or a mean.
    # For simplicity, we'll rely on ffill and fillna(0) above. If NaNs persist in 'close', 'high', 'low', 'volume',
    # TA-Lib might produce all NaN outputs for indicators.
    
    # Check for NaNs in critical columns before passing to TA-Lib
    critical_talib_inputs = ['收盘', '最高', '最低', '成交量']
    for col in critical_talib_inputs:
        if data_copy[col].isnull().any():
            logger.warning(f"NaNs found in TA-Lib critical input column '{col}' before indicator calculation. This may lead to all-NaN indicators.")
            # Option: data_copy.dropna(subset=critical_talib_inputs, inplace=True)
            # Option: data_copy[col] = data_copy[col].fillna(0) # Or a more sophisticated imputation

    # Ensure the dtypes are float64 for TA-Lib
    close = data_copy['收盘'].values.astype(np.float64)
    high = data_copy['最高'].values.astype(np.float64)
    low = data_copy['最低'].values.astype(np.float64)
    volume = data_copy['成交量'].values.astype(np.float64)

    config = get_strategy_config() # Get config for periods

    data_copy['MA5'] = talib.SMA(close, timeperiod=5)
    data_copy['MA10'] = talib.SMA(close, timeperiod=10)
    data_copy['MA20'] = talib.SMA(close, timeperiod=20)

    data_copy['MACD_DIF'], data_copy['MACD_DEA'], data_copy['MACD_HIST'] = talib.MACD(
        close, fastperiod=12, slowperiod=26, signalperiod=9
    )

    data_copy['KDJ_K'], data_copy['KDJ_D'] = talib.STOCH(
        high, low, close, fastk_period=9, slowk_period=3, slowd_period=3
    )
    data_copy['KDJ_J'] = 3 * data_copy['KDJ_K'] - 2 * data_copy['KDJ_D']

    data_copy['RSI'] = talib.RSI(close, timeperiod=config['rsi_period'])

    data_copy['BOLL_UPPER'], data_copy['BOLL_MIDDLE'], data_copy['BOLL_LOWER'] = talib.BBANDS(
        close, timeperiod=20, nbdevup=2, nbdevdn=2
    )

    data_copy['VOL_MA5'] = talib.SMA(volume, timeperiod=config['volume_ratio_to_5day_avg_days'])

    return data_copy


def check_enter(stock_code_tuple, stock_data, end_date=None):
    """
    Checks if a stock meets the entry conditions for the '东方财富短线策略'.
    """
    code, name = stock_code_tuple
    config = get_strategy_config() # Config values are now correctly typed

    logger.debug(f"[{name}({code})]: 开始检查东方财富App短线策略。", extra={'stock': code, 'strategy': STRATEGY_NAME})

    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty:
        logger.warning(f"[{name}({code})]: 策略收到空或非DataFrame数据，跳过。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    
    # Ensure a copy is used for any modifications within this function if necessary
    # However, calculate_indicators already returns a modified copy.
    # stock_data_processed = stock_data.copy() # If stock_data itself were to be modified

    required_cols_for_strategy = {'日期', '收盘', '开盘', '最高', '最低', '成交量', '换手率', '成交额', '涨跌幅'}
    if not required_cols_for_strategy.issubset(stock_data.columns):
        missing_cols = required_cols_for_strategy - set(stock_data.columns)
        logger.warning(f"[{name}({code})]: 数据缺少策略所需关键列: {missing_cols}，跳过。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # Sort and filter by date (calculate_indicators will also sort, but good to be explicit here for date filtering)
    # Make a copy before sorting and filtering to avoid modifying the original DataFrame passed to the function
    data_for_period = stock_data.copy()
    data_for_period['日期'] = pd.to_datetime(data_for_period['日期'])
    data_for_period = data_for_period.sort_values(by='日期').reset_index(drop=True)


    if end_date:
        end_date_dt = pd.to_datetime(end_date)
        # Ensure we use the already sorted and copied data_for_period
        data_for_period = data_for_period[data_for_period['日期'] <= end_date_dt].copy() # Another copy for slicing
    # else: data_for_period is already the full sorted copy


    # Determine minimum length needed for indicators
    # These periods should come from the already processed config
    max_ma_period = max(5, 10, 20) # Hardcoded for MA5, MA10, MA20
    max_macd_period = 26 + 9 # Standard MACD
    max_stoch_period = 9 + 3 + 3 # Standard KDJ
    max_rsi_period = config['rsi_period']
    max_boll_period = 20 # Standard Bollinger
    max_vol_ma_period = config['volume_ratio_to_5day_avg_days']
    max_turnover_days = config['avg_turnover_days']
    
    min_required_len = max(
        max_ma_period, max_macd_period, max_stoch_period, max_rsi_period, 
        max_boll_period, max_vol_ma_period, max_turnover_days,
        config['min_listed_days'] 
    ) + 5 # Add a small buffer

    if len(data_for_period) < min_required_len:
        logger.debug(f"[{name}({code})]: 过滤后数据长度 ({len(data_for_period)}) 不足 {min_required_len} 天，无法计算所有指标，跳过。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # Calculate indicators on the potentially date-filtered data
    data_with_indicators = calculate_indicators(data_for_period) 

    # Check if enough data remains after indicator calculation (TA-Lib might shorten it if NaNs at start)
    # Or if latest rows have NaNs in critical indicators
    if len(data_with_indicators) < 2: # Need at least current and previous day for some checks
        logger.debug(f"[{name}({code})]: 计算指标后数据不足两天，无法进行分析，跳过。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # Check for NaNs in critical indicator values for the latest point
    # This check is more robust if done on specific indicators needed for the decision.
    # Example: if data_with_indicators[['MA5', 'MA10', 'MACD_DIF']].iloc[-1].isnull().any(): logger.debug("..."); return False

    latest_data = data_with_indicators.iloc[-1]
    prev_data = data_with_indicators.iloc[-2] # Used for cross checks

    # --- Check for NaN in essential fields for the latest_data ---
    # Define which columns in latest_data are absolutely essential for the upcoming checks
    essential_latest_fields = ['涨跌幅', '收盘', 'MA20', 'MACD_DIF', 'MACD_DEA', '成交量', 'VOL_MA5', 
                               'BOLL_MIDDLE', 'RSI', 'KDJ_K', 'KDJ_D', 'KDJ_J', '换手率']
    if latest_data[essential_latest_fields].isnull().any():
        nan_fields = latest_data[essential_latest_fields].index[latest_data[essential_latest_fields].isnull()].tolist()
        logger.debug(f"[{name}({code})]: 最新数据点 ({latest_data['日期'].strftime('%Y-%m-%d')}) 的关键指标存在NaN: {nan_fields}，跳过。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False


    # --- NEW: Check for Limit Up (涨停) condition ---
    # 'limit_up_threshold' from config is now a float.
    # '涨跌幅' from latest_data should be a float due to calculate_indicators.
    if config['check_limit_up']:
        # pd.isna check is still good practice, though calculate_indicators should have handled it.
        if pd.isna(latest_data['涨跌幅']): # This check might be redundant if essential_latest_fields covers it
            logger.debug(f"[{name}({code})]: '涨跌幅'数据为NaN，无法判断涨停。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return False

        # The comparison that caused the error. Now both should be floats.
        if latest_data['涨跌幅'] < config['limit_up_threshold']:
            logger.debug(f"[{name}({code})]: 涨跌幅 ({latest_data['涨跌幅']:.2f}%) 未达到涨停阈值 ({config['limit_up_threshold']:.2f}%)。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return False

    # --- Basic Screening Conditions ---
    listed_days = len(data_with_indicators) # Use length of data after indicator calculation
    if listed_days < config['min_listed_days']: # This check might be redundant if min_required_len already covers it
        logger.debug(f"[{name}({code})]: 上市天数不足 {config['min_listed_days']} 天 ({listed_days}天)。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # Ensure there's enough data for avg_turnover_days before slicing
    if len(data_with_indicators['成交额']) < config['avg_turnover_days']:
        logger.debug(f"[{name}({code})]: 成交额数据不足 {config['avg_turnover_days']} 天，无法计算平均成交额。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    avg_daily_turnover_amount_series = data_with_indicators['成交额'].iloc[-config['avg_turnover_days']:]
    
    # This check was: avg_daily_turnover_amount_series.empty. A series from iloc will not be empty if len >= avg_turnover_days
    # Instead, check if the mean calculation is valid
    avg_daily_turnover_amount = avg_daily_turnover_amount_series.mean()

    if pd.isna(avg_daily_turnover_amount) or avg_daily_turnover_amount < config['min_avg_daily_turnover_amount']:
        logger.debug(f"[{name}({code})]: 日均成交额 ({avg_daily_turnover_amount/1_000_000_000:.2f}亿) 低于 {config['min_avg_daily_turnover_amount']/1_000_000_000:.2f}亿 (注意单位)。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # --- Technical Indicator Screening (Core Logic) ---
    ma5_cross_ma10 = False
    # Ensure lookback period does not exceed available data length minus one for comparison
    # The loop should go up to len(data_with_indicators) - 2 to access i and i+1 safely
    # And prev_data / latest_data are already data_with_indicators.iloc[-2] and data_with_indicators.iloc[-1]
    # So, for recent cross, we check prev_data and latest_data directly.
    # For a cross within N days, we need to iterate.
    
    # Check for MA5 cross MA10 within the lookback period ending on the latest day
    # MA5 > MA10 on latest_data, and MA5 <= MA10 on one of the prior (ma5_cross_ma10_period -1) days
    # A simpler check for "just crossed":
    # if prev_data['MA5'] <= prev_data['MA10'] and latest_data['MA5'] > latest_data['MA10']:
    #    ma5_cross_ma10 = True
    # For "crossed within N days":
    lookback_ma_cross = min(config['ma5_cross_ma10_period'], len(data_with_indicators) -1) # Ensure lookback is not too long
    for i in range(len(data_with_indicators) - lookback_ma_cross -1, len(data_with_indicators) - 1):
        # Check if data_with_indicators.iloc[i] and data_with_indicators.iloc[i+1] are valid
        # This loop structure is safer if we ensure i and i+1 are valid indices.
        # The loop range len(data) - N -1 to len(data) -1 means `i` goes from `len-N-1` to `len-2`.
        # `i+1` goes from `len-N` to `len-1`. This is correct.
        # Ensure MA5 and MA10 are not NaN for these points
        if pd.notna(data_with_indicators['MA5'].iloc[i]) and pd.notna(data_with_indicators['MA10'].iloc[i]) and \
           pd.notna(data_with_indicators['MA5'].iloc[i+1]) and pd.notna(data_with_indicators['MA10'].iloc[i+1]):
            if data_with_indicators['MA5'].iloc[i] <= data_with_indicators['MA10'].iloc[i] and \
               data_with_indicators['MA5'].iloc[i+1] > data_with_indicators['MA10'].iloc[i+1]:
                ma5_cross_ma10 = True
                break
    if not ma5_cross_ma10:
        logger.debug(f"[{name}({code})]: 近{config['ma5_cross_ma10_period']}天未发生5日均线上穿10日均线。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    if config['close_above_ma20'] and not (latest_data['收盘'] > latest_data['MA20']): # Assumes MA20 is not NaN due to essential_latest_fields check
        logger.debug(f"[{name}({code})]: 股价 ({latest_data['收盘']:.2f}) 未高于20日均线 ({latest_data['MA20']:.2f})。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    macd_gold_cross = False
    lookback_macd_cross = min(config['macd_gold_cross_within_days'], len(data_with_indicators) -1)
    for i in range(len(data_with_indicators) - lookback_macd_cross -1, len(data_with_indicators) - 1):
        if pd.notna(data_with_indicators['MACD_DIF'].iloc[i]) and pd.notna(data_with_indicators['MACD_DEA'].iloc[i]) and \
           pd.notna(data_with_indicators['MACD_DIF'].iloc[i+1]) and pd.notna(data_with_indicators['MACD_DEA'].iloc[i+1]):
            if data_with_indicators['MACD_DIF'].iloc[i] <= data_with_indicators['MACD_DEA'].iloc[i] and \
               data_with_indicators['MACD_DIF'].iloc[i+1] > data_with_indicators['MACD_DEA'].iloc[i+1]:
                macd_gold_cross = True
                break
    if not macd_gold_cross:
        logger.debug(f"[{name}({code})]: 近{config['macd_gold_cross_within_days']}天未发生MACD金叉。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    if config['macd_dif_above_dea_and_zero'] and not (latest_data['MACD_DIF'] > latest_data['MACD_DEA'] and latest_data['MACD_DIF'] > 0):
        logger.debug(f"[{name}({code})]: MACD不满足 DIF > DEA 且 DIF > 0。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False 
    
    if latest_data['VOL_MA5'] <= 0: # Already checked for NaN in essential_latest_fields
        logger.debug(f"[{name}({code})]: 5日均量为零或负，无法计算放量比。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    
    volume_ratio = latest_data['成交量'] / latest_data['VOL_MA5']
    if not (config['volume_ratio_to_5day_avg_min'] <= volume_ratio <= config['volume_ratio_to_5day_avg_max']):
        logger.debug(f"[{name}({code})]: 成交量 ({latest_data['成交量']:.0f}) 不满足放量条件 ({volume_ratio:.2f}倍5日均量)，要求 {config['volume_ratio_to_5day_avg_min']:.2f} - {config['volume_ratio_to_5day_avg_max']:.2f} 倍。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    
    # Ensure prev_data fields are not NaN for Boll cross check
    if config['boll_break_middle_band']:
        if pd.isna(prev_data['收盘']) or pd.isna(prev_data['BOLL_MIDDLE']):
            logger.debug(f"[{name}({code})]: 前一日收盘价或布林中轨为NaN，无法判断布林带上穿。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return False
        if not (prev_data['收盘'] <= prev_data['BOLL_MIDDLE'] and latest_data['收盘'] > latest_data['BOLL_MIDDLE']): # latest_data fields already checked for NaN
            logger.debug(f"[{name}({code})]: 未上穿布林带中轨。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return False

    if config['rsi_cross_30']:
        if pd.isna(prev_data['RSI']):
            logger.debug(f"[{name}({code})]: 前一日RSI为NaN，无法判断RSI上穿。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return False
        if not (prev_data['RSI'] <= config['rsi_lower_limit'] and latest_data['RSI'] > config['rsi_lower_limit']): # latest_data['RSI'] checked for NaN
            logger.debug(f"[{name}({code})]: RSI ({latest_data['RSI']:.2f}) 未上穿 {config['rsi_lower_limit']:.0f}。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return False
            
    if not (config['rsi_lower_limit'] <= latest_data['RSI'] <= config['rsi_upper_limit']): # latest_data['RSI'] checked for NaN
        logger.debug(f"[{name}({code})]: RSI ({latest_data['RSI']:.2f}) 不在 {config['rsi_lower_limit']:.0f}-{config['rsi_upper_limit']:.0f} 区间。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    if config['kdj_gold_cross']:
        if pd.isna(prev_data['KDJ_K']) or pd.isna(prev_data['KDJ_D']):
            logger.debug(f"[{name}({code})]: 前一日KDJ K或D值为NaN，无法判断KDJ金叉。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return False
        if not (prev_data['KDJ_K'] <= prev_data['KDJ_D'] and latest_data['KDJ_K'] > latest_data['KDJ_D']): # latest K, D checked for NaN
            logger.debug(f"[{name}({code})]: KDJ未金叉。", extra={'stock': code, 'strategy': STRATEGY_NAME})
            return False
            
    if not (config['kdj_j_lower_limit'] <= latest_data['KDJ_J'] < config['kdj_j_upper_limit']): # latest J checked for NaN
        logger.debug(f"[{name}({code})]: KDJ J值 ({latest_data['KDJ_J']:.2f}) 不在 {config['kdj_j_lower_limit']:.0f}-{config['kdj_j_upper_limit']:.0f} 区间。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    if not (config['min_daily_turnover_rate'] <= latest_data['换手率'] <= config['max_daily_turnover_rate']): # latest '换手率' checked for NaN
        logger.debug(f"[{name}({code})]: 换手率 ({latest_data['换手率']:.2f}%) 不在 {config['min_daily_turnover_rate']:.2f}-{config['max_daily_turnover_rate']:.2f}% 区间。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    logger.info(f"[{name}({code})]: ✨ 股票符合东方财富App短线策略所有入场条件！", extra={'stock': code, 'strategy': STRATEGY_NAME})
    return True
