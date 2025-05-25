# newStrategy/limit_up.py
import pandas as pd
import logging
import settings # Import settings to get global config

logger = logging.getLogger(__name__)

STRATEGY_NAME = "涨停板次日溢价" # Define unique display name for this strategy

# Default config for this strategy
DEFAULT_STRATEGY_CONFIG = {
    'min_turnover_rate': 5.0, # Example parameter
    'max_turnover_rate': 25.0,
    'price_limit_up_threshold': 9.5, # Percentage for identifying a limit-up stock
    'buy_at_open_next_day': True, # Example: buy at next day's opening price
    'sell_at_close_next_day': True, # Example: sell at next day's closing price
    'profit_target': 0.05, # Example: 5% profit target
    'stop_loss': -0.03 # Example: -3% stop loss
}

def _ensure_scalar_float(value_to_convert):
    """
    Robustly converts a value to a scalar float.
    Handles strings (e.g., "10.5%"), single-element list-like objects (e.g., np.array([10.5])), 
    and numeric types.
    """
    if pd.isna(value_to_convert):
        raise ValueError("Cannot convert NaN or None to float for comparison.")
    
    if isinstance(value_to_convert, str):
        # Remove percentage sign and leading/trailing whitespace, then convert
        cleaned_value = value_to_convert.replace('%', '').strip()
        return float(cleaned_value)
    # Check if it's list-like (list, tuple, pandas Series, numpy array) but not string/bytes
    elif pd.api.types.is_list_like(value_to_convert) and not isinstance(value_to_convert, (str, bytes)):
        if len(value_to_convert) == 1:
            # If it's an iterable with one element, extract it and convert
            element = value_to_convert[0] if not isinstance(value_to_convert, pd.Series) else value_to_convert.iloc[0]
            # Recursively call to handle if the element itself needs cleaning (e.g. list containing "5%")
            return _ensure_scalar_float(element)
        else:
            raise ValueError(f"Expected a single value, but got a list-like object with {len(value_to_convert)} elements: {value_to_convert}")
    # For direct numbers (int, float, numpy scalars like np.float64)
    return float(value_to_convert)

def get_strategy_config():
    """
    Fetches strategy-specific configuration from settings.
    Falls back to DEFAULT_STRATEGY_CONFIG if not found in global settings.
    Ensures numeric config values are floats.
    """
    raw_config = settings.get_config().get('strategies', {}).get(STRATEGY_NAME, DEFAULT_STRATEGY_CONFIG)
    
    # Ensure expected numeric fields are float
    # This is important if settings might load them as strings or other types
    processed_config = raw_config.copy() # Work on a copy
    numeric_keys = ['min_turnover_rate', 'max_turnover_rate', 'price_limit_up_threshold', 
                    'profit_target', 'stop_loss']
    for key in numeric_keys:
        if key in processed_config:
            try:
                processed_config[key] = _ensure_scalar_float(processed_config[key])
            except (ValueError, TypeError) as e:
                logger.error(f"Error converting config key '{key}' with value '{processed_config[key]}' to float: {e}. Using default if available.")
                if key in DEFAULT_STRATEGY_CONFIG:
                     processed_config[key] = DEFAULT_STRATEGY_CONFIG[key] # Fallback to default's type
                else:
                    raise ValueError(f"Config value for '{key}' ('{processed_config[key]}') is invalid and no default available.")
    return processed_config


def check_enter(stock_code_tuple, stock_data, end_date=None):
    """
    Checks if a stock meets the entry conditions for the '涨停板次日溢价' strategy.
    """
    code, name = stock_code_tuple
    config = get_strategy_config()
    logger.debug(f"[{name}({code})]: 检查涨停板次日溢价策略入场条件。", extra={'stock': code, 'strategy': STRATEGY_NAME})
    print("stock_data", stock_data)

    if stock_data.empty or len(stock_data) < 2:
        logger.debug(f"[{name}({code})]: 数据不足两天，无法判断涨停板次日溢价。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # Ensure '日期' is datetime and data is sorted
    # It's good practice to clean data once, ideally when it's loaded.
    # For robustness, we ensure critical columns are usable here.
    local_stock_data = stock_data.copy() # Work on a copy
    local_stock_data['日期'] = pd.to_datetime(local_stock_data['日期'])
    local_stock_data = local_stock_data.sort_values(by='日期').reset_index(drop=True)
    
    # Columns that need to be numeric for checks
    # Ideally, this cleaning is done once when data is loaded.
    # For this function, we'll focus on values used in comparisons.
    # Example of broader cleaning (consider applying this earlier in your workflow):
    # for col_to_clean in ['涨跌幅', '换手率', '收盘', '开盘', '前收盘']:
    #     if col_to_clean in local_stock_data.columns:
    #         try:
    #             local_stock_data[col_to_clean] = local_stock_data[col_to_clean].apply(
    #                 lambda x: _ensure_scalar_float(x) if not pd.isna(x) else pd.NA
    #             )
    #             local_stock_data[col_to_clean] = pd.to_numeric(local_stock_data[col_to_clean], errors='coerce')
    #         except Exception as e:
    #             logger.warning(f"[{name}({code})]: Column {col_to_clean} cleaning failed: {e}")


    if end_date:
        end_date_ts = pd.to_datetime(end_date)
        data_period = local_stock_data[local_stock_data['日期'] <= end_date_ts].copy()
    else:
        data_period = local_stock_data.copy()

    if data_period.empty or len(data_period) < 2:
        logger.debug(f"[{name}({code})]: 过滤日期后数据不足两天，无法判断涨停板次日溢价。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    latest_data = data_period.iloc[-1]
    # prev_data = data_period.iloc[-2] # Not directly used if '涨跌幅' is present and used

    # --- Conditions for Limit-Up Next Day Premium ---
    if '涨跌幅' not in latest_data or pd.isna(latest_data['涨跌幅']):
        logger.warning(f"[{name}({code})]: 缺少'涨跌幅'列或数据无效，无法判断涨停。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
    
    try:
        latest_涨跌幅_val = _ensure_scalar_float(latest_data['涨跌幅'])
    except (ValueError, TypeError) as e:
        logger.warning(f"[{name}({code})]: '涨跌幅' ({latest_data['涨跌幅']}) 转换失败: {e}", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
        
    is_limit_up_today = latest_涨跌幅_val >= config['price_limit_up_threshold']

    if not is_limit_up_today:
        logger.debug(f"[{name}({code})]: 今日 ({latest_data['日期'].strftime('%Y-%m-%d')}) 涨幅 ({latest_涨跌幅_val:.2f}%) 未达涨停标准 ({config['price_limit_up_threshold']:.1f}%)。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    if '换手率' not in latest_data or pd.isna(latest_data['换手率']):
        logger.warning(f"[{name}({code})]: 缺少'换手率'列或数据无效。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
        
    try:
        latest_换手率_val = _ensure_scalar_float(latest_data['换手率'])
    except (ValueError, TypeError) as e:
        logger.warning(f"[{name}({code})]: '换手率' ({latest_data['换手率']}) 转换失败: {e}", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    if not (config['min_turnover_rate'] <= latest_换手率_val <= config['max_turnover_rate']):
        logger.debug(f"[{name}({code})]: 换手率 ({latest_换手率_val:.2f}%) 不在 {config['min_turnover_rate']:.2f}-{config['max_turnover_rate']:.2f}% 区间。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
        
    logger.info(f"[{name}({code})]: 符合涨停板次日溢价入场条件。", extra={'stock': code, 'strategy': STRATEGY_NAME})
    return True

def backtest(code_name_str, data, start_date, end_date):
    """
    Simulates trades for the '涨停板次日溢价' strategy based on historical data.
    """
    config = get_strategy_config()
    symbol = code_name_str.split()[0] 
    logger.info(f"开始回测 {code_name_str} 的涨停板次日溢价策略。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})
    
    # Work on a copy to avoid modifying original data
    local_data = data.copy()
    local_data['日期'] = pd.to_datetime(local_data['日期'])
    local_data = local_data.sort_values(by='日期').reset_index(drop=True)
    
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    backtest_data = local_data[(local_data['日期'] >= start_dt) & (local_data['日期'] <= end_dt)].copy()
    
    if backtest_data.empty:
        logger.warning(f"{code_name_str}: 回测日期范围 ({start_date}-{end_date}) 无数据。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})
        return {'总交易次数': 0, '胜率': 0, '平均收益率': 0, '盈利交易次数': 0, '亏损交易次数': 0, '总收益': 0}

    # Ensure necessary columns are numeric before calculations
    # It's crucial that '收盘', '开盘', '换手率' are clean numeric types.
    cols_to_ensure_numeric = ['收盘', '开盘', '换手率']
    if '前收盘' not in backtest_data.columns: # '前收盘' will be created if not present
        cols_to_ensure_numeric.append('收盘') # '前收盘' depends on '收盘'
    else:
        cols_to_ensure_numeric.append('前收盘')


    for col in cols_to_ensure_numeric:
        if col in backtest_data.columns:
            try:
                # Apply robust conversion to each element
                backtest_data[col] = backtest_data[col].apply(lambda x: _ensure_scalar_float(x) if not pd.isna(x) else pd.NA)
                backtest_data[col] = pd.to_numeric(backtest_data[col], errors='coerce')
            except Exception as e:
                logger.error(f"{code_name_str}: Error converting column '{col}' to numeric during backtest: {e}. Results may be inaccurate.", extra={'stock': symbol, 'strategy': STRATEGY_NAME})
                # Depending on severity, you might return or raise an error
                return {'总交易次数': 0, '胜率': 0, '平均收益率': 0, '盈利交易次数': 0, '亏损交易次数': 0, '总收益': 0, '错误': f"列 {col} 转换失败"}
        else:
             logger.warning(f"{code_name_str}: Expected column '{col}' not found in backtest data.", extra={'stock': symbol, 'strategy': STRATEGY_NAME})


    if '前收盘' not in backtest_data.columns:
        if '收盘' in backtest_data.columns and not backtest_data['收盘'].isnull().all():
            backtest_data['前收盘'] = backtest_data['收盘'].shift(1)
        else:
            logger.warning(f"{code_name_str}: '收盘'列缺失或全为NA，无法计算'前收盘'。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})
            return {'总交易次数': 0, '胜率': 0, '平均收益率': 0, '盈利交易次数': 0, '亏损交易次数': 0, '总收益': 0}
    
    backtest_data.dropna(subset=['前收盘', '收盘', '开盘', '换手率'], inplace=True) # Drop rows where essential data is missing after conversion

    if backtest_data.empty:
         logger.warning(f"{code_name_str}: 清洗后或必要数据不足，回测数据为空。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})
         return {'总交易次数': 0, '胜率': 0, '平均收益率': 0, '盈利交易次数': 0, '亏损交易次数': 0, '总收益': 0}

    backtest_data['DailyChangePct'] = (backtest_data['收盘'] / backtest_data['前收盘'] - 1) * 100
    
    trades = [] 

    for i in range(len(backtest_data) - 1): 
        current_day = backtest_data.iloc[i]
        next_day = backtest_data.iloc[i+1]
        
        try:
            current_day_换手率_val = _ensure_scalar_float(current_day['换手率']) # Already cleaned, but direct access might bypass .apply if not careful
            current_day_daily_change_val = _ensure_scalar_float(current_day['DailyChangePct'])
        except (ValueError, TypeError) as e:
            logger.debug(f"{code_name_str} {current_day['日期'].strftime('%Y-%m-%d')}: 数据转换失败 ({e})，跳过当天。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})
            continue

        is_limit_up_candidate = (current_day_daily_change_val >= config['price_limit_up_threshold'] and
                                 config['min_turnover_rate'] <= current_day_换手率_val <= config['max_turnover_rate'])

        if is_limit_up_candidate:
            try:
                buy_price = _ensure_scalar_float(next_day['开盘'])
                sell_price = _ensure_scalar_float(next_day['收盘'])
            except (ValueError, TypeError) as e:
                logger.debug(f"{code_name_str} {next_day['日期'].strftime('%Y-%m-%d')}: 买入或卖出价格转换失败 ({e})。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})
                continue

            if buy_price > 0: # Ensure valid buy price
                trade_return = (sell_price - buy_price) / buy_price
                
                if trade_return >= config['profit_target']:
                    trade_return = config['profit_target'] 
                elif trade_return <= config['stop_loss']:
                    trade_return = config['stop_loss'] 

                trades.append(trade_return)
                logger.debug(f"{code_name_str} {current_day['日期'].strftime('%Y-%m-%d')}: 触发涨停板策略。次日原始收益: {(sell_price - buy_price) / buy_price:.2%}, 调整后收益: {trade_return:.2%}", 
                             extra={'stock': symbol, 'strategy': STRATEGY_NAME})
            else:
                logger.debug(f"{code_name_str} {next_day['日期'].strftime('%Y-%m-%d')}: 买入价格 ({buy_price}) 无效。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})

    total_trades = len(trades)
    if total_trades == 0:
        return {'总交易次数': 0, '胜率': 0, '平均收益率': 0, '盈利交易次数': 0, '亏损交易次数': 0, '总收益': 0}

    profitable_trades = sum(1 for r in trades if r > 0)
    losing_trades = total_trades - profitable_trades
    
    average_return = sum(trades) / total_trades
    win_rate = profitable_trades / total_trades
    total_net_profit = sum(trades) 

    stats = {
        '总交易次数': total_trades,
        '胜率': win_rate,
        '平均收益率': average_return,
        '盈利交易次数': profitable_trades,
        '亏损交易次数': losing_trades,
        '总收益': total_net_profit 
    }
    logger.info(f"回测 {code_name_str} 结果: 总交易次数={total_trades}, 胜率={win_rate:.2%}, 平均收益={average_return:.2%}, 总收益={total_net_profit:.2%}",
                 extra={'stock': symbol, 'strategy': STRATEGY_NAME})
    return stats
