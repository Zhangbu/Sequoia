# -*- encoding: UTF-8 -*-
import talib as tl
import pandas as pd
import logging

CONFIG = {
    'threshold_days': 60,
    'min_p_change': -9.5,
    'min_turnover': 200000000,
    'min_vol_ratio': 4.0,
    'vol_ma_period': 5
}

def validate_data(data, required_columns):
    """Validate that data is not None, not empty, and has required columns."""
    if data is None or data.empty:
        return False
    return all(col in data.columns for col in required_columns)

def check(code_name, data, end_date=None, threshold=CONFIG['threshold_days']):
    """
    Check for a high-volume limit-down strategy.
    
    Args:
        code_name (tuple): Stock code and name.
        data (pd.DataFrame): Stock data with '日期', '成交量', 'p_change', '收盘' columns.
        end_date (str, optional): Filter data up to this date.
        threshold (int): Number of days for volume MA calculation.
    
    Returns:
        bool: True if limit-down with high volume is detected, False otherwise.
    """
    # Validate inputs
    if not validate_data(data, ['日期', '成交量', 'p_change', '收盘']):
        logging.debug(f"{code_name}: Missing required columns or empty data")
        return False

    if len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold} days")
        return False

    data = data.copy()  # Avoid modifying input DataFrame
    if end_date is not None:
        data = data[data['日期'] <= end_date]
    
    if len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data after date filter, less than {threshold} days")
        return False

    # Check last day's price change
    last_row = data.iloc[-1]
    if last_row['p_change'] > CONFIG['min_p_change']:
        logging.debug(f"{code_name}: Price change {last_row['p_change']:.2f}% > {CONFIG['min_p_change']}%")
        return False

    # Get last threshold+1 days
    recent_data = data.tail(threshold + 1)
    if len(recent_data) < threshold + 1:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold + 1} days")
        return False

    # Compute volume MA for prior threshold days
    prior_data = recent_data.head(threshold)
    try:
        vol_ma5 = tl.MA(prior_data['成交量'].values, CONFIG['vol_ma_period'])
        mean_vol = vol_ma5[-1] if vol_ma5 is not None and len(vol_ma5) > 0 else 0
    except Exception as e:
        logging.error(f"{code_name}: Error computing volume MA: {e}")
        return False

    if mean_vol == 0:
        logging.debug(f"{code_name}: Zero volume MA")
        return False

    # Check turnover
    last_close = last_row['收盘']
    last_vol = last_row['成交量']
    turnover = last_close * last_vol * 100
    if turnover < CONFIG['min_turnover']:
        logging.debug(f"{code_name}: Turnover {turnover:.2e} < {CONFIG['min_turnover']}")
        return False

    # Check volume ratio
    vol_ratio = last_vol / mean_vol
    if vol_ratio >= CONFIG['min_vol_ratio']:
        logging.info(f"*{code_name}\nVolume Ratio: {vol_ratio:.2f}\tDrop: {last_row['p_change']:.2f}%")
        return True
    
    logging.debug(f"{code_name}: Volume ratio {vol_ratio:.2f} < {CONFIG['min_vol_ratio']}")
    return False