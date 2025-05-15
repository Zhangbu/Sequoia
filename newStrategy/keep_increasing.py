# -*- encoding: UTF-8 -*-
import talib as tl
import pandas as pd
import logging

CONFIG = {
    'ma_period': 30,
    'ma_growth_factor': 1.2
}

def validate_data(data, required_columns):
    """Validate that data is not None, not empty, and has required columns."""
    if data is None or data.empty:
        return False
    return all(col in data.columns for col in required_columns)

def check(code_name, data, end_date=None, threshold=30):
    """
    Check if the 30-day moving average is consistently rising over threshold days.
    
    Args:
        code_name (tuple): Stock code and name.
        data (pd.DataFrame): Stock data with '日期' and '收盘' columns.
        end_date (str, optional): Filter data up to this date.
        threshold (int): Number of days to analyze.
    
    Returns:
        bool: True if MA30 is rising at key points and final MA30 > 1.2 * initial MA30, False otherwise.
    """
    if not validate_data(data, ['日期', '收盘']) or len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold} days or missing columns")
        return False

    data = data.copy()  # Avoid modifying input DataFrame
    if end_date is not None:
        data = data[data['日期'] <= end_date]
    
    if len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data after date filter, less than {threshold} days")
        return False

    # recent_data = data.tail(threshold)
    # With this:
    recent_data = data.tail(threshold).copy()
    
    try:
        recent_data['ma30'] = tl.MA(recent_data['收盘'].values, CONFIG['ma_period'])
    except Exception as e:
        logging.error(f"{code_name}: Error computing MA30: {e}")
        return False

    if recent_data['ma30'].isna().any():
        logging.debug(f"{code_name}: MA30 contains NaN values")
        return False

    step1 = round(threshold / 3)
    step2 = round(threshold * 2 / 3)
    
    ma_values = recent_data['ma30'].iloc[[0, step1, step2, -1]]
    is_increasing = (ma_values[0] < ma_values[step1] < ma_values[step2] < ma_values[-1])
    is_significant_rise = ma_values[-1] > CONFIG['ma_growth_factor'] * ma_values[0]

    if is_increasing and is_significant_rise:
        logging.debug(f"{code_name}: MA30 rising, final/initial ratio: {ma_values[-1]/ma_values[0]:.2f}")
        return True
    return False