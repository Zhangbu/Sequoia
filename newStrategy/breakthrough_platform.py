# -*- encoding: UTF-8 -*-
import talib as tl
import pandas as pd
import logging
from strategy import enter

CONFIG = {
    'threshold_days': 60,
    'ma_period': 60,
    'price_range_min': -0.05,
    'price_range_max': 0.2
}

def validate_data(data, required_columns):
    """Validate that data is not None, not empty, and has required columns."""
    if data is None or data.empty:
        return False
    return all(col in data.columns for col in required_columns)

def check(code_name, data, end_date=None, threshold=CONFIG['threshold_days']):
    """
    Check for a platform breakout strategy.
    
    Args:
        code_name (tuple): Stock code and name.
        data (pd.DataFrame): Stock data with '日期', '收盘', '开盘' columns.
        end_date (str, optional): Filter data up to this date.
        threshold (int): Number of days to analyze.
    
    Returns:
        bool: True if breakout conditions are met, False otherwise.
    """
    # Validate inputs
    if not validate_data(data, ['日期', '收盘', '开盘']) or len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold} days or missing columns")
        return False

    data = data.copy()  # Avoid modifying input DataFrame
    try:
        data['ma60'] = tl.MA(data['收盘'].values, CONFIG['ma_period'])
    except Exception as e:
        logging.error(f"{code_name}: Error computing MA60: {e}")
        return False

    if data['ma60'].isna().any():
        logging.debug(f"{code_name}: MA60 contains NaN values")
        return False

    if end_date is not None:
        data = data[data['日期'] <= end_date]
    
    if len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data after date filter, less than {threshold} days")
        return False

    recent_data = data.tail(threshold)

    # Find breakout day
    breakout_condition = (recent_data['开盘'] < recent_data['ma60']) & (recent_data['ma60'] <= recent_data['收盘'])
    breakout_rows = recent_data[breakout_condition]
    
    breakthrough_row = None
    for _, row in breakout_rows.iterrows():
        try:
            if enter.check_volume(code_name, data, row['日期'], threshold):
                breakthrough_row = row
                break
        except Exception as e:
            logging.error(f"{code_name}: Error in check_volume: {e}")
            continue

    if breakthrough_row is None:
        logging.debug(f"{code_name}: No valid breakout day found")
        return False

    # Check price range before breakout
    data_front = recent_data[recent_data['日期'] < breakthrough_row['日期']]
    if data_front.empty:
        logging.debug(f"{code_name}: No data before breakout")
        return False

    price_diff_ratio = (data_front['ma60'] - data_front['收盘']) / data_front['ma60']
    if not ((CONFIG['price_range_min'] < price_diff_ratio) & (price_diff_ratio < CONFIG['price_range_max'])).all():
        logging.debug(f"{code_name}: Price range before breakout out of bounds")
        return False

    logging.info(f"{code_name}: Breakout detected on {breakthrough_row['日期']}")
    return True