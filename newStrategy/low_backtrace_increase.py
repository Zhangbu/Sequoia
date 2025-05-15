# -*- encoding: UTF-8 -*-
import pandas as pd
import logging

CONFIG = {
    'threshold_days': 60,
    'min_price_increase': 0.6,
    'max_single_day_drop': -7.0,
    'max_two_day_drop': -10.0
}

def validate_data(data, required_columns):
    """Validate that data is not None, not empty, and has required columns."""
    if data is None or data.empty:
        return False
    return all(col in data.columns for col in required_columns)

def check(code_name, data, end_date=None, threshold=CONFIG['threshold_days']):
    """
    Check for a low drawdown steady rise strategy.
    
    Args:
        code_name (tuple): Stock code and name.
        data (pd.DataFrame): Stock data with '日期', '收盘', '开盘', 'p_change' columns.
        end_date (str, optional): Filter data up to this date.
        threshold (int): Number of days to analyze.
    
    Returns:
        bool: True if price rises >=60% without significant drawdowns, False otherwise.
    """
    if not validate_data(data, ['日期', '收盘', '开盘', 'p_change']):
        logging.debug(f"{code_name}: Missing required columns or empty data")
        return False

    data = data.copy()  # Avoid modifying input DataFrame
    if end_date is not None:
        data = data[data['日期'] <= end_date]
    
    if len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold} days")
        return False

    recent_data = data.tail(threshold)
    
    # Check price increase
    first_close = recent_data.iloc[0]['收盘']
    last_close = recent_data.iloc[-1]['收盘']
    if first_close == 0:
        logging.debug(f"{code_name}: Zero opening price")
        return False
    
    ratio_increase = (last_close - first_close) / first_close
    if ratio_increase < CONFIG['min_price_increase']:
        logging.debug(f"{code_name}: Price increase {ratio_increase:.2f} < {CONFIG['min_price_increase']}")
        return False

    # Check drawdown conditions
    for i in range(1, len(recent_data)):
        curr_row = recent_data.iloc[i]
        prev_row = recent_data.iloc[i - 1]
        
        # Single-day price change
        single_day_change = prev_row['p_change']
        if single_day_change < CONFIG['max_single_day_drop']:
            logging.debug(f"{code_name}: Single-day drop {single_day_change:.2f}% < {CONFIG['max_single_day_drop']}%")
            return False
        
        # Single-day high-to-low (close - open) / open
        if curr_row['开盘'] != 0:
            high_to_low = (curr_row['收盘'] - curr_row['开盘']) / curr_row['开盘'] * 100
            if high_to_low < CONFIG['max_single_day_drop']:
                logging.debug(f"{code_name}: High-to-low drop {high_to_low:.2f}% < {CONFIG['max_single_day_drop']}%")
                return False
        else:
            logging.debug(f"{code_name}: Zero opening price on day {i}")
            return False
        
        # Two-day cumulative price change
        two_day_change = single_day_change + curr_row['p_change']
        if two_day_change < CONFIG['max_two_day_drop']:
            logging.debug(f"{code_name}: Two-day drop {two_day_change:.2f}% < {CONFIG['max_two_day_drop']}%")
            return False
        
        # Two-day high-to-low (current close - previous open) / previous open
        if prev_row['开盘'] != 0:
            two_day_high_to_low = (curr_row['收盘'] - prev_row['开盘']) / prev_row['开盘'] * 100
            if two_day_high_to_low < CONFIG['max_two_day_drop']:
                logging.debug(f"{code_name}: Two-day high-to-low {two_day_high_to_low:.2f}% < {CONFIG['max_two_day_drop']}%")
                return False
        else:
            logging.debug(f"{code_name}: Zero opening price on day {i-1}")
            return False

    logging.info(f"{code_name}: Valid low drawdown rise, increase={ratio_increase:.2f}")
    return True