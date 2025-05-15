# -*- encoding: UTF-8 -*-
import logging
import pandas as pd
from strategy import turtle_trade

CONFIG = {
    'limit_up_threshold': 9.5,
    'consolidation_ratio_min': 0.97,
    'consolidation_ratio_max': 1.03,
    'price_change_max': 5.0,
    'consolidation_days': 3
}

def validate_data(data, required_columns):
    """Validate that data is not None, not empty, and has required columns."""
    if data is None or data.empty:
        return False
    return all(col in data.columns for col in required_columns)

def check(code_name, data, end_date=None, threshold=15):
    """
    Check for a 'runway' pattern: limit-up day followed by consolidation.
    
    Args:
        code_name (tuple): Stock code and name.
        data (pd.DataFrame): Stock data with '日期', 'p_change', '收盘', '开盘' columns.
        end_date (str, optional): Filter data up to this date.
        threshold (int): Number of days to analyze for limit-up.
    
    Returns:
        bool: True if a limit-up day is followed by valid consolidation, False otherwise.
    """
    if not validate_data(data, ['日期', 'p_change', '收盘', '开盘']) or len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold} days or missing columns")
        return False

    data = data.copy()  # Avoid modifying input DataFrame
    if end_date is not None:
        data = data[data['日期'] <= end_date]
    
    if len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data after date filter, less than {threshold} days")
        return False

    recent_data = data.tail(threshold)
    
    for index, row in recent_data.iterrows():
        try:
            if row['p_change'] > CONFIG['limit_up_threshold']:
                if turtle_trade.check_enter(code_name, data, row['日期'], threshold):
                    if check_internal(code_name, data, row):
                        logging.debug(f"{code_name}: Limit-up detected on {row['日期']}")
                        return True
        except Exception as e:
            logging.error(f"{code_name}: Error processing limit-up check: {e}")
    
    return False

def check_internal(code_name, data, limitup_row):
    """
    Check if three days after a limit-up day meet consolidation criteria.
    
    Args:
        code_name (tuple): Stock code and name.
        data (pd.DataFrame): Stock data with '日期', 'p_change', '收盘', '开盘' columns.
        limitup_row (pd.Series): Row of the limit-up day.
    
    Returns:
        bool: True if consolidation criteria are met, False otherwise.
    """
    limitup_price = limitup_row['收盘']
    limitup_date = limitup_row['日期']
    
    # Get the next 3 days after limit-up
    consolidation_data = data[data['日期'] > limitup_date].head(CONFIG['consolidation_days'])
    if len(consolidation_data) < CONFIG['consolidation_days']:
        logging.debug(f"{code_name}: Insufficient consolidation days after {limitup_date}")
        return False

    # Day 1 checks
    day1 = consolidation_data.iloc[0]
    ratio_day1 = day1['收盘'] / day1['开盘']
    if not (day1['收盘'] > limitup_price and 
            day1['开盘'] > limitup_price and
            CONFIG['consolidation_ratio_min'] < ratio_day1 < CONFIG['consolidation_ratio_max']):
        return False

    # Days 2-3 checks
    days23 = consolidation_data.iloc[1:]
    ratios = days23['收盘'] / days23['开盘']
    valid_days23 = (
        (CONFIG['consolidation_ratio_min'] < ratios) & 
        (ratios < CONFIG['consolidation_ratio_max']) &
        (days23['p_change'].abs() < CONFIG['price_change_max']) &
        (days23['收盘'] > limitup_price) &
        (days23['开盘'] > limitup_price)
    )
    
    if not valid_days23.all():
        return False
    
    return True