# -*- encoding: UTF-8 -*-
import pandas as pd
import logging
import settings

CONFIG = {
    'initial_threshold_days': 60,
    'analysis_window_days': 14,
    'min_price_ratio': 1.9,
    'min_p_change': 9.5
}

def validate_data(data, required_columns):
    """Validate that data is not None, not empty, and has required columns."""
    if data is None or data.empty:
        return False
    return all(col in data.columns for col in required_columns)

def check(code_name, data, end_date=None, threshold=CONFIG['initial_threshold_days']):
    """
    Check for a high and narrow flag pattern.
    
    Args:
        code_name (tuple): Stock code and name.
        data (pd.DataFrame): Stock data with '日期', '最低', '最高', 'p_change' columns.
        end_date (str, optional): Filter data up to this date.
        threshold (int): Minimum number of days required initially.
    
    Returns:
        bool: True if flag pattern conditions are met, False otherwise.
    """
    # Validate inputs
    if not validate_data(data, ['日期', '最低', '最高', 'p_change']):
        logging.debug(f"{code_name}: Missing required columns or empty data")
        return False

    try:
        if not hasattr(settings, 'top_list') or code_name[0] not in settings.top_list:
            logging.debug(f"{code_name}: Not in institutional top list")
            return False
    except Exception as e:
        logging.error(f"{code_name}: Error accessing top_list: {e}")
        return False

    data = data.copy()  # Avoid modifying input DataFrame
    if end_date is not None:
        data = data[data['日期'] <= end_date]
    
    if len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold} days")
        return False

    # Focus on the last 14 days
    recent_data = data.tail(CONFIG['analysis_window_days'])
    if len(recent_data) < CONFIG['analysis_window_days']:
        logging.debug(f"{code_name}: Insufficient data, less than {CONFIG['analysis_window_days']} days")
        return False

    # Check price ratio
    low = recent_data['最低'].min()
    high = recent_data.iloc[-1]['最高']
    if low == 0:
        logging.debug(f"{code_name}: Zero minimum price")
        return False
    
    ratio_increase = high / low
    if ratio_increase < CONFIG['min_price_ratio']:
        logging.debug(f"{code_name}: Price ratio {ratio_increase:.2f} < {CONFIG['min_price_ratio']}")
        return False

    # Check for consecutive days with p_change >= 9.5%
    p_changes = recent_data['p_change']
    for i in range(1, len(p_changes)):
        if p_changes.iloc[i] >= CONFIG['min_p_change'] and p_changes.iloc[i-1] >= CONFIG['min_p_change']:
            logging.info(f"{code_name}: Flag pattern detected, ratio={ratio_increase:.2f}")
            return True

    logging.debug(f"{code_name}: No consecutive days with p_change >= {CONFIG['min_p_change']}%")
    return False