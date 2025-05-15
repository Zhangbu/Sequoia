# -*- encoding: UTF-8 -*-
import talib as tl
import pandas as pd
import logging
from datetime import datetime

CONFIG = {
    'ma_period': 250,
    'threshold_days': 60,
    'volume_ratio_min': 2.0,
    'price_ratio_max': 0.8,
    'days_min': 10,
    'days_max': 50
}

def validate_data(data, required_columns):
    """Validate that data is not None, not empty, and has required columns."""
    if data is None or data.empty:
        return False
    return all(col in data.columns for col in required_columns)

def check(code_name, data, end_date=None, threshold=CONFIG['threshold_days']):
    """
    Check for a pullback to the 250-day moving average strategy.
    
    Args:
        code_name (tuple): Stock code and name.
        data (pd.DataFrame): Stock data with '日期', '收盘', '成交量' columns.
        end_date (str, optional): Filter data up to this date.
        threshold (int): Number of days to analyze.
    
    Returns:
        bool: True if pullback conditions are met, False otherwise.
    """
    if not validate_data(data, ['日期', '收盘', '成交量']) or len(data) < CONFIG['ma_period']:
        logging.debug(f"{code_name}: Insufficient data, less than {CONFIG['ma_period']} days or missing columns")
        return False

    data = data.copy()  # Avoid modifying input DataFrame
    try:
        data['ma250'] = tl.MA(data['收盘'].values, CONFIG['ma_period'])
    except Exception as e:
        logging.error(f"{code_name}: Error computing MA250: {e}")
        return False

    if data['ma250'].isna().any():
        logging.debug(f"{code_name}: MA250 contains NaN values")
        return False

    begin_date = data.iloc[0]['日期']
    if end_date is not None:
        try:
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
            if end_date_dt.date() < begin_date:
                logging.debug(f"{code_name}: Not listed on {end_date}")
                return False
            data = data[data['日期'] <= end_date]
        except ValueError as e:
            logging.error(f"{code_name}: Invalid end_date format: {e}")
            return False

    if len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data after date filter, less than {threshold} days")
        return False

    recent_data = data.tail(threshold)
    last_close = recent_data.iloc[-1]['收盘']

    # Find highest and lowest closing prices
    highest_idx = recent_data['收盘'].idxmax()
    lowest_idx = recent_data['收盘'].idxmin()
    highest_row = recent_data.loc[highest_idx]
    lowest_row = recent_data.loc[lowest_idx]

    if lowest_row['成交量'] == 0 or highest_row['成交量'] == 0:
        logging.debug(f"{code_name}: Zero volume at peak or low")
        return False

    # Split data at peak
    data_front = recent_data[recent_data['日期'] < highest_row['日期']]
    data_end = recent_data[recent_data['日期'] >= highest_row['日期']]

    if data_front.empty:
        logging.debug(f"{code_name}: No data before peak")
        return False

    # Check breakout above MA250 in data_front
    if not (data_front.iloc[0]['收盘'] < data_front.iloc[0]['ma250'] and
            data_front.iloc[-1]['收盘'] > data_front.iloc[-1]['ma250']):
        logging.debug(f"{code_name}: No valid MA250 breakout")
        return False

    if not data_end.empty:
        # Check data_end stays above MA250
        if (data_end['收盘'] < data_end['ma250']).any():
            logging.debug(f"{code_name}: Price fell below MA250 after peak")
            return False
        
        # Find recent lowest close in data_end
        recent_lowest_row = data_end.loc[data_end['收盘'].idxmin()]
        
        # Check time difference between peak and recent low
        try:
            date_diff = (datetime.strptime(recent_lowest_row['日期'], '%Y-%m-%d').date() -
                        datetime.strptime(highest_row['日期'], '%Y-%m-%d').date()).days
            if not (CONFIG['days_min'] <= date_diff <= CONFIG['days_max']):
                logging.debug(f"{code_name}: Time between peak and low ({date_diff} days) out of range")
                return False
        except ValueError as e:
            logging.error(f"{code_name}: Date parsing error: {e}")
            return False

        # Check volume and price ratios
        vol_ratio = highest_row['成交量'] / recent_lowest_row['成交量']
        price_ratio = recent_lowest_row['收盘'] / highest_row['收盘']
        if not (vol_ratio > CONFIG['volume_ratio_min'] and price_ratio < CONFIG['price_ratio_max']):
            logging.debug(f"{code_name}: Volume ratio ({vol_ratio:.2f}) or price ratio ({price_ratio:.2f}) invalid")
            return False

    logging.debug(f"{code_name}: Valid pullback detected, vol_ratio={vol_ratio:.2f}, price_ratio={price_ratio:.2f}")
    return True