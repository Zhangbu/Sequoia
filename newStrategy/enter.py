# -*- encoding: UTF-8 -*-
import talib as tl
import pandas as pd
import logging

CONFIG = {
    'breakthrough_price_surge': 1.06,
    'volume_turnover_min': 200000000,
    'volume_ratio_min': 2.0,
    'continuous_volume_ratio_min': 3.0
}

def validate_data(data, required_columns):
    if data is None or data.empty:
        return False
    return all(col in data.columns for col in required_columns)

def check_breakthrough(code_name, data, end_date=None, threshold=30):
    """Check if the last closing price breaks above the highest close in the prior threshold days."""
    if end_date is not None:
        data = data[data['日期'] <= end_date]
    
    if len(data) < threshold + 1:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold + 1} days")
        return False

    if not validate_data(data, ['日期', '收盘', '开盘']):
        logging.error(f"{code_name}: Missing required columns")
        return False

    recent_data = data.tail(threshold + 1)
    last_day = recent_data.iloc[-1]
    last_close = last_day['收盘']
    last_open = last_day['开盘']
    
    prior_data = recent_data.head(threshold)
    max_price = prior_data['收盘'].max()
    second_last_close = prior_data.iloc[-1]['收盘']

    return (last_close > max_price > second_last_close and 
            max_price > last_open and 
            last_close / last_open > CONFIG['breakthrough_price_surge'])

def check_ma(code_name, data, end_date=None, ma_days=250):
    """Check if the last closing price is above the N-day moving average."""
    if data is None or len(data) < ma_days:
        logging.debug(f"{code_name}: Insufficient data, less than {ma_days} days")
        return False

    if not validate_data(data, ['日期', '收盘']):
        logging.error(f"{code_name}: Missing required columns")
        return False

    data = data.copy()
    if end_date is not None:
        data = data[data['日期'] <= end_date]

    try:
        data['ma'] = tl.MA(data['收盘'].values, ma_days)
    except Exception as e:
        logging.error(f"{code_name}: Error computing MA: {e}")
        return False

    last_row = data.iloc[-1]
    return last_row['收盘'] > last_row['ma'] if pd.notna(last_row['ma']) else False

def check_new(code_name, data, end_date=None, threshold=60):
    """Check if the stock has been listed for fewer than threshold days."""
    if data is None or data.empty:
        logging.debug(f"{code_name}: No data available")
        return False

    if end_date is not None:
        data = data[data['日期'] <= end_date]
    
    return len(data) < threshold

def check_volume(code_name, data, end_date=None, threshold=60):
    """Check if the volume ratio is >= 2 with price increase and minimum turnover."""
    if data is None or len(data) < threshold:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold} days")
        return False

    required_columns = ['日期', '收盘', '开盘', '成交量', 'p_change']
    if not validate_data(data, required_columns):
        logging.error(f"{code_name}: Missing required columns")
        return False

    data = data.copy()
    try:
        data['vol_ma5'] = tl.MA(data['成交量'].values, 5)
    except Exception as e:
        logging.error(f"{code_name}: Error computing volume MA: {e}")
        return False
    
    if end_date is not None:
        data = data[data['日期'] <= end_date]
    
    if data.empty:
        return False

    recent_data = data.tail(threshold + 1)
    if len(recent_data) < threshold + 1:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold + 1} days")
        return False

    last_day = recent_data.iloc[-1]
    if last_day['p_change'] < 2 or last_day['收盘'] < last_day['开盘']:
        return False

    last_close = last_day['收盘']
    last_vol = last_day['成交量']
    mean_vol = recent_data.head(threshold).iloc[-1]['vol_ma5']
    
    turnover = last_close * last_vol * 100
    if turnover < CONFIG['volume_turnover_min']:
        return False

    vol_ratio = last_vol / mean_vol if mean_vol != 0 else 0
    if vol_ratio >= CONFIG['volume_ratio_min']:
        logging.debug(f"*{code_name}\nVolume Ratio: {vol_ratio:.2f}\tChange: {last_day['p_change']}%")
        return True
    return False

def check_continuous_volume(code_name, data, end_date=None, threshold=60, window_size=3):
    """Check if the volume ratio exceeds 3.0 for the last window_size days."""
    if data is None or len(data) < threshold + window_size:
        logging.debug(f"{code_name}: Insufficient data, less than {threshold + window_size} days")
        return False

    required_columns = ['日期', '收盘', '成交量']
    if not validate_data(data, required_columns):
        logging.error(f"{code_name}: Missing required columns")
        return False

    data = data.copy()
    try:
        data['vol_ma5'] = tl.MA(data['成交量'].values, 5)
    except Exception as e:
        logging.error(f"{code_name}: Error computing volume MA: {e}")
        return False
    
    if end_date is not None:
        data = data[data['日期'] <= end_date]
    
    recent_data = data.tail(threshold + window_size)
    if len(recent_data) < threshold + window_size:
        return False

    prior_data = recent_data.head(threshold)
    recent_days = recent_data.tail(window_size)
    
    mean_vol = prior_data.iloc[-1]['vol_ma5']
    if mean_vol == 0:
        return False

    vol_ratios = recent_days['成交量'] / mean_vol
    if (vol_ratios >= CONFIG['continuous_volume_ratio_min']).all():
        last_close = recent_days.iloc[-1]['收盘']
        last_vol = recent_days.iloc[-1]['成交量']
        logging.debug(f"*{code_name} Volume Ratio: {last_vol/mean_vol:.2f}\n\tClose: {last_close}")
        return True
    return False