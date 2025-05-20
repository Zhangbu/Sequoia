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

def get_strategy_config():
    """
    Fetches strategy-specific configuration from settings.
    Falls back to DEFAULT_STRATEGY_CONFIG if not found in global settings.
    """
    return settings.get_config().get('strategies', {}).get(STRATEGY_NAME, DEFAULT_STRATEGY_CONFIG)


def check_enter(stock_code_tuple, stock_data, end_date=None):
    """
    Checks if a stock meets the entry conditions for the '涨停板次日溢价' strategy.
    This function should define what makes a stock a candidate for the limit-up next-day premium.
    """
    code, name = stock_code_tuple
    config = get_strategy_config()
    logger.debug(f"[{name}({code})]: 检查涨停板次日溢价策略入场条件。", extra={'stock': code, 'strategy': STRATEGY_NAME})

    if stock_data.empty or len(stock_data) < 2:
        logger.debug(f"[{name}({code})]: 数据不足两天，无法判断涨停板次日溢价。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # Ensure '日期' is datetime and data is sorted
    stock_data['日期'] = pd.to_datetime(stock_data['日期'])
    stock_data = stock_data.sort_values(by='日期').reset_index(drop=True)

    if end_date:
        end_date_ts = pd.to_datetime(end_date)
        data = stock_data[stock_data['日期'] <= end_date_ts].copy()
    else:
        data = stock_data.copy()

    if data.empty or len(data) < 2:
        logger.debug(f"[{name}({code})]: 过滤日期后数据不足两天，无法判断涨停板次日溢价。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    latest_data = data.iloc[-1]
    prev_data = data.iloc[-2]

    # --- Conditions for Limit-Up Next Day Premium ---
    # 1. Today was a Limit Up day (using a threshold like 9.5% increase)
    # To calculate daily change, you need '前收盘' (previous close) or use (close - open) / open
    # Assuming '涨跌幅' column is available and accurate
    if '涨跌幅' not in latest_data:
        logger.warning(f"[{name}({code})]: 缺少'涨跌幅'列，无法判断涨停。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
        
    is_limit_up_today = latest_data['涨跌幅'] >= config['price_limit_up_threshold']

    if not is_limit_up_today:
        logger.debug(f"[{name}({code})]: 今日 ({latest_data['日期'].strftime('%Y-%m-%d')}) 涨幅 ({latest_data['涨跌幅']:.2f}%) 未达涨停标准 ({config['price_limit_up_threshold']:.1f}%)。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False

    # 2. Turnover Rate within reasonable bounds
    if not (config['min_turnover_rate'] <= latest_data['换手率'] <= config['max_turnover_rate']):
        logger.debug(f"[{name}({code})]: 换手率 ({latest_data['换手率']:.2f}%) 不在 {config['min_turnover_rate']}-{config['max_turnover_rate']}% 区间。", extra={'stock': code, 'strategy': STRATEGY_NAME})
        return False
        
    # Additional checks could include:
    # - Stock price > 5 (already in work_flow_new1 initial filter, but good to double check)
    # - Not ST/PT stock (already in work_flow_new1 initial filter)
    # - Volume surge today (e.g., today's volume much higher than recent average)
    # - No obvious negative news (requires external data)

    logger.info(f"[{name}({code})]: 符合涨停板次日溢价入场条件。", extra={'stock': code, 'strategy': STRATEGY_NAME})
    return True

def backtest(code_name_str, data, start_date, end_date):
    """
    Simulates trades for the '涨停板次日溢价' strategy based on historical data.
    
    Args:
        code_name_str (str): Stock code and name (e.g., "000001 平安银行").
        data (pd.DataFrame): Historical data for the stock.
        start_date (str): Backtest start date in 'YYYYMMDD' format.
        end_date (str): Backtest end date in 'YYYYMMDD' format.
        
    Returns:
        dict: Backtest statistics (total trades, win rate, avg return, etc.).
    """
    config = get_strategy_config()
    symbol = code_name_str.split()[0] # Extract stock code for logging
    logger.info(f"开始回测 {code_name_str} 的涨停板次日溢价策略。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})
    
    # Ensure date column is datetime and sorted
    data['日期'] = pd.to_datetime(data['日期'])
    data = data.sort_values(by='日期').reset_index(drop=True)
    
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    # Filter data for the backtest period
    backtest_data = data[(data['日期'] >= start_dt) & (data['日期'] <= end_dt)].copy()
    
    if backtest_data.empty:
        logger.warning(f"{code_name_str}: 回测日期范围 ({start_date}-{end_date}) 无数据。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})
        return {
            '总交易次数': 0, '胜率': 0, '平均收益率': 0, 
            '盈利交易次数': 0, '亏损交易次数': 0, '总收益': 0
        }

    # Add '前收盘' (previous close) if not present, needed for daily change calculation
    if '前收盘' not in backtest_data.columns:
        backtest_data['前收盘'] = backtest_data['收盘'].shift(1)
        # Drop the first row if '前收盘' is NaN (as it won't have a previous day)
        backtest_data.dropna(subset=['前收盘'], inplace=True)
    
    if backtest_data.empty:
         logger.warning(f"{code_name_str}: 缺少足够数据用于回测，或清洗后数据为空。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})
         return {
            '总交易次数': 0, '胜率': 0, '平均收益率': 0, 
            '盈利交易次数': 0, '亏损交易次数': 0, '总收益': 0
        }

    # Calculate daily percentage change for identifying limit-up
    backtest_data['DailyChangePct'] = (backtest_data['收盘'] / backtest_data['前收盘'] - 1) * 100
    
    trades = [] # List to store trade returns

    # Iterate through data, looking for limit-up conditions for entry on the next day
    # We need at least two rows for current day and next day prices
    for i in range(len(backtest_data) - 1): 
        current_day = backtest_data.iloc[i]
        next_day = backtest_data.iloc[i+1]
        
        # Check if current day was a limit-up day AND meets turnover criteria
        is_limit_up_candidate = (current_day['DailyChangePct'] >= config['price_limit_up_threshold'] and
                                 config['min_turnover_rate'] <= current_day['换手率'] <= config['max_turnover_rate'])

        if is_limit_up_candidate:
            # Entry: Buy at next day's opening price (or adjusted logic)
            buy_price = next_day['开盘']
            
            # Exit: Sell at next day's closing price (or adjusted logic, e.g., profit target/stop loss)
            sell_price = next_day['收盘']
            
            if buy_price and sell_price and buy_price > 0: # Ensure valid prices
                trade_return = (sell_price - buy_price) / buy_price
                
                # Apply simple profit target/stop loss logic (optional)
                if trade_return >= config['profit_target']:
                    trade_return = config['profit_target'] # Cap profit at target
                elif trade_return <= config['stop_loss']:
                    trade_return = config['stop_loss'] # Cap loss at stop loss

                trades.append(trade_return)
                logger.debug(f"{code_name_str} {current_day['日期'].strftime('%Y-%m-%d')}: 触发涨停板策略。次日收益: {trade_return:.2%}", 
                             extra={'stock': symbol, 'strategy': STRATEGY_NAME})
            else:
                logger.debug(f"{code_name_str} {next_day['日期'].strftime('%Y-%m-%d')}: 买入或卖出价格无效。", extra={'stock': symbol, 'strategy': STRATEGY_NAME})

    total_trades = len(trades)
    if total_trades == 0:
        return {
            '总交易次数': 0, '胜率': 0, '平均收益率': 0, 
            '盈利交易次数': 0, '亏损交易次数': 0, '总收益': 0
        }

    profitable_trades = sum(1 for r in trades if r > 0)
    losing_trades = total_trades - profitable_trades
    
    average_return = sum(trades) / total_trades
    win_rate = profitable_trades / total_trades if total_trades > 0 else 0
    total_net_profit = sum(trades) # Sum of individual returns for total profit

    stats = {
        '总交易次数': total_trades,
        '胜率': win_rate,
        '平均收益率': average_return,
        '盈利交易次数': profitable_trades,
        '亏损交易次数': losing_trades,
        '总收益': total_net_profit # Add total net profit for overall summary
    }
    logger.info(f"回测 {code_name_str} 结果: 胜率={win_rate:.2%}, 平均收益={average_return:.2%}, 总收益={total_net_profit:.2%}",
                 extra={'stock': symbol, 'strategy': STRATEGY_NAME})
    return stats