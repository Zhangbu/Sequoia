# -*- encoding: UTF-8 -*-
import pandas as pd
import numpy as np
import talib as tl
import logging
from typing import Tuple, Optional, Dict, List
import akshare as ak
from retrying import retry

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 初始资金
INITIAL_BALANCE = 200_000

class MABullStrategy:
    """均线多头选股策略实现类"""
    
    def __init__(self, balance: float = INITIAL_BALANCE, threshold: int = 30, 
                 ma_periods: List[int] = [5, 10, 30], min_rise: float = 1.2, 
                 risk_per_trade: float = 0.01):
        """
        初始化均线多头策略
        
        Args:
            balance: 初始资金
            threshold: 趋势检查周期
            ma_periods: 均线周期列表（如 [5, 10, 30]）
            min_rise: MA30 最低涨幅要求（默认 1.2 即 20%）
            risk_per_trade: 每笔交易风险比例（默认 1%）
        """
        self.balance = balance
        self.threshold = threshold
        self.ma_periods = ma_periods
        self.min_rise = min_rise
        self.risk_per_trade = risk_per_trade
        self.positions = {}  # 持仓：{code: {'quantity': int, 'entry_price': float, 'stop_loss': float}}
        self.trades = []  # 交易记录
        self.ma_tags = [f'ma{period}' for period in ma_periods]
    
    def validate_data(self, data: pd.DataFrame, code: str) -> bool:
        """验证输入数据是否包含必要列"""
        required_columns = {'日期', '收盘', '开盘', '最高', '最低', '成交量'}
        if not required_columns.issubset(data.columns):
            logging.error(f"Data for {code} missing required columns: {required_columns - set(data.columns)}")
            return False
        return True
    
    def check_ma_trend(self, code: str, data: pd.DataFrame, end_date: Optional[str] = None) -> bool:
        """
        检查均线多头趋势：多条均线排列（MA5 > MA10 > MA30）且 MA30 持续上涨
        
        Args:
            code: 股票代码
            data: 包含日期、收盘等列的 DataFrame
            end_date: 截止日期（可选）
        
        Returns:
            bool: 是否满足均线多头趋势
        """
        try:
            if not self.validate_data(data, code):
                return False
            
            if len(data) < max(self.threshold, max(self.ma_periods)):
                logging.warning(f"Insufficient data for {code}: {len(data)} < {max(self.threshold, max(self.ma_periods))}")
                return False
            
            data = data.copy()
            # 计算多条均线
            for period, tag in zip(self.ma_periods, self.ma_tags):
                data[tag] = tl.MA(data['收盘'].values, period)
            
            if end_date:
                data = data[data['日期'] <= end_date]
            
            recent_data = data.tail(self.threshold)
            last_row = recent_data.iloc[-1]
            
            # 检查均线空值
            if any(pd.isna(last_row[tag]) for tag in self.ma_tags):
                logging.warning(f"MA calculation failed for {code}")
                return False
            
            # 检查均线多头排列（MA5 > MA10 > MA30）
            ma_values = [last_row[tag] for tag in self.ma_tags]
            if not all(ma_values[i] > ma_values[i + 1] for i in range(len(ma_values) - 1)):
                return False
            
            # 检查 MA30 趋势（分段递增且涨幅 >= min_rise）
            ma30_data = recent_data['ma30']
            first_third = round(self.threshold / 3)
            second_third = round(self.threshold * 2 / 3)
            ma30_values = [
                ma30_data.iloc[0],
                ma30_data.iloc[first_third],
                ma30_data.iloc[second_third],
                ma30_data.iloc[-1]
            ]
            if not (ma30_values[0] < ma30_values[1] < ma30_values[2] < ma30_values[3] and 
                    ma30_values[-1] >= self.min_rise * ma30_values[0]):
                return False
            
            logging.info(f"{code}: MA trend confirmed (MA5={ma_values[0]:.2f}, MA10={ma_values[1]:.2f}, MA30={ma_values[2]:.2f})")
            return True
        except Exception as e:
            logging.error(f"Error in check_ma_trend for {code}: {e}")
            return False
    
    def calculate_atr(self, data: pd.DataFrame) -> float:
        """计算平均真实波幅 (ATR)"""
        try:
            high = data['最高']
            low = data['最低']
            close = data['收盘'].shift(1)
            tr = pd.concat([high - low, abs(high - close), abs(low - close)], axis=1).max(axis=1)
            return tr.tail(20).mean()
        except Exception as e:
            logging.error(f"Error in calculate_atr: {e}")
            return np.nan
    
    def calculate_position_size(self, data: pd.DataFrame, entry_price: float) -> int:
        """
        根据 ATR 计算仓位大小
        
        Args:
            data: 包含最高、最低、收盘等列的 DataFrame
            entry_price: 入场价格
        
        Returns:
            int: 购买股数
        """
        try:
            atr = self.calculate_atr(data)
            if pd.isna(atr):
                logging.warning("ATR calculation failed, using default size")
                return 0
            
            risk_amount = self.balance * self.risk_per_trade
            risk_per_share = 2 * atr
            position_size = int(risk_amount / risk_per_share)
            max_shares = int(self.balance / entry_price)
            return min(position_size, max_shares)
        except Exception as e:
            logging.error(f"Error in calculate_position_size: {e}")
            return 0
    
    def execute_trade(self, code: str, data: pd.DataFrame, date: str, is_entry: bool) -> None:
        """
        执行交易（买入或卖出）
        
        Args:
            code: 股票代码
            data: 包含日期、收盘等列的 DataFrame
            date: 交易日期
            is_entry: True 表示买入，False 表示卖出
        """
        try:
            current_price = data[data['日期'] == date]['收盘'].iloc[0]
            if is_entry:
                if code not in self.positions:
                    quantity = self.calculate_position_size(data, current_price)
                    if quantity > 0:
                        cost = quantity * current_price
                        if cost <= self.balance:
                            atr = self.calculate_atr(data)
                            stop_loss = current_price - 2 * atr if pd.notna(atr) else current_price * 0.9
                            self.positions[code] = {
                                'quantity': quantity,
                                'entry_price': current_price,
                                'stop_loss': stop_loss
                            }
                            self.balance -= cost
                            self.trades.append({
                                'date': date,
                                'code': code,
                                'type': 'buy',
                                'price': current_price,
                                'quantity': quantity
                            })
                            logging.info(f"Buy {code}: {quantity} shares at {current_price} on {date}")
            else:
                if code in self.positions:
                    quantity = self.positions[code]['quantity']
                    revenue = quantity * current_price
                    self.balance += revenue
                    self.trades.append({
                        'date': date,
                        'code': code,
                        'type': 'sell',
                        'price': current_price,
                        'quantity': quantity
                    })
                    logging.info(f"Sell {code}: {quantity} shares at {current_price} on {date}")
                    del self.positions[code]
        except Exception as e:
            logging.error(f"Error in execute_trade for {code}: {e}")
    
    def backtest(self, stock_data: Dict[str, pd.DataFrame], start_date: str, end_date: str) -> Tuple[float, List[Dict]]:
        """
        回测均线多头策略
        
        Args:
            stock_data: 字典，{code: DataFrame}，DataFrame 包含日期、收盘、最高、最低等列
            start_date: 回测开始日期
            end_date: 回测结束日期
        
        Returns:
            Tuple[float, list]: 最终余额，交易记录
        """
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')
            for code, data in stock_data.items():
                data = data[(data['日期'] <= date_str) & (data['日期'] >= start_date)].copy()
                if len(data) < max(self.threshold, max(self.ma_periods)):
                    continue
                
                # 检查入场条件
                if self.check_ma_trend(code, data, date_str):
                    self.execute_trade(code, data, date_str, is_entry=True)
                
                # 检查离场（止损或均线跌破）
                if code in self.positions:
                    current_data = data.tail(1)
                    current_price = current_data['收盘'].iloc[0]
                    stop_loss = self.positions[code]['stop_loss']
                    # 计算最新均线
                    for period, tag in zip(self.ma_periods, self.ma_tags):
                        current_data[tag] = tl.MA(current_data['收盘'].values, period)
                    ma_values = [current_data[tag].iloc[-1] for tag in self.ma_tags]
                    # 均线跌破或止损
                    if (len(ma_values) > 1 and ma_values[0] < ma_values[1]) or current_price <= stop_loss:
                        self.execute_trade(code, data, date_str, is_entry=False)
        
        # 计算最终资产
        final_balance = self.balance
        for code, pos in self.positions.items():
            last_price = stock_data[code]['收盘'].iloc[-1]
            final_balance += pos['quantity'] * last_price
        
        return final_balance, self.trades

# 获取并过滤股票数据
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_stock_data(code: str) -> pd.DataFrame:
    return ak.stock_zh_a_hist(symbol=code, period='daily')

def get_filtered_stocks() -> Dict[str, pd.DataFrame]:
    try:
        all_data = ak.stock_zh_a_spot_em()
        filtered_data = all_data[
            (~all_data['代码'].str.startswith(('688', '300'))) &
            (~all_data['名称'].str.contains('ST', case=False, na=False)) &
            (all_data['总市值'] >= 10_000_000_000)
        ]
        
        stock_data = {}
        for code in filtered_data['代码']:
            try:
                df = fetch_stock_data(code)
                stock_data[code] = df
            except Exception as e:
                logging.error(f"Failed to fetch data for {code}: {e}")
        return stock_data
    except Exception as e:
        logging.error(f"Error fetching stock data: {e}")
        return {}

# 示例使用
if __name__ == "__main__":
    # 获取过滤后的股票数据
    stock_data = get_filtered_stocks()
    
    # 初始化策略
    strategy = MABullStrategy(balance=200_000, threshold=30, ma_periods=[5, 10, 30], min_rise=1.2)
    
    # 运行回测
    final_balance, trades = strategy.backtest(stock_data, '2023-01-01', '2023-12-31')
    print(f"Final Balance: {final_balance}")
    print(f"Trades: {len(trades)}")
    for trade in trades:
        print(trade)