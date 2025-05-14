# -*- encoding: UTF-8 -*-

# import talib as tl
# import pandas as pd
# import logging


# # TODO 真实波动幅度（ATR）放大
# # 最后一个交易日收市价从下向上突破指定区间内最高价
# def check_breakthrough(code_name, data, end_date=None, threshold=30):
#     max_price = 0
#     if end_date is not None:
#         mask = (data['日期'] <= end_date)
#         data = data.loc[mask]
#     data = data.tail(n=threshold+1)
#     if len(data) < threshold + 1:
#         logging.debug("{0}:样本小于{1}天...\n".format(code_name, threshold))
#         return False

#     # 最后一天收市价
#     last_close = float(data.iloc[-1]['收盘'])
#     last_open = float(data.iloc[-1]['开盘'])

#     data = data.head(n=threshold)
#     second_last_close = data.iloc[-1]['收盘']

#     for index, row in data.iterrows():
#         if row['收盘'] > max_price:
#             max_price = float(row['收盘'])

#     if last_close > max_price > second_last_close and max_price > last_open \
#             and last_close / last_open > 1.06:
#         return True
#     else:
#         return False


# # 收盘价高于N日均线
# def check_ma(code_name, data, end_date=None, ma_days=250):
#     if data is None or len(data) < ma_days:
#         logging.debug("{0}:样本小于{1}天...\n".format(code_name, ma_days))
#         return False

#     ma_tag = 'ma' + str(ma_days)
#     data[ma_tag] = pd.Series(tl.MA(data['收盘'].values, ma_days), index=data.index.values)

#     if end_date is not None:
#         mask = (data['日期'] <= end_date)
#         data = data.loc[mask]

#     last_close = data.iloc[-1]['收盘']
#     last_ma = data.iloc[-1][ma_tag]
#     if last_close > last_ma:
#         return True
#     else:
#         return False


# # 上市日小于60天
# def check_new(code_name, data, end_date=None, threshold=60):
#     size = len(data.index)
#     if size < threshold:
#         return True
#     else:
#         return False


# # 量比大于2
# # 例如：
# #   2017-09-26 2019-02-11 京东方A
# #   2019-03-22 浙江龙盛
# #   2019-02-13 汇顶科技
# #   2019-01-29 新城控股
# #   2017-11-16 保利地产
# def check_volume(code_name, data, end_date=None, threshold=60):
#     if len(data) < threshold:
#         logging.debug("{0}:样本小于250天...\n".format(code_name))
#         return False
#     data['vol_ma5'] = pd.Series(tl.MA(data['成交量'].values, 5), index=data.index.values)

#     if end_date is not None:
#         mask = (data['日期'] <= end_date)
#         data = data.loc[mask]
#     if data.empty:
#         return False
#     p_change = data.iloc[-1]['p_change']
#     if p_change < 2 \
#             or data.iloc[-1]['收盘'] < data.iloc[-1]['开盘']:
#         return False
#     data = data.tail(n=threshold + 1)
#     if len(data) < threshold + 1:
#         logging.debug("{0}:样本小于{1}天...\n".format(code_name, threshold))
#         return False

#     # 最后一天收盘价
#     last_close = data.iloc[-1]['收盘']
#     # 最后一天成交量
#     last_vol = data.iloc[-1]['成交量']

#     amount = last_close * last_vol * 100

#     # 成交额不低于2亿
#     if amount < 200000000:
#         return False

#     data = data.head(n=threshold)

#     mean_vol = data.iloc[-1]['vol_ma5']

#     vol_ratio = last_vol / mean_vol
#     if vol_ratio >= 2:
#         msg = "*{0}\n量比：{1:.2f}\t涨幅：{2}%\n".format(code_name, vol_ratio, p_change)
#         logging.debug(msg)
#         return True
#     else:
#         return False


# # 量比大于3.0
# def check_continuous_volume(code_name, data, end_date=None, threshold=60, window_size=3):
#     stock = code_name[0]
#     name = code_name[1]
#     data['vol_ma5'] = pd.Series(tl.MA(data['成交量'].values, 5), index=data.index.values)
#     if end_date is not None:
#         mask = (data['日期'] <= end_date)
#         data = data.loc[mask]
#     data = data.tail(n=threshold + window_size)
#     if len(data) < threshold + window_size:
#         logging.debug("{0}:样本小于{1}天...\n".format(code_name, threshold+window_size))
#         return False

#     # 最后一天收盘价
#     last_close = data.iloc[-1]['收盘']
#     # 最后一天成交量
#     last_vol = data.iloc[-1]['成交量']

#     data_front = data.head(n=threshold)
#     data_end = data.tail(n=window_size)

#     mean_vol = data_front.iloc[-1]['vol_ma5']

#     for index, row in data_end.iterrows():
#         if float(row['成交量']) / mean_vol < 3.0:
#             return False

#     msg = "*{0} 量比：{1:.2f}\n\t收盘价：{2}\n".format(code_name, last_vol/mean_vol, last_close)
#     logging.debug(msg)
#     return True
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

class VolumeBreakoutStrategy:
    """放量上涨选股策略实现类"""
    
    def __init__(self, balance: float = INITIAL_BALANCE, threshold: int = 30, 
                 ma_days: int = 250, volume_threshold: int = 60, 
                 risk_per_trade: float = 0.01):
        """
        初始化放量上涨策略
        
        Args:
            balance: 初始资金
            threshold: 突破周期（检查最高价的周期）
            ma_days: 均线周期
            volume_threshold: 量比计算周期
            risk_per_trade: 每笔交易风险比例（默认 1%）
        """
        self.balance = balance
        self.threshold = threshold
        self.ma_days = ma_days
        self.volume_threshold = volume_threshold
        self.risk_per_trade = risk_per_trade
        self.positions = {}  # 持仓：{code: {'quantity': int, 'entry_price': float, 'stop_loss': float}}
        self.trades = []  # 交易记录
        self.ma_tag = f'ma{ma_days}'
    
    def validate_data(self, data: pd.DataFrame, code: str) -> bool:
        """验证输入数据是否包含必要列"""
        required_columns = {'日期', '收盘', '开盘', '成交量', '最高', '最低'}
        if not required_columns.issubset(data.columns):
            logging.error(f"Data for {code} missing required columns: {required_columns - set(data.columns)}")
            return False
        return True
    
    def check_breakthrough(self, code: str, data: pd.DataFrame, end_date: Optional[str] = None) -> bool:
        """
        检查是否满足突破条件：最后收盘价突破前 N 天最高价，且涨幅大于 6%
        
        Args:
            code: 股票代码
            data: 包含日期、收盘、开盘等列的 DataFrame
            end_date: 截止日期（可选）
        
        Returns:
            bool: 是否满足突破条件
        """
        try:
            if not self.validate_data(data, code):
                return False
            
            if end_date:
                data = data[data['日期'] <= end_date].copy()
            
            if len(data) < self.threshold + 1:
                logging.warning(f"Insufficient data for {code}: {len(data)} < {self.threshold + 1}")
                return False
            
            recent_data = data.tail(self.threshold + 1)
            last_row = recent_data.iloc[-1]
            last_close = last_row['收盘']
            last_open = last_row['开盘']
            second_last_close = recent_data.iloc[-2]['收盘']
            prior_data = recent_data.head(self.threshold)
            highest_close = prior_data['收盘'].max()
            
            # 突破条件：收盘价 > 前 N 天最高价 > 前一天收盘价，且当日涨幅 > 6%
            return (last_close > highest_close > second_last_close and 
                    last_close > last_open and 
                    last_close / last_open > 1.06)
        except Exception as e:
            logging.error(f"Error in check_breakthrough for {code}: {e}")
            return False
    
    def check_ma(self, code: str, data: pd.DataFrame, end_date: Optional[str] = None) -> bool:
        """
        检查收盘价是否高于 N 日均线
        
        Args:
            code: 股票代码
            data: 包含日期、收盘等列的 DataFrame
            end_date: 截止日期（可选）
        
        Returns:
            bool: 是否高于均线
        """
        try:
            if not self.validate_data(data, code):
                return False
            
            if len(data) < self.ma_days:
                logging.warning(f"Insufficient data for {code}: {len(data)} < {self.ma_days}")
                return False
            
            data = data.copy()
            data[self.ma_tag] = tl.MA(data['收盘'].values, self.ma_days)
            
            if end_date:
                data = data[data['日期'] <= end_date]
            
            last_row = data.iloc[-1]
            if pd.isna(last_row[self.ma_tag]):
                logging.warning(f"MA calculation failed for {code}")
                return False
            
            return last_row['收盘'] > last_row[self.ma_tag]
        except Exception as e:
            logging.error(f"Error in check_ma for {code}: {e}")
            return False
    
    def check_new(self, code: str, data: pd.DataFrame, end_date: Optional[str] = None, 
                  threshold: int = 60) -> bool:
        """
        检查是否为新股（上市天数少于 threshold 天）
        
        Args:
            code: 股票代码
            data: 包含日期等列的 DataFrame
            end_date: 截止日期（可选）
            threshold: 上市天数阈值
        
        Returns:
            bool: 是否为新股
        """
        try:
            if not self.validate_data(data, code):
                return False
            
            if end_date:
                data = data[data['日期'] <= end_date].copy()
            
            return len(data) < threshold
        except Exception as e:
            logging.error(f"Error in check_new for {code}: {e}")
            return False
    
    def check_volume(self, code: str, data: pd.DataFrame, end_date: Optional[str] = None) -> bool:
        """
        检查量比是否大于 2，成交额是否不低于 2 亿，且为上涨日
        
        Args:
            code: 股票代码
            data: 包含日期、收盘、开盘、成交量等列的 DataFrame
            end_date: 截止日期（可选）
        
        Returns:
            bool: 是否满足量比和成交额条件
        """
        try:
            if not self.validate_data(data, code):
                return False
            
            if len(data) < self.volume_threshold + 1:
                logging.warning(f"Insufficient data for {code}: {len(data)} < {self.volume_threshold + 1}")
                return False
            
            data = data.copy()
            data['vol_ma5'] = tl.MA(data['成交量'].values, 5)
            
            if end_date:
                data = data[data['日期'] <= end_date]
            
            recent_data = data.tail(self.volume_threshold + 1)
            last_row = recent_data.iloc[-1]
            last_close = last_row['收盘']
            last_open = last_row['开盘']
            last_vol = last_row['成交量']
            mean_vol = recent_data.iloc[-2]['vol_ma5']
            
            if pd.isna(mean_vol):
                logging.warning(f"Volume MA calculation failed for {code}")
                return False
            
            vol_ratio = last_vol / mean_vol
            amount = last_close * last_vol * 100  # 成交额（元）
            
            # 条件：量比 >= 2，成交额 >= 2 亿，当日上涨
            if (vol_ratio >= 2 and 
                amount >= 200_000_000 and 
                last_close > last_open):
                logging.info(f"{code}: Volume ratio: {vol_ratio:.2f}, Amount: {amount:.2f}")
                return True
            return False
        except Exception as e:
            logging.error(f"Error in check_volume for {code}: {e}")
            return False
    
    def check_continuous_volume(self, code: str, data: pd.DataFrame, end_date: Optional[str] = None, 
                               window_size: int = 3) -> bool:
        """
        检查连续 window_size 天量比是否大于 3
        
        Args:
            code: 股票代码
            data: 包含日期、成交量等列的 DataFrame
            end_date: 截止日期（可选）
            window_size: 连续天数
        
        Returns:
            bool: 是否满足连续放量条件
        """
        try:
            if not self.validate_data(data, code):
                return False
            
            if len(data) < self.volume_threshold + window_size:
                logging.warning(f"Insufficient data for {code}: {len(data)} < {self.volume_threshold + window_size}")
                return False
            
            data = data.copy()
            data['vol_ma5'] = tl.MA(data['成交量'].values, 5)
            
            if end_date:
                data = data[data['日期'] <= end_date]
            
            recent_data = data.tail(self.volume_threshold + window_size)
            prior_data = recent_data.head(self.volume_threshold)
            last_window = recent_data.tail(window_size)
            mean_vol = prior_data.iloc[-1]['vol_ma5']
            
            if pd.isna(mean_vol):
                logging.warning(f"Volume MA calculation failed for {code}")
                return False
            
            for _, row in last_window.iterrows():
                if row['成交量'] / mean_vol < 3.0:
                    return False
            
            last_close = last_window.iloc[-1]['收盘']
            vol_ratio = last_window.iloc[-1]['成交量'] / mean_vol
            logging.info(f"{code}: Continuous volume ratio: {vol_ratio:.2f}, Close: {last_close}")
            return True
        except Exception as e:
            logging.error(f"Error in check_continuous_volume for {code}: {e}")
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
        回测放量上涨策略
        
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
                if len(data) < max(self.threshold + 1, self.ma_days, self.volume_threshold + 3):
                    continue
                
                # 检查选股条件
                if (self.check_breakthrough(code, data, date_str) and 
                    self.check_ma(code, data, date_str) and 
                    self.check_volume(code, data, date_str)):
                    self.execute_trade(code, data, date_str, is_entry=True)
                
                # 检查离场（止损或固定持有期）
                if code in self.positions:
                    current_price = data[data['日期'] == date_str]['收盘'].iloc[0]
                    stop_loss = self.positions[code]['stop_loss']
                    entry_date = pd.to_datetime(self.trades[-1]['date'] if self.trades else start_date)
                    days_held = (pd.to_datetime(date_str) - entry_date).days
                    if current_price <= stop_loss or days_held >= 10:  # 持有 10 天后卖出
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
    strategy = VolumeBreakoutStrategy(balance=200_000, threshold=30, ma_days=250, volume_threshold=60)
    
    # 运行回测
    final_balance, trades = strategy.backtest(stock_data, '2023-01-01', '2023-12-31')
    print(f"Final Balance: {final_balance}")
    print(f"Trades: {len(trades)}")
    for trade in trades:
        print(trade)