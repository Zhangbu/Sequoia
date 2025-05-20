import pandas as pd
import logging
import talib
from datetime import datetime, timedelta
import akshare as ak
import numpy as np # 用于处理NaN值

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 策略配置 ---
# 注意：这里的一些参数是示例值，您需要根据实际回测和市场情况进行调整
STRATEGY_CONFIG = {
    # 基础筛选
    'min_avg_daily_turnover_amount': 100_000_000, # 日均成交额 > 1亿元
    'avg_turnover_days': 20, # 计算日均成交额的天数
    'min_listed_days': 60, # 上市天数 > 60天
    'exclude_st': True, # 剔除ST股
    'exclude_star_st': True, # 剔除*ST股

    # 技术指标筛选
    'ma5_cross_ma10_period': 3, # 5日均线金叉10日均线，在近几天内发生
    'close_above_ma20': True, # 股价高于20日均线
    'macd_gold_cross_within_days': 3, # MACD金叉在近几天内发生
    'macd_dif_above_dea_and_zero': True, # DIF > DEA 且 DIF > 0
    'volume_ratio_to_5day_avg_min': 1.5, # 当日/昨日成交量 > 5日均量 1.5倍
    'volume_ratio_to_5day_avg_days': 5, # 计算5日均量天数
    'boll_break_middle_band': True, # 股价上穿布林带中轨
    'rsi_period': 6, # RSI计算周期
    'rsi_cross_30': True, # RSI上穿30 (超卖区拐头向上)
    'kdj_gold_cross': True, # KDJ金叉
    'kdj_j_upper_limit': 50, # KDJ J值上限，避免高位金叉

    # 基本面与市场情绪 (此处为简化处理，需结合外部数据或人工判断)
    'min_circulating_market_cap': 5_000_000_000, # 流通市值 > 50亿
    'max_circulating_market_cap': 500_000_000_000, # 流通市值 < 500亿 (5000亿)
    # 'pe_ttm_range': (10, 100), # 市盈率TTM区间，短线不强制，可以根据行业调整
    'min_daily_turnover_rate': 3.0, # 每日换手率 > 3%

    # 波动性要求 (更偏向人工观察，代码中暂不严格量化，可结合换手率辅助判断)
    # 'min_daily_amplitude': 0.03, # 每日振幅百分比
}

def calculate_indicators(data: pd.DataFrame):
    """
    计算所有需要的技术指标并添加到DataFrame中。
    """
    # 确保日期是datetime类型并排序
    data['日期'] = pd.to_datetime(data['日期'])
    data = data.sort_values(by='日期').reset_index(drop=True)

    close = data['收盘'].values
    high = data['最高'].values
    low = data['最低'].values
    volume = data['成交量'].values

    # 均线
    data['MA5'] = talib.SMA(close, timeperiod=5)
    data['MA10'] = talib.SMA(close, timeperiod=10)
    data['MA20'] = talib.SMA(close, timeperiod=20)

    # MACD
    data['MACD_DIF'], data['MACD_DEA'], data['MACD_HIST'] = talib.MACD(
        close, fastperiod=12, slowperiod=26, signalperiod=9
    )

    # KDJ
    data['KDJ_K'], data['KDJ_D'] = talib.STOCH(
        high, low, close, fastk_period=9, slowk_period=3, slowd_period=3
    )
    data['KDJ_J'] = 3 * data['KDJ_K'] - 2 * data['KDJ_D']

    # RSI
    data['RSI'] = talib.RSI(close, timeperiod=STRATEGY_CONFIG['rsi_period'])

    # 布林带
    data['BOLL_UPPER'], data['BOLL_MIDDLE'], data['BOLL_LOWER'] = talib.BBANDS(
        close, timeperiod=20, nbdevup=2, nbdevdn=2
    )

    # 5日均量
    data['VOL_MA5'] = talib.SMA(volume, timeperiod=STRATEGY_CONFIG['volume_ratio_to_5day_avg_days'])

    return data

def check_stock_meets_criteria(stock_info, stock_data, current_date=None, config=STRATEGY_CONFIG):
    """
    检查单个股票是否符合入场策略。
    :param stock_info: 股票代码和名称，例如 ("000001", "平安银行")
    :param stock_data: 包含历史数据的Pandas DataFrame
    :param current_date: 用于回测的当前日期，默认为最新数据日期
    :param config: 策略配置字典
    :return: True/False
    """
    stock_code, stock_name = stock_info

    # 1. 数据校验与日期过滤
    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty or \
       not {'日期', '收盘', '开盘', '最高', '最低', '成交量', '换手率'}.issubset(stock_data.columns):
        logger.warning(f"[{stock_name}({stock_code})]: 数据格式错误或缺少必要列。")
        return False

    # 确保日期列为datetime类型并排序
    stock_data['日期'] = pd.to_datetime(stock_data['日期'])
    stock_data = stock_data.sort_values(by='日期').reset_index(drop=True)

    if current_date:
        current_date = pd.to_datetime(current_date)
        data = stock_data[stock_data['日期'] <= current_date].copy()
    else:
        data = stock_data.copy()

    # 确保数据长度足够计算指标
    min_required_len = max(
        26, # MACD
        20, # BOLL, MA20
        config['volume_ratio_to_5day_avg_days'],
        config['avg_turnover_days'],
        config['min_listed_days'] // 2 # 粗略估计指标计算所需天数
    ) + 5 # 额外留一些余量
    if len(data) < min_required_len:
        logger.debug(f"[{stock_name}({stock_code})]: 数据长度不足 {min_required_len} 天，无法计算所有指标。")
        return False

    # 计算技术指标
    data = calculate_indicators(data)

    # 取最新一天的数据
    latest_data = data.iloc[-1]
    prev_data = data.iloc[-2] # 前一天数据

    # 检查是否有NaN值，可能是数据不够
    if latest_data.isnull().any() or prev_data.isnull().any():
        logger.debug(f"[{stock_name}({stock_code})]: 最新数据或前一天数据包含NaN值，可能由于数据不足。")
        return False

    # --- 基础筛选 ---
    # 剔除ST、*ST股
    if config['exclude_st'] and 'ST' in stock_name:
        logger.debug(f"[{stock_name}({stock_code})]: 剔除ST股。")
        return False
    if config['exclude_star_st'] and '*ST' in stock_name:
        logger.debug(f"[{stock_name}({stock_code})]: 剔除*ST股。")
        return False

    # 上市天数
    listed_days = len(data) # 假设提供的数据是从上市以来
    if listed_days < config['min_listed_days']:
        logger.debug(f"[{stock_name}({stock_code})]: 上市天数不足 {config['min_listed_days']} 天 ({listed_days}天)。")
        return False

    # 日均成交额 (取近N天)
    if len(data) < config['avg_turnover_days']:
        logger.debug(f"[{stock_name}({stock_code})]: 数据不足 {config['avg_turnover_days']} 天，无法计算日均成交额。")
        return False
    avg_daily_turnover_amount = data['成交额'].iloc[-config['avg_turnover_days']:].mean() if '成交额' in data.columns else None
    if avg_daily_turnover_amount is None or avg_daily_turnover_amount < config['min_avg_daily_turnover_amount']:
        logger.debug(f"[{stock_name}({stock_code})]: 日均成交额 ({avg_daily_turnover_amount/1_000_000:.2f}亿) 低于 {config['min_avg_turnover_amount']/1_000_000:.2f}亿。")
        return False

    # --- 技术指标筛选 ---
    # 均线形态 (5日均线 上穿 10日均线)
    ma5_cross_ma10 = False
    for i in range(1, config['ma5_cross_ma10_period'] + 1):
        if data['MA5'].iloc[-i-1] <= data['MA10'].iloc[-i-1] and data['MA5'].iloc[-i] > data['MA10'].iloc[-i]:
            ma5_cross_ma10 = True
            break
    if not ma5_cross_ma10:
        logger.debug(f"[{stock_name}({stock_code})]: 近{config['ma5_cross_ma10_period']}天未发生5日均线上穿10日均线。")
        return False

    # 股价 高于 20日均线
    if config['close_above_ma20'] and not (latest_data['收盘'] > latest_data['MA20']):
        logger.debug(f"[{stock_name}({stock_code})]: 股价 ({latest_data['收盘']:.2f}) 未高于20日均线 ({latest_data['MA20']:.2f})。")
        return False

    # MACD指标 (金叉或强势区)
    macd_gold_cross = False
    for i in range(1, config['macd_gold_cross_within_days'] + 1):
        if data['MACD_DIF'].iloc[-i-1] <= data['MACD_DEA'].iloc[-i-1] and \
           data['MACD_DIF'].iloc[-i] > data['MACD_DEA'].iloc[-i]:
            macd_gold_cross = True
            break
    if not macd_gold_cross:
        logger.debug(f"[{stock_name}({stock_code})]: 近{config['macd_gold_cross_within_days']}天未发生MACD金叉。")
        return False
    if config['macd_dif_above_dea_and_zero'] and not (latest_data['MACD_DIF'] > latest_data['MACD_DEA'] and latest_data['MACD_DIF'] > 0):
        logger.debug(f"[{stock_name}({stock_code})]: MACD不满足 DIF > DEA 且 DIF > 0。")
        return False

    # 成交量 (放量)
    # 当日成交量与5日均量对比
    if latest_data['VOL_MA5'] == 0: # 避免除以零
         logger.debug(f"[{stock_name}({stock_code})]: 5日均量为零。")
         return False
    
    volume_ratio = latest_data['成交量'] / latest_data['VOL_MA5']
    if not (config['volume_ratio_to_5day_avg_min'] <= volume_ratio <= config['volume_ratio_to_5day_avg_max']):
        logger.debug(f"[{stock_name}({stock_code})]: 成交量 ({latest_data['成交量']:.0f}) 不满足放量条件 ({volume_ratio:.2f}倍5日均量)，要求 {config['volume_ratio_to_5day_avg_min']} - {config['volume_ratio_to_5day_avg_max']} 倍。")
        return False
    
    # 布林带 (突破中轨)
    if config['boll_break_middle_band'] and not (prev_data['收盘'] <= prev_data['BOLL_MIDDLE'] and latest_data['收盘'] > latest_data['BOLL_MIDDLE']):
        logger.debug(f"[{stock_name}({stock_code})]: 未上穿布林带中轨。")
        return False

    # RSI (上穿30)
    if config['rsi_cross_30'] and not (data['RSI'].iloc[-2] <= 30 and latest_data['RSI'] > 30):
        logger.debug(f"[{stock_name}({stock_code})]: RSI ({latest_data['RSI']:.2f}) 未上穿30。")
        return False

    # KDJ (金叉且J值不过高)
    if config['kdj_gold_cross'] and not (prev_data['KDJ_K'] <= prev_data['KDJ_D'] and latest_data['KDJ_K'] > latest_data['KDJ_D']):
        logger.debug(f"[{stock_name}({stock_code})]: KDJ未金叉。")
        return False
    if config['kdj_j_upper_limit'] and not (latest_data['KDJ_J'] < config['kdj_j_upper_limit']):
        logger.debug(f"[{stock_name}({stock_code})]: KDJ J值 ({latest_data['KDJ_J']:.2f}) 过高，不满足 < {config['kdj_j_upper_limit']}。")
        return False

    # --- 基本面与市场情绪 (此处简化，需结合实际数据或API) ---
    # 流通市值 (注意：akshare的stock_zh_a_spot_em可以获取实时流通市值)
    # 这里我们假设有一个获取流通市值的函数，或者在获取股票列表时就带有该信息
    # For now, let's skip this check if the data is not available
    # current_market_cap = get_market_cap(stock_code) # 需要您实现或获取
    # if not (config['min_circulating_market_cap'] <= current_market_cap <= config['max_circulating_market_cap']):
    #     logger.debug(f"[{stock_name}({stock_code})]: 流通市值 ({current_market_cap/1_000_000_000:.2f}亿) 不在 {config['min_circulating_market_cap']/1_000_000_000:.2f}-{config['max_circulating_market_cap']/1_000_000_000:.2f}亿区间。")
    #     return False

    # 换手率
    if latest_data['换手率'] < config['min_daily_turnover_rate']:
        logger.debug(f"[{stock_name}({stock_code})]: 换手率 ({latest_data['换手率']:.2f}%) 低于 {config['min_daily_turnover_rate']}%。")
        return False

    # --- 综合判断 ---
    logger.info(f"[{stock_name}({stock_code})]: ✨ 恭喜，股票符合所有入场条件！")
    return True

# --- 交易辅助函数 (非策略本身，但与策略执行相关) ---

def simulate_trade(stock_data, buy_price, target_profit_ratio=0.02, stop_loss_ratio=0.025):
    """
    模拟单次交易的止盈止损逻辑。
    :param stock_data: 股票的日线数据 (DataFrame)
    :param buy_price: 买入价格
    :param target_profit_ratio: 目标止盈比例 (例如 0.02 代表 2%)
    :param stop_loss_ratio: 固定止损比例 (例如 0.025 代表 2.5%)
    :return: 交易结果字典
    """
    if stock_data.empty:
        return {"status": "No data", "profit": 0}

    # 假设买入后第二天开始计算
    trade_start_index = stock_data[stock_data['收盘'] >= buy_price].index[0] if not stock_data[stock_data['收盘'] >= buy_price].empty else 0
    
    # 遍历买入后的几天数据进行模拟
    for i in range(trade_start_index, len(stock_data)):
        current_day_data = stock_data.iloc[i]
        high_price = current_day_data['最高']
        low_price = current_day_data['最低']
        close_price = current_day_data['收盘']

        # 检查止盈
        profit = (high_price - buy_price) / buy_price
        if profit >= target_profit_ratio:
            logger.info(f"达到止盈目标，买入价: {buy_price:.2f}, 止盈价: {buy_price * (1 + target_profit_ratio):.2f}, 实际最高: {high_price:.2f}")
            return {"status": "Profit Taken", "profit": target_profit_ratio, "exit_price": buy_price * (1 + target_profit_ratio)}

        # 检查止损
        loss = (buy_price - low_price) / buy_price
        if loss >= stop_loss_ratio:
            logger.info(f"触发止损，买入价: {buy_price:.2f}, 止损价: {buy_price * (1 - stop_loss_ratio):.2f}, 实际最低: {low_price:.2f}")
            return {"status": "Stop Loss", "profit": -stop_loss_ratio, "exit_price": buy_price * (1 - stop_loss_ratio)}

        # 如果持股周期有限，可以在这里添加天数限制
        # if (current_day_data['日期'] - stock_data.iloc[trade_start_index]['日期']).days > 3: # 例如持有不超过3天
        #     logger.info(f"达到最大持股周期，当前收盘价: {close_price:.2f}")
        #     return {"status": "Time Limit", "profit": (close_price - buy_price) / buy_price, "exit_price": close_price}

    # 如果遍历完所有数据都没有止盈止损
    final_profit = (close_price - buy_price) / buy_price
    logger.info(f"未触发止盈止损，以最新收盘价 {close_price:.2f} 结算，利润: {final_profit:.2%}")
    return {"status": "Hold to End", "profit": final_profit, "exit_price": close_price}

# --- 选股主流程 (结合之前的并行数据获取) ---
# 注意：你需要将之前的 `Workspace` 和 `run` 函数整合到这里，
# 并且 `Workspace` 需要能够获取到策略所需的完整数据（包括成交额、换手率等）。

# 假设这是你已经获取并预处理好的所有股票数据
# stocks_data = {
#    ("000001", "平安银行"): pd.DataFrame(...),
#    ("600002", "万科A"): pd.DataFrame(...),
#    ...
# }

def find_potential_stocks(all_stocks_data, target_date=None):
    """
    根据策略从所有股票数据中筛选出符合条件的股票。
    :param all_stocks_data: 字典，键为(股票代码, 股票名称)元组，值为对应的DataFrame数据。
    :param target_date: 目标筛选日期，默认为空，表示取最新数据
    :return: 符合条件的股票列表 [(code, name), ...]
    """
    eligible_stocks = []
    logger.info(f"开始在 {len(all_stocks_data)} 支股票中筛选...")
    for stock_info, data in all_stocks_data.items():
        if check_stock_meets_criteria(stock_info, data.copy(), current_date=target_date):
            eligible_stocks.append(stock_info)
    logger.info(f"筛选完成，找到 {len(eligible_stocks)} 支符合条件的股票。")
    return eligible_stocks

# --- Main execution block (结合您的数据获取和选股逻辑) ---
if __name__ == "__main__":
    # 模拟数据获取 (替换为您实际的 akshare 数据获取逻辑)
    # 假设 fetch_all_stock_hist_data 已经实现了并行获取和缓存
    from your_data_fetch_module import fetch_all_stock_hist_data # 替换为你的数据获取模块名

    # 示例股票列表，实际可以从 ak.stock_zh_a_spot_em() 获取所有A股列表
    # 注意：ak.stock_zh_a_spot_em() 提供实时行情，但历史数据需要单独获取
    # 您可能需要先获取所有A股列表，再遍历获取历史数据
    all_a_stocks_list = [
        ("000001", "平安银行"),
        ("600000", "浦发银行"),
        ("000002", "万科A"),
        ("600519", "贵州茅台"),
        ("000008", "神州高铁"),
        ("600009", "中国宝安"),
        ("600010", "包钢股份"),
        # ... 更多股票 ... 实际使用时应该是一个完整的A股列表
    ]
    # 筛选日期，可以设置为今天的日期或回测的某个日期
    analysis_date = datetime.now().strftime('%Y%m%d') # 例如：'20250520'

    # 批量获取所有股票的历史数据（使用您之前优化的`run`函数或类似逻辑）
    # 注意：fetch_all_stock_hist_data 应该返回 {("代码", "名称"): DataFrame} 格式
    logger.info(f"开始获取历史数据，共 {len(all_a_stocks_list)} 支股票...")
    stocks_historical_data = fetch_all_stock_hist_data(all_a_stocks_list, start_date="20240101") # 获取足够长的历史数据
    logger.info(f"历史数据获取完成，成功获取 {len(stocks_historical_data)} 支股票。")

    # 执行选股
    potential_buys = find_potential_stocks(stocks_historical_data, target_date=analysis_date)

    print("\n--- 策略筛选结果 ---")
    if potential_buys:
        print("以下股票符合入场条件，建议人工复核：")
        for code, name in potential_buys:
            print(f"- {name} ({code})")
            # 可以在这里打印更多细节，如最新收盘价、RSI等
            # if (code, name) in stocks_historical_data:
            #     latest = stocks_historical_data[(code, name)].iloc[-1]
            #     print(f"  最新收盘: {latest['收盘']:.2f}, RSI: {latest['RSI']:.2f}, MACD_DIF: {latest['MACD_DIF']:.2f}")

        # 示例：对符合条件的股票进行简单模拟
        # 实际交易中不会立即买入，还需要人工复核盘中情况
        # for code, name in potential_buys[:1]: # 仅模拟第一只
        #     if (code, name) in stocks_historical_data:
        #         stock_df = stocks_historical_data[(code, name)]
        #         # 假设买入价格为最新收盘价
        #         buy_price = stock_df.iloc[-1]['收盘']
        #         logger.info(f"模拟买入 {name} ({code}) @ {buy_price:.2f}")
        #         # 模拟交易需要后续几天的数据，这里简化处理，您可以构建更复杂的回测系统
        #         # trade_result = simulate_trade(stock_df.iloc[-5:], buy_price) # 假设只看未来5天
        #         # print(f"模拟交易结果: {trade_result}")

    else:
        print("今日无符合策略条件的股票。")