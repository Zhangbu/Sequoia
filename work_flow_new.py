import pandas as pd
import logging
import talib
import numpy as np

logger = logging.getLogger(__name__)

# --- 策略配置 ---
# 注意：这里的一些参数是示例值，您需要根据实际回测和市场情况进行调整
STRATEGY_CONFIG = {
    # 基础筛选 (注意：部分筛选在主流程中已完成，这里作为二次校验或补充)
    'min_avg_daily_turnover_amount': 100_000_000, # 日均成交额 > 1亿元
    'avg_turnover_days': 20, # 计算日均成交额的天数
    'min_listed_days': 60, # 上市天数 > 60天
    # 'exclude_st': True, # 剔除ST股 (已在主流程中处理)
    # 'exclude_star_st': True, # 剔除*ST股 (已在主流程中处理)

    # 技术指标筛选
    'ma5_cross_ma10_period': 3, # 5日均线金叉10日均线，在近几天内发生
    'close_above_ma20': True, # 股价高于20日均线
    'macd_gold_cross_within_days': 3, # MACD金叉在近几天内发生
    'macd_dif_above_dea_and_zero': True, # DIF > DEA 且 DIF > 0 (用于强势区判断)
    'volume_ratio_to_5day_avg_min': 1.5, # 当日/昨日成交量 > 5日均量 1.5倍
    'volume_ratio_to_5day_avg_max': 2.5, # 限制最大放量，避免异常拉升
    'volume_ratio_to_5day_avg_days': 5, # 计算5日均量天数
    'boll_break_middle_band': True, # 股价上穿布林带中轨
    'rsi_period': 6, # RSI计算周期
    'rsi_cross_30': True, # RSI上穿30 (超卖区拐头向上)
    'rsi_lower_limit': 30, # RSI下限
    'rsi_upper_limit': 70, # RSI上限（避免过热）
    'kdj_gold_cross': True, # KDJ金叉
    'kdj_j_upper_limit': 50, # KDJ J值上限，避免高位金叉
    'kdj_j_lower_limit': 20, # KDJ J值下限，避免超跌反弹过弱

    # 基本面与市场情绪 (此处为简化处理，需结合外部数据或人工判断)
    # 'min_circulating_market_cap': 5_000_000_000, # 流通市值 > 50亿 (已在主流程中处理)
    # 'max_circulating_market_cap': 500_000_000_000, # 流通市值 < 5000亿 (已在主流程中处理)
    'min_daily_turnover_rate': 3.0, # 每日换手率 > 3%
    'max_daily_turnover_rate': 25.0, # 每日换手率 < 25% (避免过度炒作)
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

def check_enter(stock_code, stock_data, end_date=None, config=STRATEGY_CONFIG):
    """
    检查单个stock是否符合东方财富App短线交易策略的入场条件。
    :param stock_code: stock代码和名称的元组，例如 ("000001", "平安银行")
    :param stock_data: 包含历史数据的Pandas DataFrame
    :param end_date: 用于回测的当前日期，默认为None表示最新数据日期
    :param config: 策略配置字典
    :return: True/False
    """
    code, name = stock_code # 解包stock代码和名称

    # 1. 数据校验与日期过滤
    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty or \
       not {'日期', '收盘', '开盘', '最高', '最低', '成交量', '换手率', '成交额'}.issubset(stock_data.columns):
        logger.warning(f"[{name}({code})]: 数据格式错误或缺少必要列 (需要日期,收盘,开盘,最高,最低,成交量,换手率,成交额)。")
        return False

    # 确保日期列为datetime类型并排序
    stock_data['日期'] = pd.to_datetime(stock_data['日期'])
    stock_data = stock_data.sort_values(by='日期').reset_index(drop=True)

    if end_date:
        end_date = pd.to_datetime(end_date)
        data = stock_data[stock_data['日期'] <= end_date].copy()
    else:
        data = stock_data.copy()

    # 确保数据长度足够计算指标
    # MACD(26), KDJ(9), RSI(14), BOLL(20), MA20(20), VOL_MA5(5)
    # 取最长的周期 + 额外的天数来确保所有指标有值
    min_required_len = max(
        26 + 1, # MACD requires 26 periods + 1 for current day
        20 + 1, # BOLL, MA20
        config['volume_ratio_to_5day_avg_days'] + 1,
        config['avg_turnover_days'] + 1,
        config['min_listed_days'] # 上市天数直接检查data的长度
    )
    if len(data) < min_required_len:
        logger.debug(f"[{name}({code})]: 数据长度不足 {min_required_len} 天 ({len(data)}天)，无法计算所有指标。")
        return False

    # 计算技术指标
    data = calculate_indicators(data)

    # 取最新一天的数据
    latest_data = data.iloc[-1]
    prev_data = data.iloc[-2] # 前一天数据

    # 检查是否有NaN值，可能是数据不够
    if latest_data.isnull().any() or prev_data.isnull().any():
        logger.debug(f"[{name}({code})]: 最新数据或前一天数据包含NaN值，可能由于数据不足。")
        return False

    # --- 基础筛选 (部分已在主流程 `prepare` 中完成，这里作为二次校验或补充) ---
    # 上市天数
    listed_days = len(data) # 假设提供的数据是从上市以来
    if listed_days < config['min_listed_days']:
        logger.debug(f"[{name}({code})]: 上市天数不足 {config['min_listed_days']} 天 ({listed_days}天)。")
        return False

    # 日均成交额 (取近N天)
    avg_daily_turnover_amount = data['成交额'].iloc[-config['avg_turnover_days']:].mean()
    if avg_daily_turnover_amount < config['min_avg_daily_turnover_amount']:
        logger.debug(f"[{name}({code})]: 日均成交额 ({avg_daily_turnover_amount/1_000_000:.2f}亿) 低于 {config['min_avg_daily_turnover_amount']/1_000_000:.2f}亿。")
        return False

    # --- 技术指标筛选 (核心) ---
    # 均线形态 (5日均线 上穿 10日均线)
    ma5_cross_ma10 = False
    for i in range(1, config['ma5_cross_ma10_period'] + 1):
        if data['MA5'].iloc[-i-1] <= data['MA10'].iloc[-i-1] and data['MA5'].iloc[-i] > data['MA10'].iloc[-i]:
            ma5_cross_ma10 = True
            break
    if not ma5_cross_ma10:
        logger.debug(f"[{name}({code})]: 近{config['ma5_cross_ma10_period']}天未发生5日均线上穿10日均线。")
        return False

    # 股价 高于 20日均线
    if config['close_above_ma20'] and not (latest_data['收盘'] > latest_data['MA20']):
        logger.debug(f"[{name}({code})]: 股价 ({latest_data['收盘']:.2f}) 未高于20日均线 ({latest_data['MA20']:.2f})。")
        return False

    # MACD指标 (金叉或强势区)
    macd_gold_cross = False
    for i in range(1, config['macd_gold_cross_within_days'] + 1):
        if data['MACD_DIF'].iloc[-i-1] <= data['MACD_DEA'].iloc[-i-1] and \
           data['MACD_DIF'].iloc[-i] > data['MACD_DEA'].iloc[-i]:
            macd_gold_cross = True
            break
    if not macd_gold_cross:
        logger.debug(f"[{name}({code})]: 近{config['macd_gold_cross_within_days']}天未发生MACD金叉。")
        return False
    # MACD DIF > DEA 且 DIF > 0 (强势区) - 这是“或者”的关系，所以放在金叉后，如果金叉了，再看是否在强势区
    if config['macd_dif_above_dea_and_zero'] and not (latest_data['MACD_DIF'] > latest_data['MACD_DEA'] and latest_data['MACD_DIF'] > 0):
        logger.debug(f"[{name}({code})]: MACD不满足 DIF > DEA 且 DIF > 0。")
        # return False # 如果这是“并且”关系，则取消注释。如果是“或者”关系，则此处不return

    # 成交量 (放量：当日成交量 > 5日均量 1.5倍 且 < 2.5倍)
    if latest_data['VOL_MA5'] == 0: # 避免除以零
         logger.debug(f"[{name}({code})]: 5日均量为零。")
         return False
    
    volume_ratio = latest_data['成交量'] / latest_data['VOL_MA5']
    if not (config['volume_ratio_to_5day_avg_min'] <= volume_ratio <= config['volume_ratio_to_5day_avg_max']):
        logger.debug(f"[{name}({code})]: 成交量 ({latest_data['成交量']:.0f}) 不满足放量条件 ({volume_ratio:.2f}倍5日均量)，要求 {config['volume_ratio_to_5day_avg_min']} - {config['volume_ratio_to_5day_avg_max']} 倍。")
        return False
    
    # 布林带 (上穿中轨)
    if config['boll_break_middle_band'] and not (prev_data['收盘'] <= prev_data['BOLL_MIDDLE'] and latest_data['收盘'] > latest_data['BOLL_MIDDLE']):
        logger.debug(f"[{name}({code})]: 未上穿布林带中轨。")
        return False

    # RSI (上穿30且在合理区间)
    if config['rsi_cross_30'] and not (data['RSI'].iloc[-2] <= config['rsi_lower_limit'] and latest_data['RSI'] > config['rsi_lower_limit']):
        logger.debug(f"[{name}({code})]: RSI ({latest_data['RSI']:.2f}) 未上穿 {config['rsi_lower_limit']}。")
        return False
    if not (config['rsi_lower_limit'] <= latest_data['RSI'] <= config['rsi_upper_limit']):
        logger.debug(f"[{name}({code})]: RSI ({latest_data['RSI']:.2f}) 不在 {config['rsi_lower_limit']}-{config['rsi_upper_limit']} 区间。")
        return False


    # KDJ (金叉且J值不过高也不过低)
    if config['kdj_gold_cross'] and not (prev_data['KDJ_K'] <= prev_data['KDJ_D'] and latest_data['KDJ_K'] > latest_data['KDJ_D']):
        logger.debug(f"[{name}({code})]: KDJ未金叉。")
        return False
    if not (config['kdj_j_lower_limit'] <= latest_data['KDJ_J'] < config['kdj_j_upper_limit']):
        logger.debug(f"[{name}({code})]: KDJ J值 ({latest_data['KDJ_J']:.2f}) 不在 {config['kdj_j_lower_limit']}-{config['kdj_j_upper_limit']} 区间。")
        return False

    # --- 基本面与市场情绪 (此处简化，只看换手率) ---
    # 换手率
    if not (config['min_daily_turnover_rate'] <= latest_data['换手率'] <= config['max_daily_turnover_rate']):
        logger.debug(f"[{name}({code})]: 换手率 ({latest_data['换手率']:.2f}%) 不在 {config['min_daily_turnover_rate']}-{config['max_daily_turnover_rate']}% 区间。")
        return False

    logger.info(f"[{name}({code})]: ✨ stock符合东方财富App短线策略所有入场条件！")
    return True