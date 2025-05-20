# strategy/my_short_term_strategy.py
import pandas as pd
import logging
import talib
import numpy as np

logger = logging.getLogger(__name__)

# --- 策略配置 ---
STRATEGY_CONFIG = {
    'min_avg_daily_turnover_amount': 100_000_000,
    'avg_turnover_days': 20,
    'min_listed_days': 60,
    'ma5_cross_ma10_period': 3,
    'close_above_ma20': True,
    'macd_gold_cross_within_days': 3,
    'macd_dif_above_dea_and_zero': True,
    'volume_ratio_to_5day_avg_min': 1.5,
    'volume_ratio_to_5day_avg_max': 2.5,
    'volume_ratio_to_5day_avg_days': 5,
    'boll_break_middle_band': True,
    'rsi_period': 6,
    'rsi_cross_30': True,
    'rsi_lower_limit': 30,
    'rsi_upper_limit': 70,
    'kdj_gold_cross': True,
    'kdj_j_upper_limit': 50,
    'kdj_j_lower_limit': 20,
    'min_daily_turnover_rate': 3.0,
    'max_daily_turnover_rate': 25.0,
}

def calculate_indicators(data: pd.DataFrame):
    data['日期'] = pd.to_datetime(data['日期'])
    data = data.sort_values(by='日期').reset_index(drop=True)

    close = data['收盘'].values
    high = data['最高'].values
    low = data['最低'].values
    volume = data['成交量'].values

    data['MA5'] = talib.SMA(close, timeperiod=5)
    data['MA10'] = talib.SMA(close, timeperiod=10)
    data['MA20'] = talib.SMA(close, timeperiod=20)

    data['MACD_DIF'], data['MACD_DEA'], data['MACD_HIST'] = talib.MACD(
        close, fastperiod=12, slowperiod=26, signalperiod=9
    )

    data['KDJ_K'], data['KDJ_D'] = talib.STOCH(
        high, low, close, fastk_period=9, slowk_period=3, slowd_period=3
    )
    data['KDJ_J'] = 3 * data['KDJ_K'] - 2 * data['KDJ_D']

    data['RSI'] = talib.RSI(close, timeperiod=STRATEGY_CONFIG['rsi_period'])

    data['BOLL_UPPER'], data['BOLL_MIDDLE'], data['BOLL_LOWER'] = talib.BBANDS(
        close, timeperiod=20, nbdevup=2, nbdevdn=2
    )

    data['VOL_MA5'] = talib.SMA(volume, timeperiod=STRATEGY_CONFIG['volume_ratio_to_5day_avg_days'])

    return data

def check_enter(stock_code_tuple, stock_data, end_date=None, config=STRATEGY_CONFIG):
    code, name = stock_code_tuple

    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty or \
       not {'日期', '收盘', '开盘', '最高', '最低', '成交量', '换手率', '成交额'}.issubset(stock_data.columns):
        logger.warning(f"[{name}({code})]: 数据格式错误或缺少必要列 (需要日期,收盘,开盘,最高,最低,成交量,换手率,成交额)。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    stock_data['日期'] = pd.to_datetime(stock_data['日期'])
    stock_data = stock_data.sort_values(by='日期').reset_index(drop=True)

    if end_date:
        end_date = pd.to_datetime(end_date)
        data = stock_data[stock_data['日期'] <= end_date].copy()
    else:
        data = stock_data.copy()

    min_required_len = max(
        26 + 1,
        20 + 1,
        config['volume_ratio_to_5day_avg_days'] + 1,
        config['avg_turnover_days'] + 1,
        config['min_listed_days']
    )
    if len(data) < min_required_len:
        logger.debug(f"[{name}({code})]: 数据长度不足 {min_required_len} 天 ({len(data)}天)，无法计算所有指标。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    data = calculate_indicators(data)

    latest_data = data.iloc[-1]
    prev_data = data.iloc[-2]

    if latest_data.isnull().any() or prev_data.isnull().any():
        logger.debug(f"[{name}({code})]: 最新数据或前一天数据包含NaN值，可能由于数据不足。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    # --- 基础筛选 ---
    listed_days = len(data)
    if listed_days < config['min_listed_days']:
        logger.debug(f"[{name}({code})]: 上市天数不足 {config['min_listed_days']} 天 ({listed_days}天)。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    avg_daily_turnover_amount = data['成交额'].iloc[-config['avg_turnover_days']:].mean()
    if avg_daily_turnover_amount < config['min_avg_daily_turnover_amount']:
        logger.debug(f"[{name}({code})]: 日均成交额 ({avg_daily_turnover_amount/1_000_000:.2f}亿) 低于 {config['min_avg_daily_turnover_amount']/1_000_000:.2f}亿。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    # --- 技术指标筛选 (核心) ---
    ma5_cross_ma10 = False
    for i in range(1, config['ma5_cross_ma10_period'] + 1):
        if data['MA5'].iloc[-i-1] <= data['MA10'].iloc[-i-1] and data['MA5'].iloc[-i] > data['MA10'].iloc[-i]:
            ma5_cross_ma10 = True
            break
    if not ma5_cross_ma10:
        logger.debug(f"[{name}({code})]: 近{config['ma5_cross_ma10_period']}天未发生5日均线上穿10日均线。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    if config['close_above_ma20'] and not (latest_data['收盘'] > latest_data['MA20']):
        logger.debug(f"[{name}({code})]: 股价 ({latest_data['收盘']:.2f}) 未高于20日均线 ({latest_data['MA20']:.2f})。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    macd_gold_cross = False
    for i in range(1, config['macd_gold_cross_within_days'] + 1):
        if data['MACD_DIF'].iloc[-i-1] <= data['MACD_DEA'].iloc[-i-1] and \
           data['MACD_DIF'].iloc[-i] > data['MACD_DEA'].iloc[-i]:
            macd_gold_cross = True
            break
    if not macd_gold_cross:
        logger.debug(f"[{name}({code})]: 近{config['macd_gold_cross_within_days']}天未发生MACD金叉。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False
    if config['macd_dif_above_dea_and_zero'] and not (latest_data['MACD_DIF'] > latest_data['MACD_DEA'] and latest_data['MACD_DIF'] > 0):
        logger.debug(f"[{name}({code})]: MACD不满足 DIF > DEA 且 DIF > 0。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        # return False # 如果这是“并且”关系，则取消注释。如果是“或者”关系，则此处不return

    if latest_data['VOL_MA5'] == 0:
         logger.debug(f"[{name}({code})]: 5日均量为零。", extra={'stock': code, 'strategy': '东方财富短线策略'})
         return False
    
    volume_ratio = latest_data['成交量'] / latest_data['VOL_MA5']
    if not (config['volume_ratio_to_5day_avg_min'] <= volume_ratio <= config['volume_ratio_to_5day_avg_max']):
        logger.debug(f"[{name}({code})]: 成交量 ({latest_data['成交量']:.0f}) 不满足放量条件 ({volume_ratio:.2f}倍5日均量)，要求 {config['volume_ratio_to_5day_avg_min']} - {config['volume_ratio_to_5day_avg_max']} 倍。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False
    
    if config['boll_break_middle_band'] and not (prev_data['收盘'] <= prev_data['BOLL_MIDDLE'] and latest_data['收盘'] > latest_data['BOLL_MIDDLE']):
        logger.debug(f"[{name}({code})]: 未上穿布林带中轨。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    if config['rsi_cross_30'] and not (data['RSI'].iloc[-2] <= config['rsi_lower_limit'] and latest_data['RSI'] > config['rsi_lower_limit']):
        logger.debug(f"[{name}({code})]: RSI ({latest_data['RSI']:.2f}) 未上穿 {config['rsi_lower_limit']}。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False
    if not (config['rsi_lower_limit'] <= latest_data['RSI'] <= config['rsi_upper_limit']):
        logger.debug(f"[{name}({code})]: RSI ({latest_data['RSI']:.2f}) 不在 {config['rsi_lower_limit']}-{config['rsi_upper_limit']} 区间。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    if config['kdj_gold_cross'] and not (prev_data['KDJ_K'] <= prev_data['KDJ_D'] and latest_data['KDJ_K'] > latest_data['KDJ_D']):
        logger.debug(f"[{name}({code})]: KDJ未金叉。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False
    if not (config['kdj_j_lower_limit'] <= latest_data['KDJ_J'] < config['kdj_j_upper_limit']):
        logger.debug(f"[{name}({code})]: KDJ J值 ({latest_data['KDJ_J']:.2f}) 不在 {config['kdj_j_lower_limit']}-{config['kdj_j_upper_limit']} 区间。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    # --- 换手率 ---
    if not (config['min_daily_turnover_rate'] <= latest_data['换手率'] <= config['max_daily_turnover_rate']):
        logger.debug(f"[{name}({code})]: 换手率 ({latest_data['换手率']:.2f}%) 不在 {config['min_daily_turnover_rate']}-{config['max_daily_turnover_rate']}% 区间。", extra={'stock': code, 'strategy': '东方财富短线策略'})
        return False

    logger.info(f"[{name}({code})]: ✨ 股票符合东方财富App短线策略所有入场条件！", extra={'stock': code, 'strategy': '东方财富短线策略'})
    return True