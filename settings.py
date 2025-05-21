# settings.py
# -*- encoding: UTF-8 -*-
import yaml
import os
import akshare as ak
import logging
import traceback # Added for more detailed error logging in init()
import pandas as pd

logger = logging.getLogger(__name__) # Get the shared logger

# Use a different name for the global configuration dictionary to avoid collision
_CONFIG = {}
_TOP_LIST = [] # Global variable for top_list

def init():
    """
    Initializes global configuration by loading from config.yaml
    and fetching top_list data.
    """
    global _CONFIG
    global _TOP_LIST

    # Define default configurations
    # These values will be used if they are not present in config.yaml
    default_config = {
        'cron': False,
        'data_dir': "data",
        'end_date': None, # This will always be overridden to current date in work_flow_new1
        'push': {
            'enable': False, # Default to False for safety
            'wxpusher_uid': "",
            'wxpusher_token': ""
        },
        'mail': {
            'enable': False,
            'smtp_server': "",
            'from_addr': '',
            'smtp_port': 465,
            'password': "",
            'to_addr': ""
        },
        'run_limit_up_backtest': True, # Default to true for the specific backtest in work_flow_new1
        'strategies': {
            '东方财富短线策略': { # Matches STRATEGY_NAME in strategy/my_short_term_strategy.py
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
            },
            '涨停板次日溢价': {
                'min_turnover_rate': 5.0 # Example setting
            }
            # Add other strategy default configurations here
        }
    }

    # Helper for deep merging dictionaries
    def _deep_merge_dicts(source, destination):
        for key, value in source.items():
            if isinstance(value, dict) and key in destination and isinstance(destination[key], dict):
                destination[key] = _deep_merge_dicts(value, destination[key])
            else:
                destination[key] = value
        return destination

    root_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(root_dir, 'config.yaml')

    # Start with default config
    _CONFIG = default_config

    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as file:
                yaml_loaded_config = yaml.safe_load(file)
                if yaml_loaded_config: # Ensure YAML isn't empty
                    # Merge YAML config over defaults
                    _CONFIG = _deep_merge_dicts(yaml_loaded_config, _CONFIG)
                    logger.info(f"成功从 {config_file} 加载配置。", extra={'stock': 'NONE', 'strategy': '配置'})
                else:
                    logger.warning(f"{config_file} 文件为空，将使用默认配置。", extra={'stock': 'NONE', 'strategy': '配置'})
        except yaml.YAMLError as e:
            logger.error(f"解析 {config_file} 文件时出错: {e}\n{traceback.format_exc()}，将使用默认配置。", extra={'stock': 'NONE', 'strategy': '配置'})
            # Fallback to default_config if parsing fails (already _CONFIG = default_config)
        except Exception as e:
            logger.error(f"读取 {config_file} 时发生意外错误: {e}\n{traceback.format_exc()}，将使用默认配置。", extra={'stock': 'NONE', 'strategy': '配置'})
            # Fallback to default_config if any other error occurs (already _CONFIG = default_config)
    else:
        logger.warning(f"未找到 {config_file} 文件，将使用默认配置。", extra={'stock': 'NONE', 'strategy': '配置'})

    # Important: Always override end_date to None as per our decision to use current date
    _CONFIG['end_date'] = None

    # Fetch top_list data using akshare
    try:
        df = ak.stock_lhb_stock_statistic_em(symbol="近三月")
        # Added type and existence checks for '买方机构次数' and '代码'
        if not df.empty and '买方机构次数' in df.columns and '代码' in df.columns:
            # Ensure '买方机构次数' is numeric
            df['买方机构次数'] = pd.to_numeric(df['买方机构次数'], errors='coerce').fillna(0)
            mask = (df['买方机构次数'] > 1)  # 机构买入次数大于1
            _TOP_LIST = df.loc[mask, '代码'].astype(str).tolist() # Ensure codes are strings
            logger.info(f"成功加载 {len(_TOP_LIST)} 个龙虎榜股票代码。", extra={'stock': 'NONE', 'strategy': '龙虎榜'})
        else:
            logger.warning("获取龙虎榜数据为空或缺少必要列。龙虎榜列表将为空。", extra={'stock': 'NONE', 'strategy': '龙虎榜'})

    except Exception as e:
        logger.error(f"加载龙虎榜数据失败: {e}\n{traceback.format_exc()}。龙虎榜列表将为空。", extra={'stock': 'NONE', 'strategy': '龙虎榜'})
        _TOP_LIST = [] # Ensure it's an empty list on failure

    logger.debug(f"最终配置加载完成: {_CONFIG}", extra={'stock': 'NONE', 'strategy': '配置'})

def get_config():
    """Returns the globally loaded configuration dictionary."""
    return _CONFIG

def get_top_list():
    """Returns the globally loaded top list of stock codes."""
    return _TOP_LIST