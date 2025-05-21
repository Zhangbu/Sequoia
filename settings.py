# settings.py
# -*- encoding: UTF-8 -*-
import yaml
import os
import logging
import traceback

logger = logging.getLogger(__name__) # Get the shared logger

_CONFIG = {} # Global configuration dictionary

def init():
    """
    Initializes global configuration by loading from config.yaml.
    """
    global _CONFIG

    # Define default configurations
    default_config = {
        'cron': False,
        'data_dir': "data",
        'end_date': None,
        'push': {
            'enable': False,
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
        'run_limit_up_backtest': True,
        'strategies': {
            '东方财富短线策略': {
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
                'min_turnover_rate': 5.0
            }
        }
    }

    def _deep_merge_dicts(source, destination):
        for key, value in source.items():
            if isinstance(value, dict) and key in destination and isinstance(destination[key], dict):
                destination[key] = _deep_merge_dicts(value, destination[key])
            else:
                destination[key] = value
        return destination

    root_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(root_dir, 'config.yaml')

    _CONFIG = default_config

    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as file:
                yaml_loaded_config = yaml.safe_load(file)
                if yaml_loaded_config:
                    _CONFIG = _deep_merge_dicts(yaml_loaded_config, _CONFIG)
                    logger.info(f"成功从 {config_file} 加载配置。", extra={'stock': 'NONE', 'strategy': '配置'})
                else:
                    logger.warning(f"{config_file} 文件为空，将使用默认配置。", extra={'stock': 'NONE', 'strategy': '配置'})
        except yaml.YAMLError as e:
            logger.error(f"解析 {config_file} 文件时出错: {e}\n{traceback.format_exc()}，将使用默认配置。", extra={'stock': 'NONE', 'strategy': '配置'})
        except Exception as e:
            logger.error(f"读取 {config_file} 时发生意外错误: {e}\n{traceback.format_exc()}，将使用默认配置。", extra={'stock': 'NONE', 'strategy': '配置'})
    else:
        logger.warning(f"未找到 {config_file} 文件，将使用默认配置。", extra={'stock': 'NONE', 'strategy': '配置'})

    _CONFIG['end_date'] = None # Always override end_date to None

    logger.debug(f"最终配置加载完成: {_CONFIG}", extra={'stock': 'NONE', 'strategy': '配置'})

def get_config():
    """Returns the globally loaded configuration dictionary."""
    return _CONFIG