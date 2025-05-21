# work_flow_new1.py
# -*- encoding: UTF-8 -*-

import data_fetcher_new
import settings
import akshare as ak
import push # Assuming push.py exists and handles sending messages
import logging
import datetime
import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from ratelimit import limits, sleep_and_retry
import sys
from tqdm import tqdm
import importlib # For dynamic strategy loading
from pathlib import Path # For strategy discovery
import traceback # For more detailed error logging

# Get the already configured logger instance from the root
logger = logging.getLogger(__name__) 

# --- Strategy Discovery Function (Phase 2, Item 5) ---
def discover_strategies():
    """
    Dynamically discovers strategy modules and their check_enter functions.
    Looks for check_enter (or check) functions in modules within specified strategy directories.
    """
    strategies = {}
    # Define your strategy directories. Ensure these paths are correct relative to work_flow_new1.py
    strategy_dirs = [Path("strategy"), Path("newStrategy")] 

    for s_dir in strategy_dirs:
        if not s_dir.exists() or not s_dir.is_dir():
            logger.warning(f"Strategy directory not found: {s_dir.resolve()}", extra={'stock': 'NONE', 'strategy': 'Discovery'})
            continue

        # Add strategy directory to sys.path temporarily to allow direct import
        # This is a common pattern for dynamic imports when modules are not in standard Python paths
        # Ensure it's removed after discovery or handled carefully.
        # A safer way is to use importlib.util.spec_from_file_location and importlib.util.module_from_spec
        # For simplicity here, we'll try adjusting sys.path
        if str(s_dir) not in sys.path:
            sys.path.insert(0, str(s_dir))

        for strategy_file in s_dir.glob("*.py"):
            if strategy_file.name == "__init__.py" or strategy_file.stem.startswith('.'):
                continue

            module_name = strategy_file.stem # e.g., "my_short_term_strategy"
            try:
                # Dynamically import the module. Assumes unique module names across dirs
                # If module names can clash (e.g. both strategy/enter.py and newStrategy/enter.py)
                # you'll need a more sophisticated naming or loading scheme.
                module = importlib.import_module(module_name)
                
                # Check for standard 'check_enter' or 'check' function
                if hasattr(module, 'check_enter') and callable(module.check_enter):
                    strategy_func = module.check_enter
                    # Attempt to get a readable name from module (e.g., from STRATEGY_NAME or module name)
                    strategy_display_name = getattr(module, 'STRATEGY_NAME', strategy_file.stem.replace('_', ' ').title())
                    strategies[strategy_display_name] = strategy_func
                    logger.info(f"Discovered strategy: '{strategy_display_name}' from {module_name}", extra={'stock': 'NONE', 'strategy': 'Discovery'})
                elif hasattr(module, 'check') and callable(module.check): # For old strategies using 'check'
                    strategy_func = module.check
                    strategy_display_name = getattr(module, 'STRATEGY_NAME', strategy_file.stem.replace('_', ' ').title())
                    strategies[strategy_display_name] = strategy_func
                    logger.info(f"Discovered legacy strategy: '{strategy_display_name}' from {module_name}", extra={'stock': 'NONE', 'strategy': 'Discovery'})
                else:
                    logger.debug(f"Module {module_name} does not contain a 'check_enter' or 'check' function. Skipping.", extra={'stock': 'NONE', 'strategy': 'Discovery'})

            except ImportError as ie:
                logger.error(f"Could not import strategy module {module_name} from {s_dir}: {ie}", extra={'stock': 'NONE', 'strategy': 'Discovery'})
                logger.debug(f"Sys.path for import issues: {sys.path}", extra={'stock': 'NONE', 'strategy': 'Discovery'})
            except Exception as e:
                logger.error(f"Error discovering strategy in {module_name} from {s_dir}: {e}\n{traceback.format_exc()}", extra={'stock': 'NONE', 'strategy': 'Discovery'})
        
        if str(s_dir) in sys.path: # Clean up sys.path
            sys.path.remove(str(s_dir))
    
    return strategies


def prepare():
    """Main function to prepare data, run strategies, and send notifications."""
    titleMsg = ""
    selected_limit_up_stocks = []
    logger.info("Process start", extra={'stock': 'NONE', 'strategy': 'NONE'})
    try:
        # Get market snapshot data
        all_data = ak.stock_zh_a_spot_em() 
        
        required_cols = {'代码', '名称', '总市值', '涨跌幅', '成交额', '换手率', '最新价'}
        if not required_cols.issubset(all_data.columns):
            missing_cols = required_cols - set(all_data.columns)
            logger.error(f"ak.stock_zh_a_spot_em() 返回的数据缺少必要列: {missing_cols}。请检查AKShare数据源。", extra={'stock': 'NONE', 'strategy': '数据获取'})
            return "", []

        # --- Step 1: Initial filtering for stock universe (your existing subset1) ---
        logger.info("正在应用初步筛选条件...")
        # Ensure numeric columns are actually numeric, coercing errors to NaN
        for col in ['总市值', '涨跌幅', '成交额', '换手率', '最新价']:
            if col in all_data.columns:
                all_data[col] = pd.to_numeric(all_data[col], errors='coerce')
        
        # Drop rows with NaN in critical columns after coercion to prevent filtering errors
        all_data.dropna(subset=['总市值', '涨跌幅', '成交额', '换手率', '最新价'], inplace=True)
        
        # Ensure '代码' column is string type for .startswith() and set operations
        all_data['代码'] = all_data['代码'].astype(str)
        # Ensure '名称' column is string type for .contains()
        all_data['名称'] = all_data['名称'].astype(str)

        subset1_df = all_data[
            (~all_data['代码'].str.startswith('688', na=False)) & # Exclude STAR market
            (~all_data['代码'].str.startswith('300', na=False)) & # Exclude ChiNext
            (~all_data['名称'].str.contains('ST', case=False, na=False)) & # Exclude ST stocks
            (all_data['总市值'] >= 10_000_000_000) & # Min market cap (100亿)
            (all_data['成交额'] >= 200_000_000) & # Min turnover amount (2亿)
            (all_data['换手率'] >= 1.0) & # Min turnover rate (1%)
            (all_data['换手率'] <= 25.0) & # Max turnover rate (25%)
            (all_data['最新价'] >= 5.0) & # Min price (5元)
            (all_data['涨跌幅'] > -3.0) # Avoid significant drops (跌幅小于3%)
        ].copy() # Use .copy() to avoid SettingWithCopyWarning
        
        initial_filtered_count = len(subset1_df)
        logger.info(f"初步筛选后，剩余 {initial_filtered_count} 只股票。", extra={'stock': 'NONE', 'strategy': '初步筛选'})

        final_stocks_df_for_processing = pd.DataFrame()
        
        # --- Step 2: Try to intersect with Top List (Dragon-Tiger List) stocks ---
        top_list_codes = set(settings.get_top_list()) # Convert to set for faster lookup
        
        if top_list_codes: # Only attempt intersection if top_list is not empty
            logger.info(f"已从 Settings 加载 {len(top_list_codes)} 个龙虎榜股票代码用于进一步筛选。", extra={'stock': 'NONE', 'strategy': '龙虎榜'})
            
            intersection_codes = set(subset1_df['代码'].tolist()).intersection(top_list_codes)
            
            if intersection_codes: # If intersection is not empty, use it
                final_stocks_df_for_processing = subset1_df[subset1_df['代码'].isin(intersection_codes)].copy()
                logger.info(f"初步筛选和龙虎榜交集后，剩余 {len(final_stocks_df_for_processing)} 只股票。", extra={'stock': 'NONE', 'strategy': '最终筛选'})
            else:
                logger.warning("初步筛选和龙虎榜股票的交集为空。", extra={'stock': 'NONE', 'strategy': '最终筛选'})
        else:
            logger.warning("龙虎榜数据为空或加载失败，将回退到初步筛选结果。", extra={'stock': 'NONE', 'strategy': '龙虎榜'})

        # --- Step 3: Fallback and further refine if needed ---
        if final_stocks_df_for_processing.empty: # If intersection was empty or top_list was empty
            logger.info("龙虎榜交集为空或龙虎榜数据缺失，将使用初步筛选结果。", extra={'stock': 'NONE', 'strategy': '最终筛选'})
            final_stocks_df_for_processing = subset1_df.copy() # Fallback to subset1_df

        # If the number of stocks is still too large, apply additional filtering
        TARGET_STOCK_COUNT = 60 # You can put this in settings.py if you want it configurable
        if len(final_stocks_df_for_processing) > TARGET_STOCK_COUNT:
            logger.info(f"筛选后股票数量 ({len(final_stocks_df_for_processing)}) 仍然过多，将进一步精简到 {TARGET_STOCK_COUNT} 只。", extra={'stock': 'NONE', 'strategy': '精简筛选'})
            
            # --- New additional screening logic ---
            # Prioritize by:
            # 1. High turnover amount (strongest indicator of market attention/liquidity)
            # 2. High turnover rate
            # 3. Small positive price change (implies strength but not overextended limit up)
            # 4. Market Cap (e.g., favor mid-caps more)

            # Sort by Turnover Amount (desc), then Turnover Rate (desc), then Market Cap (asc - slightly smaller caps might have more room to grow), then absolute change (closer to 0 is less volatile)
            # Using stable sort for consistent results with same values
            final_stocks_df_for_processing = final_stocks_df_for_processing.sort_values(
                by=['成交额', '换手率', '总市值', '涨跌幅'], 
                ascending=[False, False, True, False] # 成交额、换手率降序，总市值升序，涨跌幅降序
            )

            # Take the top N stocks
            final_stocks_df_for_processing = final_stocks_df_for_processing.head(TARGET_STOCK_COUNT)
            logger.info(f"经过精简筛选后，最终选择 {len(final_stocks_df_for_processing)} 只股票。", extra={'stock': 'NONE', 'strategy': '精简筛选'})

        # Convert to list of tuples (code, name) for data_fetcher_new.run
        stocks = [tuple(x) for x in final_stocks_df_for_processing[['代码', '名称']].values]
        
        if not stocks:
            logger.warning("最终筛选后，没有股票符合所有条件。程序将退出。", extra={'stock': 'NONE', 'strategy': '最终筛选'})
            if settings.get_config().get('push', {}).get('enable', False):
                push.strategy("没有股票符合所有筛选条件，程序退出。")
            return "", []

        logger.info(f"最终待获取和分析的股票数量为: {len(stocks)} 只。", extra={'stock': 'NONE', 'strategy': '最终筛选'})
        
        # Statistics should be based on all_data and the *final* list of stocks
        titleMsg = statistics(all_data, stocks) 

        # --- New: Dynamically discover strategies (Phase 2, Item 5) ---
        strategies = discover_strategies()
        if not strategies:
            logger.warning("No strategies were discovered. Please check strategy directories.", extra={'stock': 'NONE', 'strategy': 'Discovery'})
            if settings.get_config().get('push', {}).get('enable', False):
                push.strategy("Warning: No strategies were discovered. Check logs.")
            return "", []

        # Weekday specific handling (can be expanded)
        if datetime.datetime.now().weekday() == 0: # Monday
            pass # Currently no special handling, all strategies run

        titleMsg, selected_limit_up_stocks = process(stocks, strategies, titleMsg, selected_limit_up_stocks)

        logger.info(f"符合涨停板次日溢价策略的股票：{len(selected_limit_up_stocks)} 只", extra={'stock': 'NONE', 'strategy': '涨停板次日溢价'})
        
        # Access config for limit_up backtest run option
        if selected_limit_up_stocks and datetime.datetime.now().weekday() == 0 and settings.get_config().get('run_limit_up_backtest', True):
            logger.info("开始回测涨停板次日溢价策略", extra={'stock': 'NONE', 'strategy': '限价板回测'})
            # We need to explicitly import limit_up here for its backtest function
            # This import will only happen if the backtest is configured to run.
            try:
                import newStrategy.limit_up as limit_up 
                backtest_results = backtest_selected_stocks(selected_limit_up_stocks, limit_up) # Pass limit_up module
                titleMsg += format_backtest_results(backtest_results)
            except ImportError:
                logger.error("Could not import 'newStrategy.limit_up'. Backtest skipped.", extra={'stock': 'NONE', 'strategy': '限价板回测'})
            except Exception as e:
                logger.error(f"涨停板次日溢价回测失败: {e}\n{traceback.format_exc()}", extra={'stock': 'NONE', 'strategy': '限价板回测'})


        if titleMsg:
            max_length = 4000 # Max message length for some push services
            print(titleMsg) # Always print to console
            if settings.get_config().get('push', {}).get('enable', False):
                if len(titleMsg) > max_length:
                    chunks = [titleMsg[i:i+max_length] for i in range(0, len(titleMsg), max_length)]
                    for chunk in chunks:
                        push.strategy(chunk)
                else:
                    push.strategy(titleMsg)
        else:
            if settings.get_config().get('push', {}).get('enable', False):
                push.strategy("无符合条件的策略结果")

    except Exception as e:
        logger.exception(f"程序执行失败: {e}\n{traceback.format_exc()}", extra={'stock': 'NONE', 'strategy': 'NONE'})
        if settings.get_config().get('push', {}).get('enable', False):
            push.strategy(f"程序执行失败: {e}")

    logger.info("Process end", extra={'stock': 'NONE', 'strategy': 'NONE'})
    return titleMsg, selected_limit_up_stocks

@sleep_and_retry
@limits(calls=10, period=60) # Limit to 10 calls per 60 seconds
def call_strategy_check(stock_info, strategy_func, end_date):
    """Calls a single strategy's check_enter function for a given stock."""
    stock_code, stock_name, stock_data_df = stock_info
    
    try:
        # Phase 3, Item 7: Basic data validation before passing to strategy
        # This is an additional check, as data_fetcher_new also performs validation
        if stock_data_df.empty or not {'日期', '收盘', '开盘', '最高', '最低', '成交量', '成交额', '换手率'}.issubset(stock_data_df.columns):
            logger.warning(f"[{stock_name}({stock_code})]: 传入策略的数据不完整或为空，跳过。", extra={'stock': stock_code, 'strategy': strategy_func.__module__})
            return (stock_code, stock_name), False
        
        # Check for NaN in critical columns (e.g., '收盘', '成交量')
        for col in ['收盘', '成交量', '成交额', '换手率']:
            if col in stock_data_df.columns and stock_data_df[col].isnull().any():
                logger.warning(f"[{stock_name}({stock_code})]: 传入策略的数据在列 '{col}' 包含NaN值。可能影响策略判断。", extra={'stock': stock_code, 'strategy': strategy_func.__module__})
                # Option: You might choose to drop NaNs or fill them here, depending on strategy's tolerance
                # stock_data_df.dropna(subset=[col], inplace=True)
                # stock_data_df[col].fillna(method='ffill', inplace=True) # or .fillna(0, inplace=True)

        result = strategy_func((stock_code, stock_name), stock_data_df, end_date=end_date)
        return (stock_code, stock_name), result
    except Exception as e:
        logger.error(f"策略函数 {strategy_func.__name__} 执行失败 for {stock_name}({stock_code}): {e}\n{traceback.format_exc()}", extra={'stock': stock_code, 'strategy': strategy_func.__module__})
        return (stock_code, stock_name), False

def process(stocks, strategies, titleMsg, selected_limit_up_stocks):
    """Processes stocks through the discovered strategies."""
    try:
        logger.info(f"开始获取 {len(stocks)} 支股票的历史数据...", extra={'stock': 'NONE', 'strategy': '数据获取'})
        
        # Access data_dir from settings for caching
        data_cache_dir = settings.get_config().get('data_dir', 'stock_data_cache')
        stocks_data_dict = data_fetcher_new.run(stocks, cache_dir=data_cache_dir) 
        
        logger.info(f"历史数据获取完成，成功获取 {len(stocks_data_dict)} 支股票数据。", extra={'stock': 'NONE', 'strategy': '数据获取'})

        # Always use current date as the analysis end date
        end_date_str = datetime.datetime.now().strftime('%Y-%m-%d') 
        end_date_ts = pd.Timestamp(end_date_str)
        logger.info(f"当前分析日期为: {end_date_ts.strftime('%Y-%m-%d')} (基于实时时间)", extra={'stock': 'NONE', 'strategy': '日期'})
        
        # Access max_workers from settings if you add it to config.yaml
        max_workers = settings.get_config().get('max_workers', 5) # Default to 5 if not in config

        for strategy_name, strategy_func in strategies.items():
            # You might want to filter strategies based on settings here too
            # E.g., if strategy_name not in settings.get_config().get('enabled_strategies', strategies.keys()): continue
            
            logger.info(f"开始运行策略: {strategy_name}", extra={'stock': 'NONE', 'strategy': strategy_name})
            
            current_strategy_results = {}
            
            # Filter stocks_data_dict for those with actual data before processing
            processable_stocks_data = {cn: df for cn, df in stocks_data_dict.items() if not df.empty}
            if not processable_stocks_data:
                logger.warning(f"No processable stock data for strategy '{strategy_name}'. Skipping.", extra={'stock': 'NONE', 'strategy': strategy_name})
                continue

            with ThreadPoolExecutor(max_workers=max_workers) as executor: 
                future_to_stock_info = {
                    executor.submit(call_strategy_check, (code_name[0], code_name[1], data), strategy_func, end_date_ts): code_name
                    for code_name, data in processable_stocks_data.items()
                }
                
                for future in tqdm(as_completed(future_to_stock_info), 
                                   total=len(future_to_stock_info), 
                                   desc=f"Running {strategy_name.ljust(20)}", # Pad for better tqdm display
                                   unit="stock",
                                   file=sys.stdout # Ensure tqdm prints to stdout without interfering with logs
                                   ):
                    original_code_name_tuple = future_to_stock_info[future]
                    try:
                        (code, name), result = future.result()
                        if result:
                            current_strategy_results[f"{code} {name}"] = stocks_data_dict[(code, name)]
                            logger.info(f"股票 {name} ({code}) 符合策略 [{strategy_name}]", extra={'stock': code, 'strategy': strategy_name})
                    except Exception as exc:
                        logger.error(f"处理股票 {original_code_name_tuple[1]}({original_code_name_tuple[0]}) 时发生异常: {exc}\n{traceback.format_exc()}", extra={'stock': original_code_name_tuple[0], 'strategy': strategy_name})

            logger.info(f"策略 [{strategy_name}] 运行完成，找到 {len(current_strategy_results)} 支符合条件的股票。", extra={'stock': 'NONE', 'strategy': strategy_name})

            if len(current_strategy_results) > 0:
                titleMsg += format_strategy_result(strategy_name, current_strategy_results)
                # Ensure '涨停板次日溢价' is handled specifically for selection before backtest
                if strategy_name == '涨停板次日溢价':
                    selected_limit_up_stocks = build_selected_limit_up_stocks(current_strategy_results)

    except Exception as e:
        logger.exception(f"处理策略和股票数据过程中失败: {e}\n{traceback.format_exc()}", extra={'stock': 'NONE', 'strategy': 'NONE'})
    return titleMsg, selected_limit_up_stocks

def check_enter(end_date=None, strategy_fun=None):
    """
    Adapter function for legacy calls or specific backtesting.
    Dynamically loaded strategies should already match the (code_tuple, data_df, end_date) signature.
    """
    def end_date_filter(stock_info_tuple_for_adapter): 
        code_tuple, data = stock_info_tuple_for_adapter
        
        try:
            if end_date is not None:
                end_date_ts = pd.Timestamp(end_date)
                if not isinstance(data.iloc[0]['日期'], pd.Timestamp):
                    data['日期'] = pd.to_datetime(data['日期'])
                
                # Check if end_date is before first available data date
                first_date = data['日期'].min() if not data.empty else None
                if first_date and end_date_ts < first_date:
                    logger.debug(f"在 {end_date} 时还未上市或无数据。", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
                    return False
                
                data_filtered = data[data['日期'] <= end_date_ts].copy()
                if data_filtered.empty:
                    logger.debug(f"股票 {code_tuple[0]} 在 {end_date} 之前没有足够数据。", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
                    return False
            else:
                data_filtered = data.copy()

            # Ensure data_filtered is not empty before passing to strategy_fun
            if data_filtered.empty:
                logger.debug(f"股票 {code_tuple[0]} 过滤后数据为空。", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
                return False

            return strategy_fun(code_tuple, data_filtered, end_date=end_date)
        except ValueError as ve:
            logger.error(f"日期解析错误或数据问题 for {code_tuple[0]}: {ve}\n{traceback.format_exc()}", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
            return False
        except IndexError as ie:
            logger.error(f"数据索引错误 (可能数据太少) for {code_tuple[0]}: {ie}\n{traceback.format_exc()}", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
            return False
        except Exception as e:
            logger.error(f"执行策略时发生意外错误 for {code_tuple[0]}: {e}\n{traceback.format_exc()}", extra={'stock': code_tuple[0], 'strategy': 'UNKNOWN'})
            return False
    return end_date_filter


def format_strategy_result(strategy, results):
    """Formats the results of a single strategy for output."""
    stock_names_list = []
    for code_name_str in results.keys():
        parts = code_name_str.split(maxsplit=1)
        if len(parts) == 2:
            stock_names_list.append(f"{parts[1]}({parts[0]})")
        else:
            stock_names_list.append(code_name_str) # Fallback if format is unexpected

    return '\n**************"{0}"**************\n{1}\n'.format(strategy, ' '.join(stock_names_list))


def build_selected_limit_up_stocks(results):
    """Builds a list of stocks suitable for limit up backtesting."""
    selected_limit_up_stocks = []
    for code_name_str, data in results.items():
        try:
            if not isinstance(code_name_str, str) or not code_name_str.strip():
                logger.warning(f"无效的 code_name_str: {code_name_str}，跳过", extra={'stock': 'NONE', 'strategy': '涨停板回测'})
                continue
                
            parts = code_name_str.strip().split(maxsplit=1)
            if len(parts) < 2:
                logger.warning(f"code_name_str 格式错误: {code_name_str}，缺少名称部分，跳过", extra={'stock': 'NONE', 'strategy': '涨停板回测'})
                continue
                
            code, name = parts[0], parts[1]
            
            if not (code.isdigit() and len(code) == 6):
                logger.warning(f"股票代码格式错误: {code}，跳过", extra={'stock': code, 'strategy': '涨停板回测'})
                continue
                
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                logger.warning(f"股票 {code_name_str} 的数据为空，跳过", extra={'stock': code, 'strategy': '涨停板回测'})
                continue
                
            selected_limit_up_stocks.append((code, name, data))
            logger.info(f"添加涨停板回测股票: 代码={code}, 名称={name}, 数据行数={len(data)}", extra={'stock': code, 'strategy': '涨停板回测'})
            
        except Exception as e:
            logger.error(f"处理 {code_name_str} 失败: {e}\n{traceback.format_exc()}", extra={'stock': code_name_str.split()[0] if code_name_str else 'UNKNOWN', 'strategy': '涨停板回测'})
            continue
    
    logger.info(f"共筛选出 {len(selected_limit_up_stocks)} 只涨停板次日溢价股票用于回测", extra={'stock': 'NONE', 'strategy': '涨停板回测'})
    return selected_limit_up_stocks


def format_backtest_results(backtest_results):
    """Formats the overall and per-stock backtest results."""
    result = "\n************************ 涨停板次日溢价回测结果 ************************\n"
    # Phase 3, Item 8: Enhance reporting for backtest results
    
    # Calculate overall summary
    total_trades_overall = sum(stats.get('总交易次数', 0) for stats in backtest_results.values())
    total_profitable_trades_overall = sum(stats.get('盈利交易次数', 0) for stats in backtest_results.values())
    total_net_profit_overall = sum(stats.get('总收益', 0) for stats in backtest_results.values()) # Assuming '总收益' is provided

    overall_win_rate = (total_profitable_trades_overall / total_trades_overall) if total_trades_overall > 0 else 0
    overall_avg_return = (total_net_profit_overall / total_trades_overall) if total_trades_overall > 0 else 0 # Average return per trade

    result += f"整体回测摘要 (所有股票):\n"
    result += f"   总交易次数: {total_trades_overall}\n"
    result += f"   总盈利交易: {total_profitable_trades_overall}\n"
    result += f"   整体胜率: {overall_win_rate:.2%}\n"
    result += f"   整体平均每笔收益率: {overall_avg_return:.2%}\n"
    result += "--------------------------------------------------------\n"

    # Per-stock results
    for code_name_str, stats in backtest_results.items():
        if stats.get('总交易次数', 0) > 0: # Only show details if trades occurred
            result += f"\n股票: {code_name_str}\n"
            result += f"   总交易次数: {stats.get('总交易次数', 'N/A')}\n"
            result += f"   胜率: {stats.get('胜率', 0):.2%}\n"
            result += f"   平均收益率: {stats.get('平均收益率', 0):.2%}\n"
            result += f"   盈利交易次数: {stats.get('盈利交易次数', 'N/A')}\n"
            result += f"   亏损交易次数: {stats.get('亏损交易次数', 'N/A')}\n"
            result += f"   总收益: {stats.get('总收益', 0):.2%}\n"
            # Add more stats if backtest function provides them, e.g., max_drawdown, profit_factor
    return result

def backtest_selected_stocks(selected_stocks, limit_up_module):
    """Runs backtests for the selected limit up stocks."""
    backtest_results = {}
    # Use fixed dates for backtesting, or get from config if needed for flexibility
    start_date = '20240101' 
    end_date = datetime.datetime.now().strftime('%Y%m%d')
    
    logger.info(f"进行涨停板次日溢价回测，日期范围: {start_date} 至 {end_date}", extra={'stock': 'NONE', 'strategy': '限价板回测'})

    for symbol, name, data in tqdm(selected_stocks, desc="Backtesting limit up strategy", unit="stock", file=sys.stdout):
        code_name_str = f"{symbol} {name}"
        try:
            # Call the backtest function from the dynamically imported limit_up module
            # Ensure your limit_up.py's backtest function returns the expected stats dict
            stats = limit_up_module.backtest(code_name_str, data, start_date, end_date)
            backtest_results[code_name_str] = stats
            logger.info(f"回测 {code_name_str} 完成: 胜率={stats.get('胜率', 0):.2%}, 平均收益率={stats.get('平均收益率', 0):.2%}", extra={'stock': symbol, 'strategy': '限价板回测'})
            time.sleep(random.uniform(0.1, 0.5)) # Small random delay for politeness
        except Exception as e:
            logger.error(f"回测 {code_name_str} 失败: {e}\n{traceback.format_exc()}", extra={'stock': symbol, 'strategy': '限价板回测'})
    return backtest_results

def statistics(all_data, stocks):
    """Calculates and formats market statistics."""
    msg = ""
    try:
        # Phase 3, Item 8: Enhance market statistics
        total_stocks_in_market = len(all_data)
        total_filtered_stocks = len(stocks) # This now refers to the final, intersected list

        # Basic stats
        limitup = len(all_data.loc[(all_data['涨跌幅'] >= 9.5)])
        limitdown = len(all_data.loc[(all_data['涨跌幅'] <= -9.5)])
        up5 = len(all_data.loc[(all_data['涨跌幅'] >= 5)])
        down5 = len(all_data.loc[(all_data['涨跌幅'] <= -5)])
        
        # Additional market sentiment indicators
        # Calculate average change (simple average)
        avg_change = all_data['涨跌幅'].mean() if not all_data.empty else 0
        
        # Calculate number of rising/falling stocks
        rising_stocks = len(all_data[all_data['涨跌幅'] > 0])
        falling_stocks = len(all_data[all_data['涨跌幅'] < 0])
        unchanged_stocks = total_stocks_in_market - rising_stocks - falling_stocks

        # Calculate median change (less susceptible to extreme outliers)
        median_change = all_data['涨跌幅'].median() if not all_data.empty else 0

        # Calculate overall market turnover (sum of '成交额')
        total_market_turnover = all_data['成交额'].sum() / 1_000_000_000 if not all_data.empty else 0 # Convert to billions

        msg = "************************ 市场统计 ************************\n"
        # Updated statistics message to reflect the final filtered count
        msg += f"市场总股票数: {total_stocks_in_market} | 最终参与分析: {total_filtered_stocks}\n"
        msg += f"涨停数: {limitup} | 跌停数: {limitdown}\n"
        msg += f"涨幅>5%: {up5} | 跌幅<-5%: {down5}\n"
        msg += f"上涨家数: {rising_stocks} | 下跌家数: {falling_stocks} | 平盘家数: {unchanged_stocks}\n"
        msg += f"市场平均涨跌幅: {avg_change:.2f}% | 市场中位数涨跌幅: {median_change:.2f}%\n"
        msg += f"市场总成交额: {total_market_turnover:.2f} 亿\n"
        msg += "************************ 策略结果 ************************\n"
    except Exception as e:
        logger.error(f"统计数据失败: {e}\n{traceback.format_exc()}", extra={'stock': 'NONE', 'strategy': '统计'})
        msg = "************************ 市场统计 ************************\n统计数据失败\n"
    return msg