"""
統一官方數據收集器
整合TWSE和TPEX官方API，提供完整的台股數據收集功能
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from .config import (
    PROJECT_ROOT,
    RAW_PRICES_FILE, 
    DATA_COLLECTION_CONFIG
)
from .official_twse_fetcher import OfficialTWSEFetcher
from .official_tpex_fetcher import OfficialTPEXFetcher
from .data_formatter import StockDataFormatter

# 設置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UnifiedOfficialFetcher:
    """統一官方數據收集器"""
    
    def __init__(self):
        self.stock_list = DATA_COLLECTION_CONFIG["STOCK_LIST"]
        self.lookback_days = DATA_COLLECTION_CONFIG["LOOKBACK_DAYS"]
        
        # 初始化TWSE和TPEX收集器
        self.twse_fetcher = OfficialTWSEFetcher()
        self.tpex_fetcher = OfficialTPEXFetcher()
        
        # 初始化數據格式化器
        self.formatter = StockDataFormatter()
        
        # 分類股票
        self.tse_stocks, self.tpex_stocks = self._classify_stocks()
        
        logger.info("統一官方數據收集器初始化完成")
        logger.info(f"上市股票 (TSE): {len(self.tse_stocks)} 支")
        logger.info(f"上櫃股票 (TPEX): {len(self.tpex_stocks)} 支")
    
    def _classify_stocks(self) -> Tuple[List[str], List[str]]:
        """
        將股票分類為上市和上櫃
        基於 stocks_config.txt 配置文件進行分類
        """
        # 讀取配置文件
        config_file = PROJECT_ROOT / "stocks_config.txt"
        tse_stocks = []
        tpex_stocks = []
        
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    # 跳過註釋和空行
                    if not line or line.startswith('#'):
                        continue
                    
                    try:
                        # 解析格式：股票代碼,股票名稱,市場類型
                        parts = line.split(',')
                        if len(parts) >= 3:
                            stock_code = parts[0].strip()
                            market_type = parts[2].strip().upper()
                            
                            # 只處理配置中的股票
                            if stock_code in self.stock_list:
                                if market_type == 'TSE':
                                    tse_stocks.append(stock_code)
                                elif market_type == 'TPEX':
                                    tpex_stocks.append(stock_code)
                                else:
                                    # 未知市場類型，默認使用TSE
                                    logger.warning(f"未知市場類型 '{market_type}' 股票 {stock_code}，使用TSE")
                                    tse_stocks.append(stock_code)
                    except Exception as e:
                        logger.warning(f"解析配置行失敗: {line}, 錯誤: {e}")
        else:
            logger.warning(f"配置文件不存在: {config_file}")
            # 如果配置文件不存在，所有股票默認使用TSE
            tse_stocks = self.stock_list.copy()
        
        # 檢查是否有股票沒有在配置文件中
        configured_stocks = set(tse_stocks + tpex_stocks)
        missing_stocks = set(self.stock_list) - configured_stocks
        if missing_stocks:
            logger.warning(f"以下股票未在配置文件中找到，默認使用TSE: {missing_stocks}")
            tse_stocks.extend(missing_stocks)
        
        logger.info(f"從配置文件分類結果: TSE={len(tse_stocks)} 支, TPEX={len(tpex_stocks)} 支")
        return tse_stocks, tpex_stocks
    
    def fetch_stock_data(self, stock_code: str, days: int = None) -> pd.DataFrame:
        """
        獲取單一股票數據（自動選擇TWSE或TPEX）
        
        Args:
            stock_code: 股票代碼
            days: 回看天數
            
        Returns:
            包含股價數據的 DataFrame
        """
        if days is None:
            days = self.lookback_days
        
        logger.info(f"開始獲取股票 {stock_code} 的官方數據 (回看 {days} 天)")
        
        # 根據股票分類選擇數據源
        if stock_code in self.tse_stocks:
            logger.info(f"使用TWSE API獲取股票 {stock_code}")
            return self.twse_fetcher.fetch_stock_historical_data(stock_code, days)
        elif stock_code in self.tpex_stocks:
            logger.info(f"使用TPEX API獲取股票 {stock_code}")
            return self.tpex_fetcher.fetch_stock_historical_data(stock_code, days)
        else:
            # 嘗試TWSE，如果失敗再嘗試TPEX
            logger.info(f"嘗試TWSE API獲取股票 {stock_code}")
            df = self.twse_fetcher.fetch_stock_historical_data(stock_code, days)
            
            if df is None or df.empty:
                logger.info(f"TWSE失敗，嘗試TPEX API獲取股票 {stock_code}")
                df = self.tpex_fetcher.fetch_stock_historical_data(stock_code, days)
            
            return df
    
    def fetch_incremental_data(self, existing_df: pd.DataFrame = None, 
                              days_to_fetch: int = 7) -> pd.DataFrame:
        """
        增量獲取數據（只獲取缺失或過期的數據）
        
        Args:
            existing_df: 現有數據DataFrame
            days_to_fetch: 需要獲取的天數
            
        Returns:
            新增的數據DataFrame
        """
        logger.info("開始增量數據收集...")
        
        # 使用所有股票列表
        target_stocks = self.stock_list
        
        logger.info(f"增量更新股票: {len(target_stocks)} 支")
        
        all_new_data = []
        
        for stock_code in target_stocks:
            logger.info(f"檢查股票 {stock_code} 的數據更新需求...")
            
            try:
                # 獲取最近幾天的數據
                df = self.fetch_stock_data(stock_code, days_to_fetch)
                
                if df is not None and not df.empty:
                    # 如果有現有數據，檢查是否需要更新
                    if existing_df is not None:
                        existing_stock_data = existing_df[existing_df['stock_code'] == stock_code]
                        
                        if not existing_stock_data.empty:
                            # 檢查最新數據日期
                            latest_existing_date = existing_stock_data['date'].max()
                            latest_new_date = df['date'].max()
                            
                            if latest_new_date > latest_existing_date:
                                logger.info(f"  ✓ 股票 {stock_code} 有新數據需要更新")
                                all_new_data.append(df)
                            else:
                                logger.info(f"  - 股票 {stock_code} 數據已是最新")
                        else:
                            logger.info(f"  ✓ 股票 {stock_code} 首次獲取數據")
                            all_new_data.append(df)
                    else:
                        logger.info(f"  ✓ 股票 {stock_code} 獲取數據")
                        all_new_data.append(df)
                else:
                    logger.warning(f"  ✗ 股票 {stock_code} 未獲取到數據")
                    
            except Exception as e:
                logger.error(f"  ✗ 獲取股票 {stock_code} 時發生錯誤: {e}")
        
        if not all_new_data:
            logger.info("沒有新數據需要更新")
            return pd.DataFrame()
        
        # 合併新數據
        new_df = pd.concat(all_new_data, ignore_index=True)
        logger.info(f"總共獲取 {len(new_df)} 筆新數據")
        
        return new_df
    
    def fetch_all_stocks_by_date_range(self, start_date: str, end_date: str = None, save_to_file: bool = True) -> Dict[str, pd.DataFrame]:
        """
        按日期範圍抓取所有股票數據
        
        Args:
            start_date: 開始日期 (YYYY-MM-DD)
            end_date: 結束日期 (YYYY-MM-DD)，None 表示到最新日期
            save_to_file: 是否保存到文件
            
        Returns:
            股票數據字典 {股票代碼: DataFrame}
        """
        logger.info(f"開始按日期範圍抓取所有股票數據: {start_date} 到 {end_date or '最新日期'}")
        
        # 創建日期範圍專用目錄
        date_range_dir = None
        if save_to_file:
            from datetime import datetime
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
            
            # 創建目錄名稱：date_range_YYYYMMDD_YYYYMMDD
            start_str = start_dt.strftime('%Y%m%d')
            end_str = end_dt.strftime('%Y%m%d')
            date_range_dir = PROJECT_ROOT / "data" / f"date_range_{start_str}_{end_str}"
            date_range_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"數據將保存到: {date_range_dir}")
        
        all_stocks_data = {}
        
        # 抓取上市股票數據
        if self.tse_stocks:
            logger.info(f"獲取 {len(self.tse_stocks)} 支上市股票數據")
            for stock_code in self.tse_stocks:
                try:
                    logger.info(f"抓取上市股票 {stock_code} 的數據...")
                    stock_data = self.twse_fetcher.fetch_stock_data_by_date_range(stock_code, start_date, end_date)
                    
                    if stock_data is not None and not stock_data.empty:
                        all_stocks_data[stock_code] = stock_data
                        
                        # 格式化並保存數據
                        if save_to_file and date_range_dir:
                            # 使用日期範圍專用目錄保存
                            success = self.formatter.format_to_standard_csv(stock_data, stock_code, output_dir=date_range_dir)
                            if success:
                                logger.info(f"成功保存股票 {stock_code} 的數據到 {date_range_dir}")
                            else:
                                logger.warning(f"保存股票 {stock_code} 的數據失敗")
                    else:
                        logger.warning(f"未能獲取股票 {stock_code} 的數據")
                        
                except Exception as e:
                    logger.error(f"抓取股票 {stock_code} 時發生錯誤: {e}")
                    continue
        
        # 抓取上櫃股票數據
        if self.tpex_stocks:
            logger.info(f"獲取 {len(self.tpex_stocks)} 支上櫃股票數據")
            for stock_code in self.tpex_stocks:
                try:
                    logger.info(f"抓取上櫃股票 {stock_code} 的數據...")
                    stock_data = self.tpex_fetcher.fetch_stock_data_by_date_range(stock_code, start_date, end_date)
                    
                    if stock_data is not None and not stock_data.empty:
                        all_stocks_data[stock_code] = stock_data
                        
                        # 格式化並保存數據
                        if save_to_file and date_range_dir:
                            # 使用日期範圍專用目錄保存
                            success = self.formatter.format_to_standard_csv(stock_data, stock_code, output_dir=date_range_dir)
                            if success:
                                logger.info(f"成功保存股票 {stock_code} 的數據到 {date_range_dir}")
                            else:
                                logger.warning(f"保存股票 {stock_code} 的數據失敗")
                    else:
                        logger.warning(f"未能獲取股票 {stock_code} 的數據")
                        
                except Exception as e:
                    logger.error(f"抓取股票 {stock_code} 時發生錯誤: {e}")
                    continue
        
        logger.info(f"按日期範圍抓取完成，總共 {len(all_stocks_data)} 支股票")
        return all_stocks_data

    def _detect_market_type(self, stock_code: str) -> str:
        """
        自動檢測股票市場類型
        
        Args:
            stock_code: 股票代碼
            
        Returns:
            'TSE' 或 'TPEX' 或 'UNKNOWN'
        """
        try:
            import requests
            
            # 先嘗試 TWSE (上市)
            try:
                url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                params = {
                    'response': 'json',
                    'date': '20241001',  # 使用一個固定的日期
                    'stockNo': stock_code
                }
                
                response = requests.get(url, params=params, timeout=10)
                data = response.json()
                
                # 檢查回應是否包含數據
                if 'data' in data and data['data']:
                    return 'TSE'
                    
            except Exception:
                pass
            
            # 再嘗試 TPEX (上櫃)
            try:
                url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
                params = {
                    'code': stock_code,
                    'date': '2024/10/01',
                    'id': '',
                    'response': 'utf-8'
                }
                
                response = requests.get(url, params=params, timeout=10)
                
                # 檢查回應是否包含數據（TPEX 返回 HTML 格式）
                if response.status_code == 200 and 'table' in response.text.lower():
                    return 'TPEX'
                    
            except Exception:
                pass
            
            return 'UNKNOWN'
            
        except Exception as e:
            logger.error(f"檢測股票 {stock_code} 市場類型時發生錯誤: {e}")
            return 'UNKNOWN'

    def fetch_stock_data_by_date_range(self, stock_code: str, start_date: str, end_date: str = None, save_to_file: bool = True) -> pd.DataFrame:
        """
        按日期範圍抓取指定股票數據
        
        Args:
            stock_code: 股票代碼
            start_date: 開始日期 (YYYY-MM-DD)
            end_date: 結束日期 (YYYY-MM-DD)，None 表示到最新日期
            save_to_file: 是否保存到文件
            
        Returns:
            股票數據DataFrame
        """
        logger.info(f"抓取股票 {stock_code} 的數據 (日期範圍: {start_date} 到 {end_date or '最新日期'})...")
        
        try:
            # 創建日期範圍專用目錄
            date_range_dir = None
            if save_to_file:
                from datetime import datetime
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
                
                # 創建目錄名稱：date_range_YYYYMMDD_YYYYMMDD
                start_str = start_dt.strftime('%Y%m%d')
                end_str = end_dt.strftime('%Y%m%d')
                date_range_dir = PROJECT_ROOT / "data" / f"date_range_{start_str}_{end_str}"
                date_range_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"數據將保存到: {date_range_dir}")
            
            stock_data = None
            
            # 判斷是上市還是上櫃股票
            if stock_code in self.tse_stocks:
                logger.info(f"股票 {stock_code} 是上市股票，使用 TWSE 抓取器")
                stock_data = self.twse_fetcher.fetch_stock_data_by_date_range(stock_code, start_date, end_date)
            elif stock_code in self.tpex_stocks:
                logger.info(f"股票 {stock_code} 是上櫃股票，使用 TPEX 抓取器")
                stock_data = self.tpex_fetcher.fetch_stock_data_by_date_range(stock_code, start_date, end_date)
            else:
                # 股票不在配置清單中，嘗試自動判斷市場類型
                logger.info(f"股票 {stock_code} 不在配置清單中，嘗試自動判斷市場類型...")
                market_type = self._detect_market_type(stock_code)
                if market_type == 'TSE':
                    logger.info(f"檢測到股票 {stock_code} 是上市股票，使用 TWSE 抓取器")
                    stock_data = self.twse_fetcher.fetch_stock_data_by_date_range(stock_code, start_date, end_date)
                elif market_type == 'TPEX':
                    logger.info(f"檢測到股票 {stock_code} 是上櫃股票，使用 TPEX 抓取器")
                    stock_data = self.tpex_fetcher.fetch_stock_data_by_date_range(stock_code, start_date, end_date)
                else:
                    logger.error(f"無法判斷股票 {stock_code} 的市場類型，請手動添加到配置清單")
                    return pd.DataFrame()
            
            if stock_data is not None and not stock_data.empty:
                logger.info(f"成功抓取股票 {stock_code} 的數據: {len(stock_data)} 筆")
                
                # 格式化並保存數據
                if save_to_file and date_range_dir:
                    success = self.formatter.format_to_standard_csv(stock_data, stock_code, output_dir=date_range_dir)
                    if success:
                        logger.info(f"成功保存股票 {stock_code} 的數據到 {date_range_dir}")
                    else:
                        logger.warning(f"保存股票 {stock_code} 的數據失敗")
                
                return stock_data
            else:
                logger.warning(f"未能獲取股票 {stock_code} 的數據")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"抓取股票 {stock_code} 時發生錯誤: {e}")
            return pd.DataFrame()

    def fetch_stock_data_backward_days(self, stock_code: str, days: int, save_to_file: bool = True) -> pd.DataFrame:
        """
        往回爬指定天數的股票數據
        
        Args:
            stock_code: 股票代碼
            days: 往回爬的天數
            save_to_file: 是否保存到文件
            
        Returns:
            股票數據DataFrame
        """
        logger.info(f"抓取股票 {stock_code} 的數據 (往回 {days} 天到最新日期)...")
        
        try:
            stock_data = None
            
            # 判斷是上市還是上櫃股票
            if stock_code in self.tse_stocks:
                logger.info(f"股票 {stock_code} 是上市股票，使用 TWSE 抓取器")
                stock_data = self.twse_fetcher.fetch_stock_historical_data(stock_code, days)
            elif stock_code in self.tpex_stocks:
                logger.info(f"股票 {stock_code} 是上櫃股票，使用 TPEX 抓取器")
                stock_data = self.tpex_fetcher.fetch_stock_historical_data(stock_code, days)
            else:
                # 股票不在配置清單中，嘗試自動判斷市場類型
                logger.info(f"股票 {stock_code} 不在配置清單中，嘗試自動判斷市場類型...")
                market_type = self._detect_market_type(stock_code)
                if market_type == 'TSE':
                    logger.info(f"檢測到股票 {stock_code} 是上市股票，使用 TWSE 抓取器")
                    stock_data = self.twse_fetcher.fetch_stock_historical_data(stock_code, days)
                elif market_type == 'TPEX':
                    logger.info(f"檢測到股票 {stock_code} 是上櫃股票，使用 TPEX 抓取器")
                    stock_data = self.tpex_fetcher.fetch_stock_historical_data(stock_code, days)
                else:
                    logger.error(f"無法判斷股票 {stock_code} 的市場類型，請手動添加到配置清單")
                    return pd.DataFrame()
            
            if stock_data is not None and not stock_data.empty:
                logger.info(f"成功抓取股票 {stock_code} 的數據: {len(stock_data)} 筆")
                
                # 格式化並保存數據
                if save_to_file:
                    success = self.formatter.format_to_standard_csv(stock_data, stock_code)
                    if success:
                        logger.info(f"成功保存股票 {stock_code} 的數據")
                    else:
                        logger.warning(f"保存股票 {stock_code} 的數據失敗")
                
                return stock_data
            else:
                logger.warning(f"未能獲取股票 {stock_code} 的數據")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"抓取股票 {stock_code} 時發生錯誤: {e}")
            return pd.DataFrame()

    def fetch_all_stocks(self, save_to_file: bool = True) -> Dict[str, pd.DataFrame]:
        """
        獲取所有股票的數據並保存為獨立CSV檔案
        
        Args:
            save_to_file: 是否保存到檔案
            
        Returns:
            股票數據字典 {股票代碼: DataFrame}
        """
        all_stocks_data = {}
        success_count = 0
        
        logger.info(f"開始獲取 {len(self.stock_list)} 支股票的官方數據...")
        
        # 獲取TWSE股票數據
        if self.tse_stocks:
            logger.info(f"獲取 {len(self.tse_stocks)} 支上市股票數據...")
            for i, stock_code in enumerate(self.tse_stocks):
                logger.debug(f"正在獲取TWSE股票 {stock_code} ({i+1}/{len(self.tse_stocks)})")
                
                try:
                    df = self.twse_fetcher.fetch_stock_historical_data(stock_code, self.lookback_days)
                    if df is not None and not df.empty:
                        all_stocks_data[stock_code] = df
                        
                        # 格式化並保存為獨立CSV檔案
                        if save_to_file:
                            success = self.formatter.format_to_standard_csv(df, stock_code)
                            if success:
                                success_count += 1
                                logger.debug(f"  ✓ 成功獲取並保存 {len(df)} 筆數據")
                            else:
                                logger.warning(f"  ✗ 數據獲取成功但保存失敗")
                        else:
                            success_count += 1
                            logger.debug(f"  ✓ 成功獲取 {len(df)} 筆數據")
                    else:
                        logger.warning(f"  ✗ 未獲取到數據")
                except Exception as e:
                    logger.error(f"  ✗ 獲取TWSE股票 {stock_code} 時發生錯誤: {e}")
        
        # 獲取TPEX股票數據
        if self.tpex_stocks:
            logger.info(f"獲取 {len(self.tpex_stocks)} 支上櫃股票數據...")
            for i, stock_code in enumerate(self.tpex_stocks):
                logger.debug(f"正在獲取TPEX股票 {stock_code} ({i+1}/{len(self.tpex_stocks)})")
                
                try:
                    df = self.tpex_fetcher.fetch_stock_historical_data(stock_code, self.lookback_days)
                    if df is not None and not df.empty:
                        all_stocks_data[stock_code] = df
                        
                        # 格式化並保存為獨立CSV檔案
                        if save_to_file:
                            success = self.formatter.format_to_standard_csv(df, stock_code)
                            if success:
                                success_count += 1
                                logger.debug(f"  ✓ 成功獲取並保存 {len(df)} 筆數據")
                            else:
                                logger.warning(f"  ✗ 數據獲取成功但保存失敗")
                        else:
                            success_count += 1
                            logger.debug(f"  ✓ 成功獲取 {len(df)} 筆數據")
                    else:
                        logger.warning(f"  ✗ 未獲取到數據")
                except Exception as e:
                    logger.error(f"  ✗ 獲取TPEX股票 {stock_code} 時發生錯誤: {e}")
        
        logger.info(f"股票數據收集完成，成功處理 {success_count} 支股票")
        logger.info(f"每支股票數據已保存為獨立CSV檔案到: data/")
        
        return all_stocks_data
    
    def _save_data(self, df: pd.DataFrame) -> None:
        """
        保存數據到檔案
        
        Args:
            df: 要保存的DataFrame
        """
        try:
            # 確保目錄存在
            RAW_PRICES_FILE.parent.mkdir(parents=True, exist_ok=True)
            
            # 保存為CSV
            df.to_csv(RAW_PRICES_FILE, index=False, encoding='utf-8-sig')
            
            logger.info(f"官方數據已保存到: {RAW_PRICES_FILE}")
            
        except Exception as e:
            logger.error(f"保存官方數據時發生錯誤: {e}")
    
    def test_connections(self) -> Dict[str, bool]:
        """
        測試與TWSE和TPEX API的連接
        
        Returns:
            連接測試結果字典
        """
        results = {}
        
        # 測試TWSE連接
        logger.info("測試TWSE API連接...")
        results['twse'] = self.twse_fetcher.test_connection()
        
        # 測試TPEX連接
        logger.info("測試TPEX API連接...")
        results['tpex'] = self.tpex_fetcher.test_connection()
        
        # 總結
        logger.info("API連接測試結果:")
        logger.info(f"  TWSE: {'✓ 成功' if results['twse'] else '✗ 失敗'}")
        logger.info(f"  TPEX: {'✓ 成功' if results['tpex'] else '✗ 失敗'}")
        
        return results
    
    def get_data_source_info(self) -> Dict[str, any]:
        """
        獲取數據源信息
        
        Returns:
            數據源信息字典
        """
        return {
            'twse_stocks': self.tse_stocks,
            'tpex_stocks': self.tpex_stocks,
            'total_stocks': len(self.stock_list),
            'lookback_days': self.lookback_days,
            'api_endpoints': {
                'twse': self.twse_fetcher.stock_day_url,
                'tpex': 'CSV files (manual data)'
            }
        }
    
    def check_stocks_need_update(self) -> dict:
        """
        檢查哪些股票需要更新
        
        Returns:
            需要更新的股票清單
        """
        from datetime import datetime, timedelta
        from .config import get_stocks_by_market
        
        need_update = {
            'tse_stocks': [],
            'tpex_stocks': [],
            'missing_stocks': []
        }
        
        # 重新載入股票清單（檢查是否有新增的股票）
        current_stocks = get_stocks_by_market()
        current_tse_stocks = current_stocks.get('TSE', [])
        current_tpex_stocks = current_stocks.get('TPEX', [])
        all_current_stocks = current_tse_stocks + current_tpex_stocks
        
        # 檢查格式化文件是否存在
        data_dir = PROJECT_ROOT / "data"
        if not data_dir.exists():
            data_dir.mkdir(parents=True, exist_ok=True)
        
        # 檢查所有股票（包括新增的）
        for stock_code in all_current_stocks:
            csv_file = data_dir / f"{stock_code}.csv"
            
            if not csv_file.exists():
                # 文件不存在，需要更新
                need_update['missing_stocks'].append(stock_code)
                if stock_code in current_tse_stocks:
                    need_update['tse_stocks'].append(stock_code)
                else:
                    need_update['tpex_stocks'].append(stock_code)
            else:
                # 檢查文件是否過舊
                try:
                    df = pd.read_csv(csv_file)
                    if not df.empty and '交易日期' in df.columns:
                        # 轉換民國年日期為datetime
                        latest_date_str = df['交易日期'].iloc[-1]
                        try:
                            # 民國年轉西元年
                            roc_year, month, day = latest_date_str.split('/')
                            gregorian_year = int(roc_year) + 1911
                            latest_date = datetime(int(gregorian_year), int(month), int(day))
                            
                            # 檢查是否超過1天（但對於TPEX股票，允許更長的間隔）
                            days_old = (datetime.now() - latest_date).days
                            if stock_code in current_tpex_stocks:
                                # TPEX股票允許7天間隔，避免頻繁更新
                                threshold_days = 7
                            else:
                                # TSE股票允許3天間隔，考慮週末和假日
                                threshold_days = 3
                            
                            logger.debug(f"股票 {stock_code}: 最新日期 {latest_date.date()}, 距今 {days_old} 天, 閾值 {threshold_days} 天")
                            
                            if days_old > threshold_days:
                                if stock_code in current_tse_stocks:
                                    need_update['tse_stocks'].append(stock_code)
                                else:
                                    need_update['tpex_stocks'].append(stock_code)
                        except Exception:
                            # 日期解析失敗，需要更新
                            if stock_code in current_tse_stocks:
                                need_update['tse_stocks'].append(stock_code)
                            else:
                                need_update['tpex_stocks'].append(stock_code)
                    else:
                        # 文件為空，需要更新
                        if stock_code in current_tse_stocks:
                            need_update['tse_stocks'].append(stock_code)
                        else:
                            need_update['tpex_stocks'].append(stock_code)
                except Exception:
                    # 讀取失敗，需要更新
                    if stock_code in current_tse_stocks:
                        need_update['tse_stocks'].append(stock_code)
                    else:
                        need_update['tpex_stocks'].append(stock_code)
        
        return need_update
    
    def fetch_and_format_incremental(self, days: int = None) -> dict:
        """
        增量更新股票數據（只更新需要的股票）
        
        Args:
            days: 回看天數，預設使用配置中的值
            
        Returns:
            格式化結果統計
        """
        if days is None:
            days = self.lookback_days
        
        # 檢查需要更新的股票
        need_update = self.check_stocks_need_update()
        
        total_need_update = len(need_update['tse_stocks']) + len(need_update['tpex_stocks'])
        
        if total_need_update == 0:
            logger.info("所有股票數據都是最新的，無需更新")
            return {
                'success': [],
                'failed': [],
                'total': len(self.stock_list),
                'skipped': len(self.stock_list)
            }
        
        logger.info(f"發現 {total_need_update} 支股票需要更新")
        logger.info(f"TSE股票: {len(need_update['tse_stocks'])} 支")
        logger.info(f"TPEX股票: {len(need_update['tpex_stocks'])} 支")
        if need_update['tse_stocks']:
            logger.debug(f"需要更新的TSE股票: {need_update['tse_stocks']}")
        if need_update['tpex_stocks']:
            logger.debug(f"需要更新的TPEX股票: {need_update['tpex_stocks']}")
        
        # 獲取需要更新的股票數據
        all_data = {}
        
        # 獲取TSE股票數據（強制更新模式）
        for stock_code in need_update['tse_stocks']:
            try:
                logger.info(f"強制更新TSE股票 {stock_code}")
                # 使用強制更新模式
                df = self.twse_fetcher.fetch_stock_historical_data(stock_code, days, force_update=True)
                # 將所有結果（包括空DataFrame）傳遞給格式化器，讓它決定成功/失敗
                all_data[stock_code] = df if df is not None else pd.DataFrame()
            except Exception as e:
                logger.error(f"獲取TSE股票 {stock_code} 數據失敗: {e}")
                all_data[stock_code] = pd.DataFrame()
        
        # 獲取TPEX股票數據（強制更新模式）
        for stock_code in need_update['tpex_stocks']:
            try:
                logger.info(f"強制更新TPEX股票 {stock_code}")
                # 使用強制更新模式
                df = self.tpex_fetcher.fetch_stock_historical_data(stock_code, days, force_update=True)
                # 將所有結果（包括空DataFrame）傳遞給格式化器，讓它決定成功/失敗
                all_data[stock_code] = df if df is not None else pd.DataFrame()
            except Exception as e:
                logger.error(f"獲取TPEX股票 {stock_code} 數據失敗: {e}")
                all_data[stock_code] = pd.DataFrame()
        
        successful_count = sum(1 for df in all_data.values() if not df.empty)
        logger.info(f"嘗試獲取 {len(all_data)} 支股票的數據，成功 {successful_count} 支")
        
        # 批量格式化
        format_results = self.formatter.batch_format_stocks(all_data)
        
        # 添加跳過的股票數量
        format_results['skipped'] = len(self.stock_list) - total_need_update
        
        return format_results
    
    def fetch_and_format_all_stocks(self, days: int = None) -> dict:
        """
        獲取所有股票數據並格式化為標準CSV格式
        
        Args:
            days: 回看天數，預設使用配置中的值
            
        Returns:
            格式化結果統計
        """
        if days is None:
            days = self.lookback_days
        
        logger.info(f"開始獲取所有股票數據並格式化 (回看 {days} 天)...")
        
        # 獲取所有股票數據
        all_data = {}
        
        # 獲取TSE股票數據
        for stock_code in self.tse_stocks:
            try:
                df = self.twse_fetcher.fetch_stock_historical_data(stock_code, days)
                # 將所有結果（包括空DataFrame）傳遞給格式化器，讓它決定成功/失敗
                all_data[stock_code] = df if df is not None else pd.DataFrame()
            except Exception as e:
                logger.error(f"獲取TSE股票 {stock_code} 數據失敗: {e}")
                all_data[stock_code] = pd.DataFrame()
        
        # 獲取TPEX股票數據
        for stock_code in self.tpex_stocks:
            try:
                df = self.tpex_fetcher.fetch_stock_historical_data(stock_code, days)
                # 將所有結果（包括空DataFrame）傳遞給格式化器，讓它決定成功/失敗
                all_data[stock_code] = df if df is not None else pd.DataFrame()
            except Exception as e:
                logger.error(f"獲取TPEX股票 {stock_code} 數據失敗: {e}")
                all_data[stock_code] = pd.DataFrame()
        
        successful_count = sum(1 for df in all_data.values() if not df.empty)
        logger.info(f"嘗試獲取 {len(all_data)} 支股票的數據，成功 {successful_count} 支")
        
        # 批量格式化
        format_results = self.formatter.batch_format_stocks(all_data)
        
        return format_results
    
    def fetch_and_format_stock(self, stock_code: str, days: int = None) -> bool:
        """
        獲取單個股票數據並格式化為標準CSV格式
        
        Args:
            stock_code: 股票代碼
            days: 回看天數，預設使用配置中的值
            
        Returns:
            是否成功
        """
        if days is None:
            days = self.lookback_days
        
        logger.info(f"開始獲取股票 {stock_code} 數據並格式化 (回看 {days} 天)...")
        
        try:
            # 獲取數據
            if stock_code in self.tse_stocks:
                df = self.twse_fetcher.fetch_stock_historical_data(stock_code, days)
            elif stock_code in self.tpex_stocks:
                df = self.tpex_fetcher.fetch_stock_historical_data(stock_code, days)
            else:
                logger.error(f"股票 {stock_code} 不在配置清單中")
                return False
            
            if df is None or df.empty:
                logger.warning(f"股票 {stock_code} 沒有數據")
                return False
            
            # 格式化並保存
            success = self.formatter.format_to_standard_csv(df, stock_code)
            
            if success:
                logger.info(f"成功格式化股票 {stock_code} 的數據")
            else:
                logger.error(f"格式化股票 {stock_code} 數據失敗")
            
            return success
            
        except Exception as e:
            logger.error(f"處理股票 {stock_code} 時發生錯誤: {e}")
            return False


def main():
    """主函數 - 用於測試統一官方數據收集器"""
    fetcher = UnifiedOfficialFetcher()
    
    # 測試連接
    connection_results = fetcher.test_connections()
    
    if any(connection_results.values()):
        logger.info("開始測試統一數據收集...")
        
        # 獲取數據源信息
        info = fetcher.get_data_source_info()
        logger.info(f"數據源信息: {info}")
        
        # 測試獲取股票數據
        test_stocks = fetcher.stock_list[:2]  # 只測試前2支
        logger.info(f"測試獲取股票: {test_stocks}")
        
        for stock_code in test_stocks:
            df = fetcher.fetch_stock_data(stock_code, days=30)
            
            if df is not None and not df.empty:
                logger.info(f"股票 {stock_code} 測試成功！獲取到 {len(df)} 筆數據")
                logger.info(f"數據範圍: {df['date'].min()} 到 {df['date'].max()}")
            else:
                logger.error(f"股票 {stock_code} 測試失敗 - 沒有獲取到數據")
    else:
        logger.error("所有API連接測試失敗，無法進行數據收集測試")


if __name__ == "__main__":
    main()
