"""
台灣證券交易所(TWSE)官方數據收集器
直接從TWSE官方API獲取上市股票數據
"""

import pandas as pd
import requests
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import json
from io import StringIO
import re

from .config import (
    PROJECT_ROOT,
    RAW_PRICES_FILE, 
    DATA_COLLECTION_CONFIG,
    API_CONFIG
)

# 設置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OfficialTWSEFetcher:
    """台灣證券交易所官方數據收集器"""
    
    def __init__(self):
        self.stock_list = DATA_COLLECTION_CONFIG["STOCK_LIST"]
        self.lookback_days = DATA_COLLECTION_CONFIG["LOOKBACK_DAYS"]
        
        # TWSE官方API配置
        self.base_url = "https://www.twse.com.tw"
        self.stock_day_url = f"{self.base_url}/exchangeReport/STOCK_DAY"
        
        # API限制：每5秒最多3次請求
        self.request_delay = 2.0
        self.max_retries = 3
        
        # 設置請求頭
        self.session = self._setup_session()
        
        logger.info("TWSE官方數據收集器初始化完成")
    
    def _setup_session(self) -> requests.Session:
        """設置HTTP會話"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-TW,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Referer': 'https://www.twse.com.tw/',
            'Connection': 'keep-alive',
        })
        return session
    
    def fetch_stock_monthly_data(self, stock_code: str, year: int, month: int) -> pd.DataFrame:
        """
        獲取單一股票指定月份的數據
        
        Args:
            stock_code: 股票代碼
            year: 年份
            month: 月份
            
        Returns:
            包含股價數據的 DataFrame
        """
        # 構建日期參數（TWSE API需要完整的日期，但只關注年月）
        date_str = f"{year}{month:02d}01"
        
        params = {
            'response': 'json',
            'date': date_str,
            'stockNo': stock_code
        }
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"正在獲取 {stock_code} {year}-{month:02d} 的數據 (嘗試 {attempt + 1}/{self.max_retries})")
                
                response = self.session.get(
                    self.stock_day_url, 
                    params=params, 
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # 檢查API回應狀態
                    if data.get('stat') == 'OK':
                        # 解析數據
                        df = self._parse_twse_response(data, stock_code)
                        if not df.empty:
                            logger.debug(f"  ✓ 成功獲取 {len(df)} 筆數據")
                            return df
                        else:
                            logger.warning(f"  ⚠ 數據為空")
                    else:
                        logger.warning(f"  ⚠ API回應錯誤: {data.get('stat', 'Unknown')}")
                        if 'message' in data:
                            logger.warning(f"    錯誤訊息: {data['message']}")
                
                else:
                    logger.warning(f"  ⚠ HTTP錯誤: {response.status_code}")
                
                # 重試前等待
                if attempt < self.max_retries - 1:
                    time.sleep(self.request_delay)
                    
            except Exception as e:
                logger.error(f"  ✗ 獲取數據時發生錯誤: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.request_delay)
        
        logger.error(f"  ✗ 獲取 {stock_code} {year}-{month:02d} 數據失敗")
        return pd.DataFrame()
    
    def _parse_twse_response(self, data: dict, stock_code: str) -> pd.DataFrame:
        """
        解析TWSE API回應數據
        
        Args:
            data: API回應的JSON數據
            stock_code: 股票代碼
            
        Returns:
            解析後的DataFrame
        """
        try:
            # 獲取數據欄位
            fields = data.get('fields', [])
            data_rows = data.get('data', [])
            
            if not fields or not data_rows:
                return pd.DataFrame()
            
            # 創建DataFrame
            df = pd.DataFrame(data_rows, columns=fields)
            
            # 數據清理和轉換
            df = self._clean_twse_data(df, stock_code)
            
            return df
            
        except Exception as e:
            logger.error(f"解析TWSE數據時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _convert_existing_csv_to_standard_format(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        將現有CSV數據轉換為標準格式
        
        Args:
            df: 現有CSV數據
            stock_code: 股票代碼
            
        Returns:
            標準格式的DataFrame
        """
        try:
            # 添加股票代碼
            df = df.copy()
            df['stock_code'] = stock_code
            
            # 轉換日期格式（從民國年轉換為西元年）
            if '交易日期' in df.columns:
                def convert_roc_date(date_str):
                    try:
                        if isinstance(date_str, str) and '/' in date_str:
                            parts = date_str.split('/')
                            if len(parts) == 3:
                                roc_year = int(parts[0])
                                gregorian_year = roc_year + 1911
                                return f"{gregorian_year}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                    except:
                        pass
                    return None
                
                df['date'] = df['交易日期'].apply(convert_roc_date)
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])
            
            # 轉換數值欄位
            numeric_columns = ['成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '成交筆數']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 重新命名欄位為標準格式
            df = df.rename(columns={
                '成交股數': 'volume',
                '成交金額': 'turnover', 
                '開盤價': 'open',
                '最高價': 'high',
                '最低價': 'low',
                '收盤價': 'close',
                '成交筆數': 'transactions'
            })
            
            # 計算漲跌價差
            if 'close' in df.columns:
                df['change'] = df['close'].diff()
            
            # 按日期排序
            df = df.sort_values('date').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"轉換現有CSV數據格式時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _clean_twse_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        清理和轉換TWSE數據
        
        Args:
            df: 原始DataFrame
            stock_code: 股票代碼
            
        Returns:
            清理後的DataFrame
        """
        try:
            # 複製DataFrame避免修改原始數據
            df = df.copy()
            
            # 添加股票代碼
            df['stock_code'] = stock_code
            
            # 重命名欄位（根據TWSE API的實際欄位名稱調整）
            column_mapping = {
                '日期': 'date',
                '成交股數': 'volume',
                '成交金額': 'turnover',
                '開盤價': 'open',
                '最高價': 'high',
                '最低價': 'low',
                '收盤價': 'close',
                '漲跌價差': 'change',
                '成交筆數': 'transactions'
            }
            
            # 重命名存在的欄位
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            # 轉換日期格式 - TWSE使用民國年格式 (113/11/01)
            if 'date' in df.columns:
                def convert_twse_date(date_str):
                    try:
                        # 民國年轉西元年
                        year, month, day = date_str.split('/')
                        year = int(year) + 1911  # 民國年轉西元年
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    except:
                        return None
                
                df['date'] = df['date'].apply(convert_twse_date)
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            # 轉換數值欄位
            numeric_columns = ['volume', 'turnover', 'open', 'high', 'low', 'close', 'change', 'transactions']
            for col in numeric_columns:
                if col in df.columns:
                    # 移除逗號並轉換為數值
                    df[col] = df[col].astype(str).str.replace(',', '').str.replace('--', '0')
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 移除無效數據
            df = df.dropna(subset=['date', 'close'])
            
            # 按日期排序
            df = df.sort_values('date').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"清理TWSE數據時發生錯誤: {e}")
            return pd.DataFrame()
    
    def fetch_stock_data_by_date_range(self, stock_code: str, start_date: str, end_date: str = None) -> pd.DataFrame:
        """
        按日期範圍獲取股票歷史數據
        
        Args:
            stock_code: 股票代碼
            start_date: 開始日期 (YYYY-MM-DD)
            end_date: 結束日期 (YYYY-MM-DD)，None 表示到最新日期
            
        Returns:
            股票數據DataFrame
        """
        try:
            from datetime import datetime
            
            # 解析日期
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
            
            logger.info(f"按日期範圍抓取股票 {stock_code} 的數據: {start_date} 到 {end_date or '最新日期'}")
            
            # 直接調用 TWSE API 抓取指定日期範圍的數據
            return self._fetch_from_twse_api_by_date_range(stock_code, start_dt, end_dt)
            
        except Exception as e:
            logger.error(f"按日期範圍抓取股票 {stock_code} 數據時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _fetch_from_twse_api_by_date_range(self, stock_code: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        按日期範圍從 TWSE API 抓取股票數據
        
        Args:
            stock_code: 股票代碼
            start_date: 開始日期
            end_date: 結束日期
            
        Returns:
            股票數據DataFrame
        """
        try:
            import requests
            import json
            
            logger.info(f"從 TWSE API 抓取股票 {stock_code} 的日期範圍數據: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
            
            all_data = []
            
            # 計算需要抓取的月份範圍
            current_date = start_date.replace(day=1)  # 從開始月份的第一天開始
            end_month = end_date.replace(day=1)
            
            while current_date <= end_month:
                year = current_date.year
                month = current_date.month
                
                try:
                    # 轉換為民國年格式
                    roc_year = year - 1911
                    
                    # 構建 TWSE API URL
                    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                    
                    # 請求參數
                    params = {
                        'response': 'json',
                        'date': f'{year}{month:02d}01',
                        'stockNo': stock_code
                    }
                    
                    logger.info(f"嘗試獲取 {year}年{month}月 的數據...")
                    
                    # 發送請求
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    }
                    
                    response = requests.get(url, params=params, headers=headers, timeout=30)
                    response.raise_for_status()
                    
                    # 解析 JSON 回應
                    data = response.json()
                    
                    if 'data' in data and data['data']:
                        # 轉換為 DataFrame
                        df = pd.DataFrame(data['data'], columns=data['fields'])
                        
                        # 清理數據
                        cleaned_df = self._clean_twse_data(df, stock_code)
                        
                        if not cleaned_df.empty:
                            all_data.append(cleaned_df)
                            logger.info(f"找到 {year}年{month}月 的股票數據")
                        else:
                            logger.warning(f"清理後 {year}年{month}月 的數據為空")
                    else:
                        logger.warning(f"未找到 {year}年{month}月 的股票數據")
                        
                except Exception as e:
                    logger.error(f"獲取 {year}年{month}月 數據時發生錯誤: {e}")
                
                # 移到下一個月
                if month == 12:
                    current_date = current_date.replace(year=year + 1, month=1)
                else:
                    current_date = current_date.replace(month=month + 1)
            
            if not all_data:
                logger.warning(f"未能獲取股票 {stock_code} 的任何數據")
                return pd.DataFrame()
            
            # 合併所有數據
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 過濾日期範圍
            if 'date' in combined_df.columns:
                # 轉換日期格式進行比較
                combined_df['_temp_date'] = pd.to_datetime(combined_df['date'])
                
                # 過濾日期範圍
                mask = (combined_df['_temp_date'] >= start_date) & (combined_df['_temp_date'] <= end_date)
                filtered_df = combined_df[mask].copy()
                filtered_df = filtered_df.drop('_temp_date', axis=1)
                
                logger.info(f"成功從 TWSE API 抓取股票 {stock_code} 的日期範圍數據: {len(filtered_df)} 筆")
                return filtered_df
            else:
                logger.warning(f"數據中缺少日期欄位")
                return combined_df
                
        except Exception as e:
            logger.error(f"從 TWSE API 抓取股票 {stock_code} 日期範圍數據時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _calculate_months_to_download(self, target_start_date: datetime, earliest_existing_date: datetime, existing_dates: set) -> list:
        """
        計算需要下載的月份列表
        
        Args:
            target_start_date: 目標開始日期
            earliest_existing_date: 現有數據的最早日期
            existing_dates: 現有日期集合
            
        Returns:
            需要下載的月份列表 [(year, month), ...]
        """
        months_to_download = []
        current_date = target_start_date.replace(day=1)
        end_date = earliest_existing_date.replace(day=1)
        
        while current_date < end_date:
            year = current_date.year
            month = current_date.month
            
            # 檢查該月是否有現有數據
            month_data = [date_obj for date_obj in existing_dates 
                         if date_obj.year == year and date_obj.month == month]
            
            if not month_data:
                months_to_download.append((year, month))
                logger.info(f"需要下載 {year}年{month}月 的歷史數據")
            
            # 移到下個月
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        return months_to_download

    def fetch_stock_historical_data(self, stock_code: str, days: int = None, force_update: bool = False) -> pd.DataFrame:
        """
        獲取股票歷史數據（增量下載）
        
        Args:
            stock_code: 股票代碼
            days: 回看天數
            force_update: 是否強制更新，跳過增量檢查
            
        Returns:
            歷史數據DataFrame
        """
        if days is None:
            days = self.lookback_days
        
        logger.info(f"開始獲取股票 {stock_code} 的歷史數據 (增量下載)")
        
        # 檢查現有CSV文件，確定需要下載的月份範圍
        csv_file = PROJECT_ROOT / "data" / f"{stock_code}.csv"
        existing_data = pd.DataFrame()
        existing_dates = set()
        
        if csv_file.exists() and not force_update:
            try:
                existing_data = pd.read_csv(csv_file)
                if not existing_data.empty and '交易日期' in existing_data.columns:
                    # 提取現有日期
                    for date_str in existing_data['交易日期']:
                        try:
                            if '/' in str(date_str):
                                roc_year, month, day = str(date_str).split('/')
                                gregorian_year = int(roc_year) + 1911
                                date_obj = datetime(gregorian_year, int(month), int(day))
                                existing_dates.add(date_obj)
                        except:
                            continue
                    logger.info(f"現有數據包含 {len(existing_dates)} 個交易日")
            except Exception as e:
                logger.warning(f"讀取現有CSV文件失敗: {e}")
        
        # 計算需要獲取的月份範圍（不超過當前月份）
        current_date = datetime.now()
        end_date = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # 如果有現有數據，從最新日期開始補齊
        if existing_dates:
            latest_existing_date = max(existing_dates)
            logger.info(f"現有數據最新日期: {latest_existing_date}")
            
            # 從最新日期的下一個交易日開始補齊
            start_date = latest_existing_date + timedelta(days=1)
        else:
            # 沒有現有數據，從指定天數前開始，但不早於 TWSE API 限制日期
            start_date = end_date - timedelta(days=days)
            
            # TWSE API 限制：最早查詢到民國 99 年 1 月 4 日 (2010-01-04)
            min_date = datetime(2010, 1, 4)
            if start_date < min_date:
                start_date = min_date
                logger.info(f"調整開始日期至 TWSE API 最早支援日期: {start_date.date()}")
            
            logger.info(f"沒有現有數據，從 {start_date.date()} 開始下載")
        
        all_data = []
        current_date = start_date.replace(day=1)  # 從月初開始
        
        # 確定需要下載的月份
        months_to_download = []
        temp_date = current_date
        current_date_check = datetime.now()
        
        while temp_date <= end_date:
            year = temp_date.year
            month = temp_date.month
            
            # 檢查是否為未來月份（超過當前日期）
            if temp_date.year > current_date_check.year or (temp_date.year == current_date_check.year and temp_date.month > current_date_check.month):
                logger.info(f"跳過未來月份: {year}年{month}月")
                temp_date = temp_date.replace(day=1) + timedelta(days=32)
                temp_date = temp_date.replace(day=1)
                continue
            
            # 檢查該月是否完整（有數據且最新日期在該月內）
            month_data = [date_obj for date_obj in existing_dates 
                         if date_obj.year == year and date_obj.month == month]
            
            # 如果該月沒有數據，或者最新數據不是最近的交易日，則需要下載
            if not month_data:
                months_to_download.append((year, month))
                logger.info(f"需要下載 {year}年{month}月 的數據（該月無數據）")
            else:
                # 檢查該月最新數據是否足夠新
                latest_month_date = max(month_data)
                days_since_latest = (datetime.now() - latest_month_date).days
                
                # 計算交易日（排除週末）
                trading_days_since_latest = self._count_trading_days(latest_month_date, datetime.now())
                
                # 如果最新數據超過1個交易日，且當前月份是最近月份，則需要更新
                current_month = datetime.now().replace(day=1)
                is_current_month = (temp_date.year == current_month.year and temp_date.month == current_month.month)
                
                if trading_days_since_latest > 1 and is_current_month:
                    months_to_download.append((year, month))
                    logger.info(f"需要下載 {year}年{month}月 的數據（數據過舊，{trading_days_since_latest}個交易日前）")
            
            # 移動到下個月
            if temp_date.month == 12:
                temp_date = temp_date.replace(year=temp_date.year + 1, month=1)
            else:
                temp_date = temp_date.replace(month=temp_date.month + 1)
        
        if not months_to_download:
            # 檢查現有數據是否涵蓋了用戶要求的時間範圍
            if existing_data.empty:
                logger.info(f"股票 {stock_code} 沒有現有數據，需要下載")
            else:
                # 計算現有數據的時間範圍
                existing_dates_list = sorted(existing_dates)
                if existing_dates_list:
                    earliest_date = existing_dates_list[0]
                    latest_date = existing_dates_list[-1]
                    days_covered = (latest_date - earliest_date).days + 1
                    
                    logger.info(f"現有數據涵蓋 {days_covered} 天 ({earliest_date.date()} 到 {latest_date.date()})")
                    logger.info(f"用戶要求 {days} 天的數據")
                    
                    # 如果現有數據涵蓋的天數少於用戶要求，需要下載更多歷史數據
                    if days_covered < days:
                        logger.info(f"現有數據不足以涵蓋用戶要求的 {days} 天，需要下載更多歷史數據")
                        # 重新計算需要下載的月份，從更早的日期開始
                        target_start_date = latest_date - timedelta(days=days)
                        months_to_download = self._calculate_months_to_download(target_start_date, earliest_date, existing_dates)
                    else:
                        logger.info(f"股票 {stock_code} 的數據已涵蓋用戶要求的時間範圍，無需下載")
                        return existing_data
                else:
                    logger.info(f"股票 {stock_code} 的數據已是最新，無需下載")
                    return existing_data
        
        logger.info(f"需要下載 {len(months_to_download)} 個月的數據")
        
        # 下載缺失的月份數據
        for year, month in months_to_download:
            # 獲取該月數據
            month_data = self.fetch_stock_monthly_data(stock_code, year, month)
            
            if not month_data.empty:
                all_data.append(month_data)
            
            # 避免請求過於頻繁
            time.sleep(self.request_delay)
        
        if not all_data:
            logger.warning(f"股票 {stock_code} 沒有獲取到任何新數據")
            return existing_data
        
        # 合併新數據
        new_df = pd.concat(all_data, ignore_index=True)
        new_df = new_df.drop_duplicates(subset=['date']).sort_values('date').reset_index(drop=True)
        
        # 如果有現有數據，合併並去重
        if not existing_data.empty:
            # 轉換現有數據格式
            existing_df = self._convert_existing_csv_to_standard_format(existing_data, stock_code)
            if not existing_df.empty:
                # 合併所有數據
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['date']).sort_values('date').reset_index(drop=True)
            else:
                combined_df = new_df
        else:
            combined_df = new_df
        
        logger.info(f"股票 {stock_code} 總共獲取 {len(combined_df)} 筆數據 (新增 {len(new_df)} 筆)")
        
        return combined_df
    
    def fetch_all_stocks(self, save_to_file: bool = True) -> pd.DataFrame:
        """
        獲取所有股票的數據
        
        Args:
            save_to_file: 是否保存到檔案
            
        Returns:
            合併後的股價數據 DataFrame
        """
        all_data = []
        
        logger.info(f"開始獲取 {len(self.stock_list)} 支股票的TWSE官方數據...")
        
        for i, stock_code in enumerate(self.stock_list):
            logger.debug(f"正在獲取股票 {stock_code} ({i+1}/{len(self.stock_list)}) - 進度: {i/len(self.stock_list)*100:.1f}%")
            
            try:
                df = self.fetch_stock_historical_data(stock_code, self.lookback_days)
                if not df.empty:
                    all_data.append(df)
                    logger.debug(f"  ✓ 成功獲取 {len(df)} 筆數據")
                else:
                    logger.warning(f"  ✗ 未獲取到數據")
            except Exception as e:
                logger.error(f"  ✗ 獲取股票 {stock_code} 時發生錯誤: {e}")
            
            # 避免請求過於頻繁
            time.sleep(self.request_delay)
        
        if not all_data:
            logger.error("沒有成功獲取任何股票數據")
            return pd.DataFrame()
        
        # 合併所有數據
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.sort_values(['stock_code', 'date']).reset_index(drop=True)
        
        logger.info(f"TWSE官方數據收集完成，總共 {len(combined_df)} 筆數據")
        
        # 保存到檔案
        if save_to_file:
            self._save_data(combined_df)
        
        return combined_df
    
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
            
            logger.info(f"數據已保存到: {RAW_PRICES_FILE}")
            
        except Exception as e:
            logger.error(f"保存數據時發生錯誤: {e}")
    
    def test_connection(self) -> bool:
        """
        測試與TWSE API的連接
        
        Returns:
            連接是否成功
        """
        try:
            # 測試獲取台積電(2330)最近一個月的數據
            test_data = self.fetch_stock_monthly_data('2330', 2024, 11)
            
            if not test_data.empty:
                logger.info("✓ TWSE API連接測試成功")
                return True
            else:
                logger.warning("⚠ TWSE API連接測試失敗 - 無數據返回")
                return False
                
        except Exception as e:
            logger.error(f"✗ TWSE API連接測試失敗: {e}")
            return False
    
    def _count_trading_days(self, start_date: datetime, end_date: datetime) -> int:
        """
        計算兩個日期之間的交易日數量（排除週末）
        
        Args:
            start_date: 開始日期
            end_date: 結束日期
            
        Returns:
            交易日數量
        """
        trading_days = 0
        current_date = start_date + timedelta(days=1)  # 從下一天開始計算
        
        while current_date <= end_date:
            # 週一=0, 週日=6，排除週六(5)和週日(6)
            if current_date.weekday() < 5:
                trading_days += 1
            current_date += timedelta(days=1)
        
        return trading_days


def main():
    """主函數 - 用於測試TWSE數據收集器"""
    fetcher = OfficialTWSEFetcher()
    
    # 測試連接
    if fetcher.test_connection():
        logger.info("開始測試數據收集...")
        
        # 測試獲取單一股票數據
        test_stock = '2330'  # 台積電
        df = fetcher.fetch_stock_historical_data(test_stock, days=30)
        
        if not df.empty:
            logger.info(f"測試成功！獲取到 {len(df)} 筆數據")
            logger.info(f"數據範圍: {df['date'].min()} 到 {df['date'].max()}")
            logger.info(f"欄位: {list(df.columns)}")
        else:
            logger.error("測試失敗 - 沒有獲取到數據")
    else:
        logger.error("API連接測試失敗，無法進行數據收集測試")


if __name__ == "__main__":
    main()
