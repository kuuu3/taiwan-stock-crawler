"""
證券櫃檯買賣中心(TPEX)數據收集器
使用網頁爬蟲自動抓取TPEX股票數據
"""

import pandas as pd
import requests
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import os
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


class OfficialTPEXFetcher:
    """證券櫃檯買賣中心數據收集器（使用網頁爬蟲）"""
    
    def __init__(self):
        self.stock_list = DATA_COLLECTION_CONFIG["STOCK_LIST"]
        self.lookback_days = DATA_COLLECTION_CONFIG["LOOKBACK_DAYS"]
        
        # TPEX API 配置
        self.base_url = "https://www.tpex.org.tw"
        self.timeout = API_CONFIG["TPEX_API"]["TIMEOUT"]
        
        # 設置請求標頭
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.tpex.org.tw/'
        }
        
        logger.info("TPEX數據收集器初始化完成（使用網頁爬蟲）")
    
    def fetch_stock_data(self, stock_code: str, days: int = None) -> pd.DataFrame:
        """
        抓取指定股票的歷史數據
        
        Args:
            stock_code: 股票代碼
            days: 回看天數，如果為None則使用預設值
            
        Returns:
            包含股票歷史數據的DataFrame
        """
        if days is None:
            days = self.lookback_days
            
        logger.info(f"開始抓取TPEX股票 {stock_code} 的歷史數據 (回看 {days} 天)")
        
        try:
            # 計算日期範圍
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # 轉換為民國年格式
            start_year = start_date.year - 1911
            end_year = end_date.year - 1911
            
            # 構建請求URL
            url = f"{self.base_url}/openapi/v1/tpex_mainboard_daily_close_quotes"
            
            # 請求參數
            params = {
                'd': f"{end_year:03d}/{end_date.month:02d}/{end_date.day:02d}",
                's': '0,asc',
                'o': 'htm'
            }
            
            logger.info(f"請求URL: {url}")
            logger.info(f"請求參數: {params}")
            
            # 發送請求
            response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            # 解析回應
            if response.text.strip():
                # 嘗試解析為JSON
                try:
                    data = response.json()
                    if 'aaData' in data:
                        df = self._parse_tpex_data(data['aaData'], stock_code)
                        logger.info(f"成功抓取股票 {stock_code} 的 {len(df)} 筆數據")
                        return df
                    else:
                        logger.warning(f"TPEX回應中沒有找到數據: {data}")
                        return pd.DataFrame()
                except json.JSONDecodeError:
                    logger.warning(f"TPEX回應不是有效的JSON格式")
                    return pd.DataFrame()
            else:
                logger.warning("TPEX回應為空")
                return pd.DataFrame()
                
        except requests.exceptions.RequestException as e:
            logger.error(f"TPEX請求失敗: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"抓取TPEX股票 {stock_code} 數據時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _parse_tpex_data(self, data: List[List], stock_code: str) -> pd.DataFrame:
        """
        解析TPEX數據
        
        Args:
            data: TPEX API返回的原始數據
            stock_code: 股票代碼
            
        Returns:
            解析後的DataFrame
        """
        if not data:
            return pd.DataFrame()
        
        try:
            # 過濾指定股票的數據
            stock_data = []
            for row in data:
                if len(row) > 0 and str(row[0]).strip() == str(stock_code).strip():
                    stock_data.append(row)
            
            if not stock_data:
                logger.warning(f"沒有找到股票 {stock_code} 的數據")
                return pd.DataFrame()
            
            # 轉換為DataFrame
            df = pd.DataFrame(stock_data, columns=[
                '股票代號', '股票名稱', '收盤價', '漲跌', '漲跌幅', '開盤價', 
                '最高價', '最低價', '成交股數', '成交金額', '成交筆數'
            ])
            
            # 數據清理和轉換
            df = self._clean_tpex_data(df)
            
            return df
            
        except Exception as e:
            logger.error(f"解析TPEX數據時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _clean_tpex_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清理TPEX數據
        
        Args:
            df: 原始DataFrame
            
        Returns:
            清理後的DataFrame
        """
        try:
            # 複製DataFrame避免修改原始數據
            df = df.copy()
            
            # 轉換數值欄位
            numeric_columns = ['收盤價', '開盤價', '最高價', '最低價', '成交股數', '成交金額', '成交筆數']
            for col in numeric_columns:
                if col in df.columns:
                    # 移除逗號並轉換為數值
                    df[col] = df[col].astype(str).str.replace(',', '').str.replace('--', '0')
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 處理漲跌欄位
            if '漲跌' in df.columns:
                df['漲跌'] = df['漲跌'].astype(str).str.replace('+', '').str.replace('--', '0')
                df['漲跌'] = pd.to_numeric(df['漲跌'], errors='coerce')
            
            # 處理漲跌幅欄位
            if '漲跌幅' in df.columns:
                df['漲跌幅'] = df['漲跌幅'].astype(str).str.replace('%', '').str.replace('--', '0')
                df['漲跌幅'] = pd.to_numeric(df['漲跌幅'], errors='coerce')
            
            # 添加交易日期（使用當前日期）
            df['交易日期'] = datetime.now().strftime('%Y/%m/%d')
            
            # 重新排列欄位順序
            column_order = ['交易日期', '股票代號', '股票名稱', '收盤價', '開盤價', '最高價', '最低價', 
                          '漲跌', '漲跌幅', '成交股數', '成交金額', '成交筆數']
            
            # 只保留存在的欄位
            available_columns = [col for col in column_order if col in df.columns]
            df = df[available_columns]
            
            return df
            
        except Exception as e:
            logger.error(f"清理TPEX數據時發生錯誤: {e}")
            return df
    
    def load_csv_data(self, csv_file: Path) -> pd.DataFrame:
        """
        載入CSV文件數據
        
        Args:
            csv_file: CSV文件路徑
            
        Returns:
            載入的DataFrame
        """
        try:
            # 嘗試不同的編碼
            encodings = ['utf-8', 'utf-8-sig', 'big5', 'cp950']
            
            for encoding in encodings:
                try:
                    # TPEX CSV需要跳過前4行標題
                    df = pd.read_csv(csv_file, encoding=encoding, skiprows=4)
                    logger.info(f"成功載入 {csv_file.name} (編碼: {encoding}, 跳過前4行)")
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    logger.warning(f"編碼 {encoding} 失敗: {e}")
                    continue
            else:
                logger.error(f"無法載入 {csv_file.name} - 編碼問題")
                return pd.DataFrame()
            
            # 清理數據
            df = self._clean_csv_data(df, csv_file)
            
            return df
            
        except Exception as e:
            logger.error(f"載入CSV文件 {csv_file.name} 時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _clean_csv_data(self, df: pd.DataFrame, csv_file: Path) -> pd.DataFrame:
        """
        清理CSV數據
        
        Args:
            df: 原始DataFrame
            csv_file: CSV文件路徑
            
        Returns:
            清理後的DataFrame
        """
        try:
            # 從文件名提取股票代碼
            filename = csv_file.stem
            # 假設文件名格式為 ST43_3260_202405.csv
            parts = filename.split('_')
            if len(parts) >= 2:
                stock_code = parts[1]
            else:
                logger.warning(f"無法從文件名 {filename} 提取股票代碼")
                return pd.DataFrame()
            
            # 添加股票代碼
            df['stock_code'] = stock_code
            
            # 重命名欄位（根據TPEX CSV的實際欄位名稱調整）
            column_mapping = {
                '日 期': 'date',
                '成交仟股': 'volume',
                '成交仟元': 'turnover',
                '開盤': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盤': 'close',
                '漲跌': 'change',
                '筆數': 'transactions'
            }
            
            # 重命名存在的欄位
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            # 轉換日期格式 - TPEX使用民國年格式 (113/05/02)
            if 'date' in df.columns:
                def convert_tpex_date(date_str):
                    try:
                        # 民國年轉西元年
                        year, month, day = date_str.split('/')
                        year = int(year) + 1911  # 民國年轉西元年
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    except:
                        return None
                
                df['date'] = df['date'].apply(convert_tpex_date)
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
            
            # 按日期排序 (確保日期欄位不是分類型別)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            logger.info(f"清理完成，股票 {stock_code} 有 {len(df)} 筆數據")
            
            return df
            
        except Exception as e:
            logger.error(f"清理CSV數據時發生錯誤: {e}")
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
            
            # 直接調用 TPEX API 抓取指定日期範圍的數據
            return self._fetch_from_tpex_api_by_date_range(stock_code, start_dt, end_dt)
            
        except Exception as e:
            logger.error(f"按日期範圍抓取股票 {stock_code} 數據時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _fetch_from_tpex_api_by_date_range(self, stock_code: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        按日期範圍從 TPEX API 抓取股票數據
        
        Args:
            stock_code: 股票代碼
            start_date: 開始日期
            end_date: 結束日期
            
        Returns:
            股票數據DataFrame
        """
        try:
            import requests
            
            logger.info(f"從 TPEX API 抓取股票 {stock_code} 的日期範圍數據: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
            
            all_data = []
            
            # 計算需要抓取的月份範圍
            current_date = start_date.replace(day=1)  # 從開始月份的第一天開始
            end_month = end_date.replace(day=1)
            
            while current_date <= end_month:
                year = current_date.year
                month = current_date.month
                
                try:
                    # 構建 TPEX API URL
                    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
                    
                    # 請求參數
                    params = {
                        'code': stock_code,
                        'date': f'{year}/{month:02d}/01',
                        'id': '',
                        'response': 'utf-8'
                    }
                    
                    logger.info(f"嘗試獲取 {year}年{month}月 的數據...")
                    
                    # 發送請求
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    }
                    
                    response = requests.get(url, params=params, headers=headers, timeout=30)
                    response.raise_for_status()
                    
                    # 解析回應
                    data = self._parse_tpex_response(response, stock_code)
                    if not data.empty:
                        all_data.append(data)
                        logger.info(f"找到 {year}年{month}月 的股票數據")
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
            if '交易日期' in combined_df.columns:
                # 轉換日期格式進行比較
                combined_df['_temp_date'] = combined_df['交易日期'].apply(self._convert_roc_to_datetime)
                
                # 過濾日期範圍
                mask = (combined_df['_temp_date'] >= start_date) & (combined_df['_temp_date'] <= end_date)
                filtered_df = combined_df[mask].copy()
                filtered_df = filtered_df.drop('_temp_date', axis=1)
                
                logger.info(f"成功從 TPEX 官網抓取股票 {stock_code} 的日期範圍數據: {len(filtered_df)} 筆")
                return filtered_df
            else:
                logger.warning(f"數據中缺少交易日期欄位")
                return combined_df
                
        except Exception as e:
            logger.error(f"從 TPEX API 抓取股票 {stock_code} 日期範圍數據時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _convert_roc_to_datetime(self, roc_date_str: str) -> datetime:
        """
        將民國年日期字串轉換為 datetime 對象
        
        Args:
            roc_date_str: 民國年日期字串 (格式: 114/08/21)
            
        Returns:
            datetime 對象
        """
        try:
            if pd.isna(roc_date_str) or roc_date_str == '':
                return datetime.min
            
            # 處理民國年格式 (114/08/21)
            parts = str(roc_date_str).split('/')
            if len(parts) == 3:
                roc_year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
                
                # 轉換為西元年
                gregorian_year = roc_year + 1911
                
                return datetime(gregorian_year, month, day)
            else:
                return datetime.min
                
        except Exception as e:
            logger.error(f"轉換民國年日期 {roc_date_str} 時發生錯誤: {e}")
            return datetime.min
    
    def _parse_tpex_response(self, response, stock_code: str) -> pd.DataFrame:
        """
        解析 TPEX API 回應
        
        Args:
            response: requests 回應對象
            stock_code: 股票代碼
            
        Returns:
            解析後的 DataFrame
        """
        try:
            # 嘗試解析 JSON 格式
            try:
                data = response.json()
                if 'data' in data and data['data']:
                    # 轉換為 DataFrame
                    df = pd.DataFrame(data['data'], columns=data['fields'])
                    return self._clean_tpex_data(df)
                else:
                    logger.warning(f"TPEX API 回應中沒有數據")
                    return pd.DataFrame()
            except:
                # 如果不是 JSON 格式，嘗試解析 HTML
                content = response.text
                
                # 簡單的 HTML 表格解析
                import re
                
                # 尋找表格數據
                table_pattern = r'<table[^>]*>(.*?)</table>'
                tables = re.findall(table_pattern, content, re.DOTALL | re.IGNORECASE)
                
                if tables:
                    # 解析第一個表格
                    table_html = tables[0]
                    
                    # 提取行數據
                    row_pattern = r'<tr[^>]*>(.*?)</tr>'
                    rows = re.findall(row_pattern, table_html, re.DOTALL | re.IGNORECASE)
                    
                    data_rows = []
                    for row in rows:
                        # 提取單元格數據
                        cell_pattern = r'<td[^>]*>(.*?)</td>'
                        cells = re.findall(cell_pattern, row, re.DOTALL | re.IGNORECASE)
                        
                        if len(cells) >= 8:  # 確保有足夠的欄位
                            # 清理 HTML 標籤
                            clean_cells = []
                            for cell in cells:
                                clean_cell = re.sub(r'<[^>]*>', '', cell).strip()
                                clean_cells.append(clean_cell)
                            
                            data_rows.append(clean_cells)
                    
                    if data_rows:
                        # 創建 DataFrame
                        columns = ['交易日期', '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '成交筆數']
                        df = pd.DataFrame(data_rows, columns=columns[:len(data_rows[0])])
                        return self._clean_tpex_data(df)
                
                logger.warning(f"無法解析 TPEX API 回應")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"解析 TPEX API 回應時發生錯誤: {e}")
            return pd.DataFrame()

    def fetch_stock_historical_data(self, stock_code: str, days: int = None, force_update: bool = False) -> pd.DataFrame:
        """
        獲取股票歷史數據（從手動CSV文件）
        
        Args:
            stock_code: 股票代碼
            days: 回看天數
            force_update: 是否強制更新，跳過增量檢查
            
        Returns:
            歷史數據DataFrame
        """
        if days is None:
            days = self.lookback_days
        
        logger.info(f"開始獲取TPEX股票 {stock_code} 的歷史數據 (回看 {days} 天)")
        
        # 直接讀取格式化後的CSV文件
        csv_file = PROJECT_ROOT / "data" / f"{stock_code}.csv"
        
        # 檢查CSV文件是否存在且數據是否最新
        csv_exists = csv_file.exists()
        csv_data_fresh = False
        
        if csv_exists and not force_update:
            try:
                df = pd.read_csv(csv_file)
                if not df.empty and '交易日期' in df.columns:
                    # 檢查最新日期
                    latest_date_str = df['交易日期'].iloc[-1]
                    try:
                        roc_year, month, day = latest_date_str.split('/')
                        gregorian_year = int(roc_year) + 1911
                        latest_date = datetime(int(gregorian_year), int(month), int(day))
                        
                        # 檢查是否超過1天
                        days_old = (datetime.now() - latest_date).days
                        csv_data_fresh = days_old <= 1
                        
                        if not csv_data_fresh:
                            logger.info(f"CSV文件數據過期 ({days_old} 天前)，嘗試使用TPEX API更新")
                    except:
                        logger.warning(f"無法解析CSV文件中的日期格式，嘗試使用TPEX API")
                        csv_data_fresh = False
            except:
                logger.warning(f"無法讀取CSV文件，嘗試使用TPEX API")
                csv_data_fresh = False
        
        # 如果CSV文件不存在或數據過期，嘗試TPEX API
        if not csv_exists or not csv_data_fresh:
            logger.info(f"嘗試從TPEX API獲取股票 {stock_code} 的最新數據")
            api_df = self._fetch_from_tpex_api(stock_code, days)
            if not api_df.empty:
                return api_df
            elif csv_exists:
                logger.warning(f"TPEX API獲取失敗，使用過期的CSV數據")
            else:
                logger.warning(f"TPEX API獲取失敗且無CSV文件")
                return pd.DataFrame()
        
        # 使用CSV文件數據
        try:
            df = pd.read_csv(csv_file)
            
            if df.empty:
                logger.warning(f"股票 {stock_code} 的CSV文件為空")
                return pd.DataFrame()
            
            # 轉換日期格式（從民國年轉換為西元年）
            def convert_roc_date(date_str):
                try:
                    if '/' in date_str:
                        parts = date_str.split('/')
                        if len(parts) == 3 and len(parts[0]) == 3:
                            roc_year = int(parts[0])
                            gregorian_year = roc_year + 1911
                            return datetime.strptime(f"{gregorian_year}/{parts[1]}/{parts[2]}", '%Y/%m/%d').date()
                except:
                    pass
                return None
            
            df['date'] = df['交易日期'].apply(convert_roc_date)
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
            df['change'] = df['close'].diff()
            
            # 限制回看天數
            if days > 0:
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=days)
                df = df[df['date'] >= start_date]
            
            logger.info(f"成功獲取股票 {stock_code} 的歷史數據: {len(df)} 筆")
            return df
            
        except Exception as e:
            logger.error(f"讀取股票 {stock_code} 的CSV文件時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _fetch_from_tpex_api(self, stock_code: str, days: int) -> pd.DataFrame:
        """
        直接從 TPEX 官網爬取股票歷史數據
        
        Args:
            stock_code: 股票代碼
            days: 回看天數
            
        Returns:
            歷史數據DataFrame
        """
        try:
            import requests
            
            logger.info(f"直接從 TPEX 官網爬取股票 {stock_code} 的歷史數據...")
            
            # 嘗試不同的月份獲取歷史數據
            end_date = datetime.now()
            all_data = []
            
            # 獲取最近幾個月的數據
            # 計算需要涵蓋的月份數，確保包含完整回看期間
            months_needed = (days // 30) + 2  # 增加緩衝月份
            for i in range(months_needed):
                try:
                    # 計算目標月份
                    target_date = end_date - timedelta(days=i * 30)
                    year = target_date.year
                    month = target_date.month
                    
                    # 轉換為民國年格式
                    roc_year = year - 1911
                    
                    # 構建 TPEX API URL (使用正確的格式)
                    url = f"https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
                    
                    # 請求參數
                    params = {
                        'code': stock_code,
                        'date': f'{year}/{month:02d}/01',
                        'id': '',
                        'response': 'utf-8'
                    }
                    
                    logger.info(f"嘗試獲取 {year}年{month}月 的數據...")
                    logger.info(f"請求URL: {url}")
                    logger.info(f"請求參數: {params}")
                    
                    # 發送請求，添加必要的 headers
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1'
                    }
                    response = requests.get(url, params=params, headers=headers, timeout=30)
                    
                    if response.status_code == 200:
                        # 檢查是否包含股票數據
                        response_text = response.text
                        if stock_code in response_text and any(keyword in response_text for keyword in ['成交', '收盤', '開盤', '最高', '最低']):
                            logger.info(f"找到 {year}年{month}月 的股票數據")
                            
                            # 解析 CSV 格式數據
                            df = self._parse_tpex_csv(response_text, stock_code)
                            if not df.empty:
                                all_data.append(df)
                    
                    # 避免請求過於頻繁，使用隨機延遲
                    import random
                    delay = random.uniform(1.0, 1.5)
                    time.sleep(delay)
                    
                except Exception as e:
                    logger.warning(f"獲取 {year}年{month}月 數據時發生錯誤: {e}")
                    continue
            
            if all_data:
                # 合併所有數據
                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['date']).sort_values('date').reset_index(drop=True)
                
                # 限制到指定的天數
                if len(combined_df) > days:
                    combined_df = combined_df.tail(days)
                
                logger.info(f"成功從 TPEX 官網獲取股票 {stock_code} 的歷史數據: {len(combined_df)} 筆")
                return combined_df
            else:
                logger.warning(f"無法從 TPEX 官網獲取股票 {stock_code} 的數據")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"從 TPEX 官網獲取股票 {stock_code} 數據時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _parse_tpex_csv(self, csv_text: str, stock_code: str) -> pd.DataFrame:
        """解析 TPEX CSV 格式數據"""
        try:
            import pandas as pd
            from io import StringIO
            
            # 找到 CSV 數據的開始位置
            lines = csv_text.split('\n')
            csv_start = -1
            
            for i, line in enumerate(lines):
                # 支援多種標題格式
                if ('日 期' in line and '成交張數' in line) or \
                   ('日 期' in line and '成交仟股' in line) or \
                   ('日 期' in line and '成交股數' in line):
                    csv_start = i
                    break
            
            if csv_start == -1:
                logger.warning(f"無法找到 CSV 標題行")
                return pd.DataFrame()
            
            # 提取 CSV 數據
            csv_lines = lines[csv_start:]
            csv_content = '\n'.join(csv_lines)
            
            # 使用 pandas 讀取 CSV
            df = pd.read_csv(StringIO(csv_content))
            
            if df.empty:
                return pd.DataFrame()
            
            # 清理數據
            df = self._clean_tpex_data(df, stock_code)
            
            return df
            
        except Exception as e:
            logger.error(f"解析 TPEX CSV 時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _parse_tpex_table(self, table, stock_code: str) -> pd.DataFrame:
        """解析 TPEX 表格數據"""
        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return pd.DataFrame()
            
            # 提取表頭
            headers = []
            header_row = rows[0]
            for cell in header_row.find_all(['th', 'td']):
                headers.append(cell.get_text(strip=True))
            
            # 提取數據行
            data = []
            for row in rows[1:]:
                row_data = []
                for cell in row.find_all('td'):
                    row_data.append(cell.get_text(strip=True))
                if row_data:
                    data.append(row_data)
            
            if not data:
                return pd.DataFrame()
            
            # 創建DataFrame
            df = pd.DataFrame(data, columns=headers)
            
            # 清理數據
            df = self._clean_tpex_data(df, stock_code)
            
            return df
            
        except Exception as e:
            logger.error(f"解析 TPEX 表格時發生錯誤: {e}")
            return pd.DataFrame()
    
    def _clean_tpex_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """清理 TPEX 數據"""
        try:
            # 添加股票代碼
            df['stock_code'] = stock_code
            
            # 重命名欄位 (支援新的 CSV 格式)
            column_mapping = {
                '日 期': 'date',
                '日期': 'date',
                '成交張數': 'volume',
                '成交仟股': 'volume',  # 2024年格式
                '成交股數': 'volume', 
                '成交仟元': 'turnover',
                '成交金額': 'turnover',
                '開盤': 'open',
                '開盤價': 'open',
                '最高': 'high',
                '最高價': 'high',
                '最低': 'low',
                '最低價': 'low',
                '收盤': 'close',
                '收盤價': 'close',
                '漲跌': 'change',
                '漲跌價差': 'change',
                '筆數': 'transactions',
                '成交筆數': 'transactions'
            }
            
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            # 轉換日期格式
            if 'date' in df.columns:
                def convert_date(date_str):
                    try:
                        if '/' in date_str:
                            parts = date_str.split('/')
                            if len(parts) == 3:
                                if len(parts[0]) == 3:  # 民國年
                                    roc_year = int(parts[0])
                                    gregorian_year = roc_year + 1911
                                    return datetime.strptime(f"{gregorian_year}/{parts[1]}/{parts[2]}", '%Y/%m/%d').date()
                                else:  # 西元年
                                    return datetime.strptime(date_str, '%Y/%m/%d').date()
                    except:
                        pass
                    return None
                
                df['date'] = df['date'].apply(convert_date)
                df = df.dropna(subset=['date'])
            
            # 轉換數值欄位
            numeric_columns = ['volume', 'turnover', 'open', 'high', 'low', 'close', 'change', 'transactions']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace(',', '').str.replace('--', '0')
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 轉換成交量單位 (張數轉股數，1張=1000股；仟股轉股數，1仟股=1000股)
            if 'volume' in df.columns:
                df['volume'] = df['volume'] * 1000
            
            # 轉換成交金額單位 (仟元轉元)
            if 'turnover' in df.columns:
                df['turnover'] = df['turnover'] * 1000
            
            # 按日期排序 (確保日期欄位不是分類型別)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"清理 TPEX 數據時發生錯誤: {e}")
            return df
    
    def _convert_yfinance_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        轉換 yfinance 數據格式為標準格式
        
        Args:
            df: yfinance 返回的 DataFrame
            stock_code: 股票代碼
            
        Returns:
            標準格式的 DataFrame
        """
        try:
            # yfinance 返回的 DataFrame 索引是日期
            df = df.reset_index()
            
            # 添加股票代碼
            df['stock_code'] = stock_code
            
            # 重命名欄位為標準格式
            column_mapping = {
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }
            
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            # 計算漲跌價差
            if 'close' in df.columns:
                df['change'] = df['close'].diff()
            
            # 確保日期格式正確
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.date
            
            # 按日期排序 (確保日期欄位不是分類型別)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"轉換 yfinance 數據格式時發生錯誤: {e}")
            return df
    
    def fetch_all_stocks(self, save_to_file: bool = True) -> pd.DataFrame:
        """
        獲取所有TPEX股票的數據
        
        Args:
            save_to_file: 是否保存到檔案
            
        Returns:
            合併後的股價數據 DataFrame
        """
        all_data = []
        
        logger.info(f"開始獲取TPEX股票的數據...")
        
        # 獲取所有可用的CSV文件
        csv_files = self.get_available_csv_files()
        
        if not csv_files:
            logger.warning("沒有找到任何TPEX CSV文件")
            return pd.DataFrame()
        
        # 按股票代碼分組處理
        stock_files = {}
        for csv_file in csv_files:
            filename = csv_file.stem
            parts = filename.split('_')
            if len(parts) >= 2:
                stock_code = parts[1]
                if stock_code not in stock_files:
                    stock_files[stock_code] = []
                stock_files[stock_code].append(csv_file)
        
        logger.info(f"找到 {len(stock_files)} 支股票的數據文件")
        
        for stock_code, files in stock_files.items():
            logger.info(f"處理股票 {stock_code} ({len(files)} 個文件)")
            
            try:
                df = self.fetch_stock_historical_data(stock_code, self.lookback_days)
                if not df.empty:
                    all_data.append(df)
                    logger.info(f"  ✓ 成功獲取 {len(df)} 筆數據")
                else:
                    logger.warning(f"  ✗ 未獲取到數據")
            except Exception as e:
                logger.error(f"  ✗ 處理股票 {stock_code} 時發生錯誤: {e}")
        
        if not all_data:
            logger.error("沒有成功獲取任何TPEX股票數據")
            return pd.DataFrame()
        
        # 合併所有數據
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.sort_values(['stock_code', 'date']).reset_index(drop=True)
        
        logger.info(f"TPEX數據收集完成，總共 {len(combined_df)} 筆數據")
        
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
            
            logger.info(f"TPEX數據已保存到: {RAW_PRICES_FILE}")
            
        except Exception as e:
            logger.error(f"保存TPEX數據時發生錯誤: {e}")
    
    def test_connection(self) -> bool:
        """
        測試TPEX API連接
        
        Returns:
            是否連接成功
        """
        try:
            logger.info("測試TPEX API連接...")
            
            # 測試基本連接
            test_url = f"{self.base_url}/openapi/v1/tpex_mainboard_daily_close_quotes"
            test_params = {
                'd': '113/10/02',  # 測試日期
                's': '0,asc',
                'o': 'htm'
            }
            
            response = requests.get(test_url, params=test_params, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            if response.text.strip():
                logger.info("✓ TPEX API連接測試成功")
                return True
            else:
                logger.warning("⚠ TPEX API回應為空")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ TPEX API連接測試失敗: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ TPEX API測試時發生錯誤: {e}")
            return False


def main():
    """主函數 - 用於測試TPEX數據收集器"""
    fetcher = OfficialTPEXFetcher()
    
    # 測試連接
    if fetcher.test_connection():
        logger.info("開始測試TPEX數據收集...")
        
        # 測試獲取單一股票數據
        test_stock = '3260'  # 威剛
        df = fetcher.fetch_stock_historical_data(test_stock, days=30)
        
        if not df.empty:
            logger.info(f"測試成功！獲取到 {len(df)} 筆數據")
            logger.info(f"數據範圍: {df['date'].min()} 到 {df['date'].max()}")
            logger.info(f"欄位: {list(df.columns)}")
        else:
            logger.error("測試失敗 - 沒有獲取到數據")
    else:
        logger.error("TPEX數據測試失敗，無法進行數據收集測試")


if __name__ == "__main__":
    main()
