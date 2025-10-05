#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票數據格式轉換器
將原始數據轉換為指定的CSV格式
"""

import pandas as pd
import os
import sys
from pathlib import Path
from datetime import datetime
import logging

# 設置輸出編碼
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

from .config import PROJECT_ROOT

logger = logging.getLogger(__name__)

class StockDataFormatter:
    """股票數據格式轉換器"""
    
    def __init__(self, output_dir: str = None):
        """
        初始化格式轉換器
        
        Args:
            output_dir: 輸出目錄，如果為None則使用默認路徑
        """
        if output_dir is None:
            self.output_dir = PROJECT_ROOT / "data"
        else:
            self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def format_to_standard_csv(self, df: pd.DataFrame, stock_code: str, output_dir: Path = None) -> bool:
        """
        將DataFrame轉換為標準CSV格式
        
        Args:
            df: 原始數據DataFrame
            stock_code: 股票代碼
            output_dir: 輸出目錄，None 表示使用預設目錄
            
        Returns:
            是否成功
        """
        try:
            if df.empty:
                logger.warning(f"股票 {stock_code} 沒有數據")
                return False
            
            # 檢查必要的欄位（支持中英文欄位名）
            required_columns_en = ['date', 'volume', 'turnover', 'open', 'high', 'low', 'close']
            required_columns_zh = ['交易日期', '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價']
            
            # 檢查是否有英文欄位名
            has_english_columns = any(col in df.columns for col in required_columns_en)
            # 檢查是否有中文欄位名
            has_chinese_columns = any(col in df.columns for col in required_columns_zh)
            
            if not has_english_columns and not has_chinese_columns:
                logger.error(f"股票 {stock_code} 數據缺少必要欄位")
                logger.info(f"可用欄位: {list(df.columns)}")
                return False
            
            # 如果已經是中文欄位名，直接使用
            if has_chinese_columns and not has_english_columns:
                logger.info(f"股票 {stock_code} 數據已是標準格式，直接保存")
                return self._save_formatted_data(df, stock_code)
            
            # 檢查英文欄位是否齊全
            missing_columns = [col for col in required_columns_en if col not in df.columns]
            if missing_columns:
                logger.error(f"股票 {stock_code} 數據缺少必要欄位: {missing_columns}")
                logger.info(f"可用欄位: {list(df.columns)}")
                return False
            
            # 創建標準格式的DataFrame
            formatted_df = pd.DataFrame()
            
            # 1. 交易日期 (民國年格式)
            formatted_df['交易日期'] = df['date'].apply(self._convert_to_roc_date)
            
            # 2. 成交股數
            formatted_df['成交股數'] = df['volume'].fillna(0).astype(float)
            
            # 3. 成交金額
            formatted_df['成交金額'] = df['turnover'].fillna(0).astype(float)
            
            # 4. 開盤價
            formatted_df['開盤價'] = df['open'].fillna(0).astype(float)
            
            # 5. 最高價
            formatted_df['最高價'] = df['high'].fillna(0).astype(float)
            
            # 6. 最低價
            formatted_df['最低價'] = df['low'].fillna(0).astype(float)
            
            # 7. 收盤價
            formatted_df['收盤價'] = df['close'].fillna(0).astype(float)
            
            # 8. 漲跌價差 (處理+/-符號)
            formatted_df['漲跌價差'] = df['change'].apply(self._format_change)
            
            # 9. 成交筆數
            formatted_df['成交筆數'] = df['transactions'].fillna(0).astype(float)
            
            # 按日期排序 (確保日期欄位不是分類型別)
            if '交易日期' in formatted_df.columns:
                # 民國年格式轉換為西元年格式再排序
                def convert_roc_to_gregorian(roc_date_str):
                    try:
                        if '/' in roc_date_str:
                            parts = roc_date_str.split('/')
                            if len(parts) == 3 and len(parts[0]) == 3:
                                roc_year = int(parts[0])
                                gregorian_year = roc_year + 1911
                                return f"{gregorian_year}/{parts[1]}/{parts[2]}"
                    except:
                        pass
                    return roc_date_str
                
                # 轉換為西元年格式進行排序
                formatted_df['_sort_date'] = formatted_df['交易日期'].apply(convert_roc_to_gregorian)
                formatted_df['_sort_date'] = pd.to_datetime(formatted_df['_sort_date'])
                formatted_df = formatted_df.sort_values('_sort_date')
                formatted_df = formatted_df.drop('_sort_date', axis=1)
            else:
                formatted_df = formatted_df.sort_values('交易日期')
            
            # 保存為CSV文件
            return self._save_formatted_data(formatted_df, stock_code, output_dir)
            
        except Exception as e:
            logger.error(f"格式化股票 {stock_code} 數據時發生錯誤: {e}")
            return False
    
    def _save_formatted_data(self, df: pd.DataFrame, stock_code: str, output_dir: Path = None) -> bool:
        """
        保存格式化數據到CSV文件
        
        Args:
            df: 格式化後的DataFrame
            stock_code: 股票代碼
            output_dir: 輸出目錄，None 表示使用預設目錄
            
        Returns:
            是否成功
        """
        try:
            # 使用指定的輸出目錄或預設目錄
            save_dir = output_dir if output_dir else self.output_dir
            output_file = save_dir / f"{stock_code}.csv"
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            logger.info(f"成功保存股票 {stock_code} 的格式化數據到 {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"保存股票 {stock_code} 數據時發生錯誤: {e}")
            return False
    
    def _convert_to_roc_date(self, date_str) -> str:
        """
        將西元年日期轉換為民國年格式
        
        Args:
            date_str: 西元年日期字符串
            
        Returns:
            民國年日期字符串 (如: 104/02/13)
        """
        try:
            if pd.isna(date_str):
                return ""
            
            # 如果是字符串，先轉換為datetime
            if isinstance(date_str, str):
                date_obj = pd.to_datetime(date_str)
            else:
                date_obj = date_str
            
            # 轉換為民國年
            roc_year = date_obj.year - 1911
            month = date_obj.month
            day = date_obj.day
            
            return f"{roc_year:03d}/{month:02d}/{day:02d}"
            
        except Exception as e:
            logger.warning(f"轉換日期 {date_str} 時發生錯誤: {e}")
            return ""
    
    def _format_change(self, change_value) -> str:
        """
        格式化漲跌價差
        
        Args:
            change_value: 漲跌價差值
            
        Returns:
            格式化後的漲跌價差 (如: +0.45, -1.20, X)
        """
        try:
            if pd.isna(change_value) or change_value == 0:
                return "X"
            
            change_float = float(change_value)
            
            if change_float > 0:
                return f"+{change_float:.2f}"
            elif change_float < 0:
                return f"{change_float:.2f}"
            else:
                return "X"
                
        except Exception as e:
            logger.warning(f"格式化漲跌價差 {change_value} 時發生錯誤: {e}")
            return "X"
    
    def batch_format_stocks(self, stock_data_dict: dict) -> dict:
        """
        批量格式化多個股票的數據
        
        Args:
            stock_data_dict: 股票數據字典 {股票代碼: DataFrame}
            
        Returns:
            格式化結果統計
        """
        results = {
            'success': [],
            'failed': [],
            'total': len(stock_data_dict)
        }
        
        logger.info(f"開始批量格式化 {len(stock_data_dict)} 支股票的數據...")
        
        for stock_code, df in stock_data_dict.items():
            try:
                success = self.format_to_standard_csv(df, stock_code)
                if success:
                    results['success'].append(stock_code)
                else:
                    results['failed'].append(stock_code)
            except Exception as e:
                logger.error(f"格式化股票 {stock_code} 時發生錯誤: {e}")
                results['failed'].append(stock_code)
        
        logger.info(f"批量格式化完成: 成功 {len(results['success'])} 支, 失敗 {len(results['failed'])} 支")
        return results
    
    def get_sample_output(self, stock_code: str) -> str:
        """
        獲取樣本輸出格式
        
        Args:
            stock_code: 股票代碼
            
        Returns:
            樣本輸出字符串
        """
        sample_file = self.output_dir / f"{stock_code}.csv"
        
        if sample_file.exists():
            try:
                with open(sample_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                # 返回前5行作為樣本
                sample_lines = lines[:6]  # 包含標題行
                return "".join(sample_lines)
                
            except Exception as e:
                logger.error(f"讀取樣本文件 {sample_file} 時發生錯誤: {e}")
                return ""
        
        return ""

def main():
    """主函數 - 測試數據格式轉換"""
    formatter = StockDataFormatter()
    
    # 創建測試數據
    test_data = pd.DataFrame({
        'date': ['2024-02-13', '2024-02-14', '2024-02-15'],
        'volume': [7599922.0, 8000000.0, 7500000.0],
        'turnover': [528270219.0, 560000000.0, 525000000.0],
        'open': [69.35, 70.0, 69.5],
        'high': [69.65, 70.5, 70.0],
        'low': [69.35, 69.8, 69.0],
        'close': [69.45, 70.2, 69.8],
        'change': [0.45, 0.75, -0.4],
        'transactions': [1771.0, 1800.0, 1700.0]
    })
    
    # 測試格式化
    success = formatter.format_to_standard_csv(test_data, '2330')
    
    if success:
        print("數據格式化成功！")
        print("\n樣本輸出:")
        print(formatter.get_sample_output('2330'))
    else:
        print("數據格式化失敗！")

if __name__ == "__main__":
    main()
