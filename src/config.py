"""
股票數據抓取工具配置檔案
包含所有全域設定、路徑和參數
"""

import os
from pathlib import Path

# 專案根目錄
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"

# 確保目錄存在
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 數據檔案路徑
RAW_PRICES_FILE = DATA_DIR / "raw_prices.csv"


def load_stocks_from_config() -> list:
    """
    從 stocks_config.txt 載入股票清單
    
    Returns:
        股票代碼列表
    """
    stocks_config_file = PROJECT_ROOT / "stocks_config.txt"
    
    if not stocks_config_file.exists():
        # 如果配置文件不存在，返回空列表
        return []
    
    stocks = []
    try:
        with open(stocks_config_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                
                # 跳過空行和註釋
                if not line or line.startswith('#'):
                    continue
                
                # 解析股票信息
                parts = line.split(',')
                if len(parts) >= 4:
                    stock_code = parts[0].strip()
                    stocks.append(stock_code)
        
        return stocks
        
    except Exception as e:
        print(f"載入股票配置失敗: {e}")
        return []

def get_target_stocks() -> list:
    """
    從 stocks_config.txt 載入目標股票清單
    
    Returns:
        目標股票代碼列表
    """
    stocks_config_file = PROJECT_ROOT / "stocks_config.txt"
    
    if not stocks_config_file.exists():
        return []
    
    target_stocks = []
    try:
        with open(stocks_config_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                
                # 跳過空行和註釋
                if not line or line.startswith('#'):
                    continue
                
                # 解析股票信息
                parts = line.split(',')
                if len(parts) >= 4:
                    stock_code = parts[0].strip()
                    is_target = parts[3].strip().upper() == 'Y'
                    
                    if is_target:
                        target_stocks.append(stock_code)
        
        return target_stocks
        
    except Exception as e:
        print(f"載入目標股票配置失敗: {e}")
        return []

def get_stocks_by_market() -> dict:
    """
    從 stocks_config.txt 載入股票並按市場分類
    
    Returns:
        字典格式: {'TSE': [股票代碼列表], 'TPEX': [股票代碼列表]}
    """
    stocks_config_file = PROJECT_ROOT / "stocks_config.txt"
    
    if not stocks_config_file.exists():
        return {'TSE': [], 'TPEX': []}
    
    tse_stocks = []
    tpex_stocks = []
    
    try:
        with open(stocks_config_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                
                # 跳過空行和註釋
                if not line or line.startswith('#'):
                    continue
                
                # 解析股票信息
                parts = line.split(',')
                if len(parts) >= 3:
                    stock_code = parts[0].strip()
                    market_type = parts[2].strip().upper()
                    
                    if market_type == 'TSE':
                        tse_stocks.append(stock_code)
                    elif market_type == 'TPEX':
                        tpex_stocks.append(stock_code)
        
        return {'TSE': tse_stocks, 'TPEX': tpex_stocks}
        
    except Exception as e:
        print(f"載入股票市場分類失敗: {e}")
        return {'TSE': [], 'TPEX': []}

# 數據收集參數
DATA_COLLECTION_CONFIG = {
    "STOCK_LIST": load_stocks_from_config(),  # 從 stocks_config.txt 動態載入
    "LOOKBACK_DAYS": 365,              # 回看天數（一年交易日，避免查詢過早日期）
}


# API 配置
API_CONFIG = {
    "TWSE_API": {
        "BASE_URL": "https://mis.twse.com.tw",
        "TIMEOUT": 30
    },
    "TPEX_API": {
        "BASE_URL": "https://www.tpex.org.tw",
        "TIMEOUT": 30
    }
}
