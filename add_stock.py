#!/usr/bin/env python3
"""
添加股票到 stocks_config.txt
支持自動檢測股票市場類型（TSE/TPEX）
"""

import sys
import codecs
from pathlib import Path

# 移除編碼設置，讓系統自動處理

def get_stock_name_and_market(stock_code: str) -> tuple:
    """
    獲取股票名稱和市場類型
    
    Args:
        stock_code: 股票代碼
        
    Returns:
        (stock_name, market_type) 或 (None, 'UNKNOWN')
    """
    import requests
    import time
    
    # 已知股票名稱映射
    known_stock_names = {
        '6651': '全宇昕',
        '6922': '富強鑫',
        '3430': '奇鈦科',
        '4716': '大立光',
        '6158': '禾昌',
        '6425': '易通展',
        '2330': '台積電',
        '2317': '鴻海',
        '2881': '富邦金',
        '2454': '聯發科',
        '2308': '台達電',
        '6505': '台塑化',
        '2408': '南亞科',
        '2363': '矽統',
        '2344': '華邦電',
        '3260': '威剛',
        '3324': '雙鴻',
        '3535': '晶心科',
        '5443': '均豪',
        '2481': '強茂',
    }
    
    # 先檢查已知清單
    known_tpex_stocks = set()
    known_tse_stocks = set()
    config_file = Path("stocks_config.txt")
    
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    parts = line.strip().split(',')
                    if len(parts) >= 3:
                        code = parts[0].strip()
                        market = parts[2].strip()
                        if market == 'TPEX':
                            known_tpex_stocks.add(code)
                        elif market == 'TSE':
                            known_tse_stocks.add(code)
    
    # 如果已經在已知清單中，直接返回
    if stock_code in known_tpex_stocks:
        stock_name = known_stock_names.get(stock_code, f"股票{stock_code}")
        return (stock_name, 'TPEX')
    if stock_code in known_tse_stocks:
        stock_name = known_stock_names.get(stock_code, f"股票{stock_code}")
        return (stock_name, 'TSE')
    
    # 嘗試通過實際API檢測
    try:
        # 先嘗試TSE API
        tse_url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        tse_params = {
            'response': 'json',
            'date': '20240101',  # 使用一個固定日期
            'stockNo': stock_code
        }
        
        print(f"Checking TSE API...")
        response = requests.get(tse_url, params=tse_params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and data['data']:
                # 從TSE API獲取股票名稱
                stock_name = known_stock_names.get(stock_code, f"股票{stock_code}")
                print(f"Found stock {stock_code} in TSE")
                return (stock_name, 'TSE')
        
        time.sleep(1)  # 禮貌間隔
        
        # 再嘗試TPEX API
        tpex_url = f"https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
        tpex_params = {
            'code': stock_code,
            'date': '2024/01/01',
            'response': 'utf-8'
        }
        
        print(f"Checking TPEX API...")
        response = requests.get(tpex_url, params=tpex_params, timeout=10)
        if response.status_code == 200:
            # 檢查回應內容是否包含股票數據
            content = response.text
            if '成交股數' in content or '成交金額' in content or stock_code in content:
                # 從TPEX API獲取股票名稱
                stock_name = f"股票{stock_code}"  # 默認名稱
                
                # 嘗試從內容中提取股票名稱
                # TPEX API 格式: "個股日成交資訊 股票代號:6651 股票名稱:全宇昕 資料日期:113/06"
                if '股票名稱:' in content:
                    lines = content.split('\n')
                    for line in lines:
                        if '股票名稱:' in line:
                            # 提取股票名稱
                            parts = line.split('股票名稱:')
                            if len(parts) > 1:
                                name_part = parts[1].strip()
                                # 移除後面的空白和資料日期部分
                                if ' ' in name_part:
                                    stock_name = name_part.split()[0]
                                else:
                                    stock_name = name_part
                                
                                # 檢查提取的名稱是否有效（不是空字符串且不是股票代碼）
                                if stock_name and stock_name != stock_code and len(stock_name) > 1:
                                    # 成功提取到股票名稱
                                    break
                                else:
                                    # 提取失敗，使用已知映射或默認格式
                                    stock_name = known_stock_names.get(stock_code, f"股票{stock_code}")
                            else:
                                stock_name = known_stock_names.get(stock_code, f"股票{stock_code}")
                            break
                    else:
                        # 沒有找到股票名稱行，使用已知映射或默認格式
                        stock_name = known_stock_names.get(stock_code, f"股票{stock_code}")
                print(f"Found stock {stock_code} in TPEX")
                return (stock_name, 'TPEX')
        
        print(f"Stock {stock_code} not found in TSE or TPEX")
        return (None, 'UNKNOWN')
        
    except Exception as e:
        print(f"API detection failed: {e}")
        # 降級到簡單規則
        if len(stock_code) == 4 and stock_code.isdigit():
            stock_name = known_stock_names.get(stock_code, f"股票{stock_code}")
            return (stock_name, 'TSE')
        elif len(stock_code) == 3 and stock_code.isdigit():
            stock_name = known_stock_names.get(stock_code, f"股票{stock_code}")
            return (stock_name, 'TPEX')
        else:
            return (None, 'UNKNOWN')

def add_stock_to_config(stock_code: str, stock_name: str = None, market_type: str = None):
    """
    添加股票到 stocks_config.txt
    
    Args:
        stock_code: 股票代碼
        stock_name: 股票名稱（可選）
        market_type: 市場類型（可選，會自動檢測）
    """
    config_file = Path("stocks_config.txt")
    
    # 檢查股票是否已存在
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            content = f.read()
            if stock_code in content:
                print(f"Stock {stock_code} already exists in config file")
                return False
    
    # 自動檢測市場類型和股票名稱
    if market_type is None or stock_name is None:
        detected_name, detected_market = get_stock_name_and_market(stock_code)
        print(f"Auto-detected market type for stock {stock_code}: {detected_market}")
        
        if detected_market == 'UNKNOWN':
            print(f"Cannot determine market type for stock {stock_code}")
            return False
        
        if market_type is None:
            market_type = detected_market
        if stock_name is None:
            stock_name = detected_name
    
    # 添加新股票
    new_line = f"{stock_code},{stock_name},{market_type}\n"
    
    try:
        with open(config_file, 'a', encoding='utf-8') as f:
            f.write(new_line)
        
        print(f"Successfully added stock {stock_code} to {market_type} market")
        return True
        
    except Exception as e:
        print(f"Error adding stock: {e}")
        return False

def main():
    """主函數"""
    if len(sys.argv) < 2:
        print("Usage: python add_stock.py <stock_code> [stock_name] [market_type]")
        print("Example: python add_stock.py 2330 台積電 TSE")
        print("Example: python add_stock.py 2330  # auto-detect market type")
        return
    
    stock_code = sys.argv[1]
    stock_name = sys.argv[2] if len(sys.argv) > 2 else None
    market_type = sys.argv[3] if len(sys.argv) > 3 else None
    
    print(f"Adding stock: {stock_code}")
    if stock_name:
        print(f"Stock name: {stock_name}")
    if market_type:
        print(f"Market type: {market_type}")
    
    success = add_stock_to_config(stock_code, stock_name, market_type)
    
    if success:
        print("Stock added successfully!")
    else:
        print("Failed to add stock!")

if __name__ == "__main__":
    main()
