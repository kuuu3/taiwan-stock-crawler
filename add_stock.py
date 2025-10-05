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
    
    # 先檢查配置檔案中是否已存在
    config_file = Path("stocks_config.txt")
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    parts = line.strip().split(',')
                    if len(parts) >= 3 and parts[0].strip() == stock_code:
                        market = parts[2].strip()
                        stock_name = parts[1].strip() if len(parts) > 1 else f"股票{stock_code}"
                        return (stock_name, market)
    
    # 嘗試通過實際API檢測
    try:
        # 先嘗試TSE API
        tse_url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
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
                print(f"Found stock {stock_code} in TSE")
                return (f"股票{stock_code}", 'TSE')
        
        time.sleep(1)  # 禮貌間隔
        
        # 再嘗試TPEX API
        tpex_url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
        tpex_params = {
            'code': stock_code,
            'date': '2024/01/01',
            'response': 'utf-8'
        }
        
        print(f"Checking TPEX API...")
        response = requests.get(tpex_url, params=tpex_params, timeout=10)
        if response.status_code == 200:
            content = response.text
            if '成交股數' in content or '成交金額' in content or stock_code in content:
                # 嘗試從內容中提取股票名稱
                stock_name = f"股票{stock_code}"  # 預設名稱
                
                if '股票名稱:' in content:
                    lines = content.split('\n')
                    for line in lines:
                        if '股票名稱:' in line:
                            parts = line.split('股票名稱:')
                            if len(parts) > 1:
                                name_part = parts[1].strip()
                                if ' ' in name_part:
                                    extracted_name = name_part.split()[0]
                                else:
                                    extracted_name = name_part
                                
                                # 檢查提取的名稱是否有效
                                if extracted_name and extracted_name != stock_code and len(extracted_name) > 1:
                                    stock_name = extracted_name
                                    break
                            break
                
                print(f"Found stock {stock_code} in TPEX")
                return (stock_name, 'TPEX')
        
        print(f"Stock {stock_code} not found in TSE or TPEX")
        return (None, 'UNKNOWN')
        
    except Exception as e:
        print(f"API detection failed: {e}")
        # 降級到簡單規則
        if len(stock_code) == 4 and stock_code.isdigit():
            return (f"股票{stock_code}", 'TSE')
        elif len(stock_code) == 3 and stock_code.isdigit():
            return (f"股票{stock_code}", 'TPEX')
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

def interactive_add_stocks():
    """互動式新增股票"""
    print("=== 互動式股票新增 ===")
    print("支援以下輸入格式：")
    print("1. 單一股票代碼: 2330")
    print("2. 多個股票代碼: 2330,2317,2454")
    print("3. 空格分隔: 2330 2317 2454")
    print("4. 輸入 'quit' 或 'exit' 退出")
    print()
    
    while True:
        user_input = input("請輸入股票代碼: ").strip()
        
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("退出程式")
            break
        
        if not user_input:
            print("請輸入有效的股票代碼")
            continue
        
        # 解析輸入
        stock_codes = []
        
        # 檢查是否包含逗號分隔
        if ',' in user_input:
            stock_codes = [code.strip() for code in user_input.split(',') if code.strip()]
        # 檢查是否包含空格分隔
        elif ' ' in user_input:
            stock_codes = [code.strip() for code in user_input.split() if code.strip()]
        else:
            # 單一股票代碼
            stock_codes = [user_input]
        
        # 驗證股票代碼格式
        valid_codes = []
        for code in stock_codes:
            if code.isdigit() and (len(code) == 3 or len(code) == 4):
                valid_codes.append(code)
            else:
                print(f"警告: {code} 不是有效的股票代碼格式")
        
        if valid_codes:
            print(f"\n即將新增以下股票: {', '.join(valid_codes)}")
            
            # 批次新增
            success_count = 0
            failed_count = 0
            
            for i, stock_code in enumerate(valid_codes, 1):
                print(f"[{i}/{len(valid_codes)}] 處理股票代碼: {stock_code}")
                
                try:
                    success = add_stock_to_config(stock_code)
                    if success:
                        success_count += 1
                        print(f"✓ {stock_code} 新增成功")
                    else:
                        failed_count += 1
                        print(f"✗ {stock_code} 新增失敗")
                except Exception as e:
                    failed_count += 1
                    print(f"✗ {stock_code} 處理錯誤: {e}")
                
                print("-" * 30)
            
            print("=" * 50)
            print(f"批次新增完成！")
            print(f"成功: {success_count} 個")
            print(f"失敗: {failed_count} 個")
        else:
            print("沒有找到有效的股票代碼")
        
        print()
        print(" 提示：輸入 'quit'、'exit' 或 'q' 可以退出互動模式")
        print()

def main():
    """主函數"""
    if len(sys.argv) < 2:
        print("=== 股票新增工具 ===")
        print()
        print("使用方式：")
        print("1. 單一股票: python add_stock.py <stock_code> [stock_name] [market_type]")
        print("2. 互動模式: python add_stock.py")
        print()
        print("範例：")
        print("  python add_stock.py 2330")
        print("  python add_stock.py")
        print()
        
        # 詢問是否進入互動模式
        choice = input("是否進入互動模式？(y/n): ").strip().lower()
        if choice in ['y', 'yes', '是']:
            interactive_add_stocks()
        return
    
    stock_code = sys.argv[1]
    stock_name = sys.argv[2] if len(sys.argv) > 2 else None
    market_type = sys.argv[3] if len(sys.argv) > 3 else None
    
    print(f"新增股票: {stock_code}")
    if stock_name:
        print(f"股票名稱: {stock_name}")
    if market_type:
        print(f"市場類型: {market_type}")
    
    success = add_stock_to_config(stock_code, stock_name, market_type)
    
    if success:
        print("股票新增成功！")
    else:
        print("股票新增失敗！")

if __name__ == "__main__":
    main()
