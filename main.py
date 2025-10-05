#!/usr/bin/env python3
"""
股票數據抓取工具主程式
"""

import sys
import os
from pathlib import Path

# 添加當前目錄到 Python 路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.unified_official_fetcher import UnifiedOfficialFetcher
from src.data_formatter import StockDataFormatter

def show_menu():
    """顯示選單"""
    print("\n=== 股票數據抓取工具 ===")
    print("選擇操作：")
    print("1. 抓取所有股票數據(預設365天)")
    print("2. 抓取指定股票數據")
    print("3. 按日期範圍抓取所有股票")
    print("4. 按日期範圍抓取指定股票")
    print("5. 往回爬指定天數")
    print("6. 增量更新股票數據")
    print("7. 測試API連接")
    print("8. 退出")

def main():
    """主函數"""
    show_menu()
    
    while True:
        try:
            choice = input("\n請選擇操作 (1-8): ").strip()
            
            if choice == "1":
                fetch_all_stocks()
                show_menu()
            elif choice == "2":
                fetch_specific_stock()
                show_menu()
            elif choice == "3":
                fetch_all_stocks_by_date_range()
                show_menu()
            elif choice == "4":
                fetch_specific_stock_by_date_range()
                show_menu()
            elif choice == "5":
                fetch_stock_backward_days()
                show_menu()
            elif choice == "6":
                incremental_update()
                show_menu()
            elif choice == "7":
                test_connections()
                show_menu()
            elif choice == "8":
                print("再見！")
                break
            else:
                print("無效選擇，請輸入 1-8")
                show_menu()
                
        except KeyboardInterrupt:
            print("\n\n程式被中斷，再見！")
            break
        except Exception as e:
            print(f"發生錯誤: {e}")

def fetch_all_stocks():
    """抓取所有股票數據"""
    print("\n=== 抓取所有股票數據 ===")
    
    fetcher = UnifiedOfficialFetcher()
    
    # 顯示股票清單
    stock_list = fetcher.stock_list
    print(f"將抓取 {len(stock_list)} 支股票:")
    print(f"股票清單: {', '.join(stock_list[:10])}{'...' if len(stock_list) > 10 else ''}")
    
    confirm = input("確認開始抓取？(y/N): ").strip().lower()
    if confirm != 'y':
        print("取消抓取")
        return
    
    print("開始抓取...")
    stocks_data = fetcher.fetch_all_stocks()
    
    if stocks_data is not None and len(stocks_data) > 0:
        print(f" 成功抓取 {len(stocks_data)} 支股票數據")
        print(f"每支股票數據已保存為獨立CSV檔案到: data/")
        
        # 顯示成功抓取的股票
        print(f"成功抓取的股票: {', '.join(stocks_data.keys())}")
    else:
        print(" 股票數據抓取失敗")

def fetch_specific_stock():
    """抓取指定股票數據"""
    print("\n=== 抓取指定股票數據 ===")
    
    stock_code = input("請輸入股票代碼 (例如: 2330): ").strip()
    
    if not stock_code:
        print("股票代碼不能為空")
        return
    
    fetcher = UnifiedOfficialFetcher()
    
    days = input("請輸入回看天數 (預設30天): ").strip()
    try:
        days = int(days) if days else 30
    except ValueError:
        days = 30
    
    print(f"抓取股票 {stock_code} 的 {days} 天數據...")
    
    stock_data = fetcher.fetch_stock_data(stock_code, days)
    
    if stock_data is not None and not stock_data.empty:
        print(f" 成功抓取股票 {stock_code} 的數據")
        print(f"數據筆數: {len(stock_data)}")
        print("\n數據預覽：")
        print(stock_data.head())
        
        # 格式化並保存數據
        formatter = StockDataFormatter()
        success = formatter.format_to_standard_csv(stock_data, stock_code)
        
        if success:
            print(f" 數據已保存至: data/{stock_code}.csv")
        else:
            print(" 數據格式化失敗")
    else:
        print(f" 無法抓取股票 {stock_code} 的數據")

def fetch_all_stocks_by_date_range():
    """按日期範圍抓取所有股票數據"""
    print("\n=== 按日期範圍抓取所有股票數據 ===")
    
    fetcher = UnifiedOfficialFetcher()
    
    # 獲取開始日期
    start_date = input("請輸入開始日期 (格式: YYYY-MM-DD，例如: 2024-01-01): ").strip()
    if not start_date:
        print("開始日期不能為空")
        return
    
    # 驗證日期格式
    try:
        from datetime import datetime
        datetime.strptime(start_date, '%Y-%m-%d')
    except ValueError:
        print("日期格式錯誤，請使用 YYYY-MM-DD 格式")
        return
    
    # 獲取結束日期
    end_date = input("請輸入結束日期 (格式: YYYY-MM-DD，留空表示到最新日期): ").strip()
    if end_date:
        try:
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            print("日期格式錯誤，請使用 YYYY-MM-DD 格式")
            return
    
    # 顯示股票清單
    stock_list = fetcher.stock_list
    print(f"將抓取 {len(stock_list)} 支股票:")
    print(f"股票清單: {', '.join(stock_list[:10])}{'...' if len(stock_list) > 10 else ''}")
    print(f"日期範圍: {start_date} 到 {end_date or '最新日期'}")
    
    confirm = input("確認開始抓取？(y/N): ").strip().lower()
    if confirm != 'y':
        print("取消抓取")
        return
    
    print("開始抓取...")
    stocks_data = fetcher.fetch_all_stocks_by_date_range(start_date, end_date)
    
    if stocks_data and len(stocks_data) > 0:
        print(f" 成功抓取 {len(stocks_data)} 支股票數據")
        
        # 顯示保存位置
        from datetime import datetime
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
        start_str = start_dt.strftime('%Y%m%d')
        end_str = end_dt.strftime('%Y%m%d')
        date_range_dir = f"data/date_range_{start_str}_{end_str}/"
        print(f"每支股票數據已保存為獨立CSV檔案到: {date_range_dir}")
        
        # 顯示成功抓取的股票
        print(f"成功抓取的股票: {', '.join(stocks_data.keys())}")
    else:
        print(" 股票數據抓取失敗")

def fetch_specific_stock_by_date_range():
    """按日期範圍抓取指定股票數據"""
    print("\n=== 按日期範圍抓取指定股票數據 ===")
    
    stock_code = input("請輸入股票代碼 (例如: 2330): ").strip()
    if not stock_code:
        print("股票代碼不能為空")
        return
    
    # 獲取開始日期
    start_date = input("請輸入開始日期 (格式: YYYY-MM-DD，例如: 2024-01-01): ").strip()
    if not start_date:
        print("開始日期不能為空")
        return
    
    # 驗證日期格式
    try:
        from datetime import datetime
        datetime.strptime(start_date, '%Y-%m-%d')
    except ValueError:
        print("日期格式錯誤，請使用 YYYY-MM-DD 格式")
        return
    
    # 獲取結束日期
    end_date = input("請輸入結束日期 (格式: YYYY-MM-DD，留空表示到最新日期): ").strip()
    if end_date:
        try:
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            print("日期格式錯誤，請使用 YYYY-MM-DD 格式")
            return
    
    fetcher = UnifiedOfficialFetcher()
    
    print(f"抓取股票 {stock_code} 的數據 (日期範圍: {start_date} 到 {end_date or '最新日期'})...")
    
    stock_data = fetcher.fetch_stock_data_by_date_range(stock_code, start_date, end_date)
    
    if stock_data is not None and not stock_data.empty:
        print(f" 成功抓取股票 {stock_code} 的數據")
        print(f"數據筆數: {len(stock_data)}")
        print("\n數據預覽：")
        print(stock_data.head())
        
        # 格式化並保存數據
        formatter = StockDataFormatter()
        success = formatter.format_to_standard_csv(stock_data, stock_code)
        
        if success:
            print(f" 數據已保存至: data/{stock_code}.csv")
        else:
            print(" 數據格式化失敗")
    else:
        print(f" 無法抓取股票 {stock_code} 的數據")

def fetch_stock_backward_days():
    """往回爬指定天數"""
    print("\n=== 往回爬指定天數 ===")
    
    stock_code = input("請輸入股票代碼 (例如: 2330): ").strip()
    if not stock_code:
        print("股票代碼不能為空")
        return
    
    days = input("請輸入往回爬的天數 (例如: 30): ").strip()
    try:
        days = int(days)
        if days <= 0:
            print("天數必須大於0")
            return
    except ValueError:
        print("天數必須是數字")
        return
    
    fetcher = UnifiedOfficialFetcher()
    
    print(f"抓取股票 {stock_code} 的數據 (往回 {days} 天到最新日期)...")
    
    stock_data = fetcher.fetch_stock_data_backward_days(stock_code, days)
    
    if stock_data is not None and not stock_data.empty:
        print(f" 成功抓取股票 {stock_code} 的數據")
        print(f"數據筆數: {len(stock_data)}")
        print("\n數據預覽：")
        print(stock_data.head())
        
        # 格式化並保存數據
        formatter = StockDataFormatter()
        success = formatter.format_to_standard_csv(stock_data, stock_code)
        
        if success:
            print(f" 數據已保存至: data/{stock_code}.csv")
        else:
            print(" 數據格式化失敗")
    else:
        print(f" 無法抓取股票 {stock_code} 的數據")

def incremental_update():
    """增量更新股票數據"""
    print("\n=== 增量更新股票數據 ===")
    
    fetcher = UnifiedOfficialFetcher()
    
    # 檢查需要更新的股票
    need_update = fetcher.check_stocks_need_update()
    total_need_update = len(need_update['tse_stocks']) + len(need_update['tpex_stocks'])
    
    if total_need_update == 0:
        print(" 所有股票數據都是最新的，無需更新")
        return
    
    print(f"發現 {total_need_update} 支股票需要更新")
    print(f"TSE股票: {len(need_update['tse_stocks'])} 支")
    print(f"TPEX股票: {len(need_update['tpex_stocks'])} 支")
    
    confirm = input("確認開始增量更新？(y/N): ").strip().lower()
    if confirm != 'y':
        print("取消更新")
        return
    
    print("開始增量更新...")
    results = fetcher.fetch_and_format_incremental()
    
    print(f" 更新完成！")
    print(f"成功更新: {len(results['success'])} 支股票")
    print(f"更新失敗: {len(results['failed'])} 支股票")
    print(f"跳過: {results['skipped']} 支股票")

def test_connections():
    """測試API連接"""
    print("\n=== 測試API連接 ===")
    
    fetcher = UnifiedOfficialFetcher()
    results = fetcher.test_connections()
    
    print("API連接測試結果:")
    print(f"TWSE: {' 成功' if results['twse'] else ' 失敗'}")
    print(f"TPEX: {' 成功' if results['tpex'] else ' 失敗'}")
    
    if any(results.values()):
        print("\n 至少有一個API可用")
        
        # 顯示數據源信息
        info = fetcher.get_data_source_info()
        print(f"\n數據源信息:")
        print(f"總股票數: {info['total_stocks']}")
        print(f"回看天數: {info['lookback_days']}")
        print(f"TSE股票: {len(info['twse_stocks'])} 支")
        print(f"TPEX股票: {len(info['tpex_stocks'])} 支")
    else:
        print("\n 所有API都無法連接，請檢查網路")

if __name__ == "__main__":
    main()

