# 股票數據爬蟲工具

這是一個獨立的股票數據爬蟲工具，可以從 [台灣證券交易所](https://www.twse.com.tw/) 和 [證券櫃檯買賣中心](https://www.tpex.org.tw/) 抓取台股數據。

##  快速開始

### 1. 安裝依賴套件

```bash
pip install pandas requests
```

### 2. 運行主程式

```bash
python main.py
```

運行後會顯示互動式選單：

```
=== 股票數據抓取工具 ===
選擇操作：
1. 抓取所有股票數據
2. 抓取指定股票數據
3. 增量更新股票數據
4. 按日期範圍抓取所有股票數據
5. 按日期範圍抓取指定股票數據
6. 往回爬指定天數
7. 測試API連接
8. 退出
```

##  專案結構

```
stock_data_fetcher/
├── main.py                    # 主程式入口
├── stocks_config.txt          # 股票清單配置
├── requirements.txt           # 依賴套件
├── add_stock.py              # 股票管理工具
├── data/                     # 數據目錄
│   ├── [股票代碼].csv        # 個別股票數據
│   └── date_range_*/         # 日期範圍專用目錄
└── src/                     # 核心程式
    ├── __init__.py
    ├── config.py
    ├── unified_official_fetcher.py  # 統一數據收集器
    ├── official_twse_fetcher.py     # TWSE 上市股票抓取器
    ├── official_tpex_fetcher.py     # TPEX 上櫃股票抓取器
    └── data_formatter.py            # 數據格式化
```

##  使用方式

### 方式一：互動式操作（推薦）

```bash
python main.py
```

### 方式二：程式化使用

```python
from src.unified_official_fetcher import UnifiedOfficialFetcher

# 初始化收集器
fetcher = UnifiedOfficialFetcher()

# 抓取所有股票數據
stocks_data = fetcher.fetch_all_stocks()

# 按日期範圍抓取
stocks_data = fetcher.fetch_all_stocks_by_date_range('2024-01-01', '2024-01-31')

# 抓取指定股票
stock_data = fetcher.fetch_stock_data_by_date_range('2330', '2024-01-01', '2024-01-31')

# 往回爬指定天數
stock_data = fetcher.fetch_stock_data_backward_days('2330', 30)
```


##  功能特色

### 1. 統一介面
- 自動選擇 TWSE 或 TPEX API
- 支援任意股票代碼（自動檢測市場類型）
- 一個介面處理上市和上櫃股票

### 2. 靈活日期範圍
- **按日期範圍抓取所有股票**：創建獨立目錄保存
- **按日期範圍抓取指定股票**：支援任意股票代碼
- **往回爬指定天數**：自動下載缺失的歷史數據

### 3. 智能數據管理
- **增量更新**：只更新需要的股票數據
- **獨立目錄**：日期範圍抓取不覆蓋現有數據
- **自動檢測**：不在配置清單中的股票自動檢測市場類型

### 4. 數據格式化
- 標準化數據格式
- 民國年與西元年轉換
- 每支股票獨立 CSV 檔案



##  股票管理

### 添加新股票

```bash
# 自動檢測市場類型
python add_stock.py 2330

# 手動指定市場類型
python add_stock.py 2330 台積電 TSE
```

### 編輯股票清單

編輯 `stocks_config.txt` 檔案：

```
# 格式：股票代碼,股票名稱,市場類型,是否目標股票
# 市場類型：TSE=上市, TPEX=上櫃
# 是否目標股票：Y=是, N=否

2330,台積電,TSE,N
2317,鴻海,TSE,N
3260,威剛,TPEX,Y
```

##  輸出檔案

### 標準數據
- `data/[股票代碼].csv` - 個別股票的格式化數據

### 日期範圍數據
- `data/date_range_YYYYMMDD_YYYYMMDD/[股票代碼].csv` - 日期範圍專用目錄

##  常見問題

### Q: 無法連接 API？
A: 請檢查網路連接，或稍後再試

### Q: 某些股票抓不到數據？
A: 可能是股票代碼錯誤或該股票已下市

### Q: 數據格式問題？
A: 請檢查 `stocks_config.txt` 格式是否正確，可能是分類錯誤

### Q: 如何添加新股票？
A: 使用 `python add_stock.py [股票代碼]` 或手動編輯配置文件

### Q: 支援任意股票代碼嗎？
A: 是的！系統會自動檢測股票市場類型，無需預先配置

##  免責聲明

本軟體僅供學習和研究使用，使用者需自行承擔使用風險：

1. **數據準確性**：本工具提供的數據僅供參考，不保證其準確性、完整性或及時性
2. **投資風險**：任何基於本工具數據的投資決策，使用者需自行承擔風險
3. **法律責任**：使用者需遵守相關法律法規，作者不承擔任何法律責任
4. **服務中斷**：作者不保證服務的連續性和穩定性
5. **數據來源**：數據來源於公開的官方 API，如有變更或中斷，本工具可能無法正常運作

**使用本軟體即表示您同意上述免責聲明。**

##  注意事項

1. **網路連接**：需要穩定的網路連接
2. **API 限制**：避免過於頻繁的請求
3. **數據準確性**：使用官方 API 確保數據準確
4. **用途限制**：僅供學習和研究使用

##  支援

如有問題，請檢查：
1. 網路連接狀態
2. 股票代碼是否正確
3. 配置文件格式是否正確
