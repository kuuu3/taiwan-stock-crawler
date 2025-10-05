"""
股票數據抓取工具核心模組
Stock Data Fetcher Core Modules
"""

__version__ = "1.0.0"
__author__ = "Stock Data Fetcher Team"

# 導入主要類別
from .config import (
    DATA_COLLECTION_CONFIG,
    get_target_stocks,
    get_stocks_by_market,
    load_stocks_from_config
)

from .unified_official_fetcher import UnifiedOfficialFetcher
from .official_twse_fetcher import OfficialTWSEFetcher
from .official_tpex_fetcher import OfficialTPEXFetcher
from .data_formatter import StockDataFormatter

__all__ = [
    'UnifiedOfficialFetcher',
    'OfficialTWSEFetcher', 
    'OfficialTPEXFetcher',
    'StockDataFormatter',
    'DATA_COLLECTION_CONFIG',
    'get_target_stocks',
    'get_stocks_by_market',
    'load_stocks_from_config'
]
