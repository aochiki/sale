
import requests
import datetime
import logging
from typing import Dict, Optional

class ExchangeRateService:
    """
    Frankfurter APIを利用して為替レートを取得するサービス。
    キャッシュ機能を持ち、APIへの過剰なアクセスを防止する。
    """
    def __init__(self, base_currency: str = "JPY"):
        self.base_url = "https://api.frankfurter.app"
        self.base_currency = base_currency
        self.cache: Dict[str, Dict[str, float]] = {} # { "2026-02-15": {"AUD": 100.0, "USD": 150.0} }

    def get_rate(self, from_currency: str, date: Optional[datetime.date] = None) -> float:
        """
        指定された日付の from_currency から JPY へのレートを取得する。
        1 [from_currency] = [result] JPY
        """
        if not from_currency or from_currency.strip() == "" or from_currency.upper() in ["NAN", "NONE"]:
            return 1.0
            
        from_currency = from_currency.upper().strip()
        if from_currency == self.base_currency:
            return 1.0
        
        # 日付が指定されていない場合は今日
        if date is None:
            date = datetime.date.today()
        
        # Frankfurter APIは自動的に直近の営業日の値を返す。
        date_str = date.isoformat()
        
        # キャッシュ確認
        if date_str in self.cache and from_currency in self.cache[date_str]:
            return self.cache[date_str][from_currency]
        
        # セッションの初期化 (Connection Pooling)
        if not hasattr(self, '_session'):
            import requests
            self._session = requests.Session()

        try:
            url = f"{self.base_url}/{date_str}"
            params = {"from": from_currency, "to": self.base_currency}
            
            response = self._session.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                rate = data.get("rates", {}).get(self.base_currency)
                if rate:
                    if date_str not in self.cache:
                        self.cache[date_str] = {}
                    self.cache[date_str][from_currency] = float(rate)
                    return float(rate)
            else:
                logging.warning(f"API Error: {response.status_code} for {from_currency} on {date_str}")
        except Exception as e:
            logging.error(f"Failed to fetch exchange rate: {e}")
        
        return 1.0 # 失敗時のフォールバック

    def get_rates_batch(self, currency_date_pairs):
        """複数の通貨・日付ペアのレートを並列で取得する"""
        from concurrent.futures import ThreadPoolExecutor
        
        results = {}
        def fetch_one(pair):
            curr, dt = pair
            return pair, self.get_rate(curr, dt)

        # 重複を除去
        unique_pairs = list(set(currency_date_pairs))
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            for pair, rate in executor.map(fetch_one, unique_pairs):
                results[pair] = rate
        return results
