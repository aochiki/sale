import pandas as pd
import json
import io
import re
import fnmatch
import logging
import datetime
from aggregator.exchange_service import ExchangeRateService

logger = logging.getLogger(__name__)

class DataFormatter:
    def __init__(self, mappings_df, exchange_rates=None, exchange_service=None):
        self.mappings = mappings_df
        self.exchange_dict = {}
        if exchange_rates is not None and not exchange_rates.empty:
            for _, row in exchange_rates.iterrows():
                self.exchange_dict[row["currency_code"].upper()] = float(row["rate_to_jpy"])
        
        self.exchange_service = exchange_service or ExchangeRateService()

    def detect_source(self, filename, content_str=None):
        # 1. Filename-based (Strong hint)
        fn_lower = filename.lower()
        if "itunes" in fn_lower or "apple" in fn_lower:
            return "ITUNES"
        if "orchard" in fn_lower:
            return "ORCHARD"
        if "nextone" in fn_lower:
            return "NEXTONE"

        # 2. Content-based detection (Case-Insensitive)
        if content_str:
            lines = content_str.splitlines()[:50] # Increase search range
            sample = "\n".join(lines).lower()
            
            if "vendor identifier" in sample or "storefront name" in sample or "quantity" in sample:
                return "ITUNES"
            if "currency conversion rate" in sample or "net share account currency" in sample:
                return "ORCHARD"
            if ("配信元サイト名" in sample and "売上確定日" in sample) or ("isrc" in sample and "売上確定日" in sample):
                return "NEXTONE"
            if "youtube revenue split" in sample or "partner revenue" in sample or "adjustment type" in sample:
                return "YOUTUBE"
        
        return "UNKNOWN"

    def format_file(self, file_obj, filename, source_type=None, platforms_df=None):
        # 1. Read file content with multiple encoding attempts
        raw_data = file_obj.read()
        encodings = ['utf-8-sig', 'utf-16', 'cp932', 'utf-16-be', 'utf-8']
        content_str = None
        
        for enc in encodings:
            try:
                content_str = raw_data.decode(enc)
                if content_str.strip():
                    break
            except:
                continue
        
        if not content_str:
            content_str = raw_data.decode('utf-8', errors='replace')
        
        # 1.5. Handle GZIP if detected (Check for magic number)
        if raw_data.startswith(b'\x1f\x8b'):
            import gzip
            try:
                content_str = gzip.decompress(raw_data).decode('utf-8', errors='replace')
            except Exception as e:
                logger.error(f"Gzip decompression failed: {e}")
        
        # 2. Detect source
        if not source_type or source_type == "UNKNOWN":
            source_type = self.detect_source(filename, content_str)
        
        # NexTone特有の判定ロジック補強 (ファイル名だけでなく中身のキーワードも見る)
        if source_type == "UNKNOWN":
            c_low = content_str.lower()
            if "nextone" in c_low or "使用料合計" in c_low or "分配額" in c_low:
                source_type = "NEXTONE"
        
        # 3. Read into DataFrame
        try:
            df = self._read_raw_to_df(content_str, source_type)
        except Exception as e:
            logger.error(f"Read error: {e}")
            return None, [], []

        if df is None or df.empty:
            return None, [], []
        
        # 4. Apply mapping and normalization
        raw_columns = df.columns.tolist()
        new_df, unmapped_cols = self._apply_mapping(df, source_type, filename, platforms_df=platforms_df)
        
        return new_df, unmapped_cols, raw_columns

    def _read_raw_to_df(self, content_str, source_type):
        lines = content_str.splitlines()
        
        # Apple Music or UNKNOWN with possible metadata
        if source_type in ["ITUNES", "UNKNOWN"]:
            start_row = 0
            end_date = None
            df_metadata = {}
            header_keywords = ["vendor identifier", "provider identifier", "isrc", "upc", "storefront name", "country of sale", "quantity"]
            
            # Find best header row (max columns or keyword match)
            max_cols = -1
            max_cols_row = 0
            
            for i, line in enumerate(lines[:100]): # Search up to 100 lines
                l_low = line.lower()
                if "end date" in l_low:
                    parts = line.split('\t') if '\t' in line else line.split(',')
                    if len(parts) > 1:
                        end_date = parts[1].strip()
                        df_metadata['_extracted_end_date'] = end_date
                
                if "start date" in l_low:
                    parts = line.split('\t') if '\t' in line else line.split(',')
                    if len(parts) > 1:
                        start_date = parts[1].strip()
                        df_metadata['_extracted_start_date'] = start_date
                
                if "report type" in l_low:
                    parts = line.split('\t') if '\t' in line else line.split(',')
                    if len(parts) > 1:
                        report_type_val = parts[1].strip()
                        df_metadata['Report Type'] = report_type_val
                
                # Check keywords
                if any(k in l_low for k in header_keywords):
                    start_row = i
                    break
                
                # Check column count
                col_count = max(line.count('\t'), line.count(','))
                if col_count > max_cols:
                    max_cols = col_count
                    max_cols_row = i
            
            # If no keywords, use the row with most columns (if it has substantial content)
            if start_row == 0 and max_cols >= 2:
                start_row = max_cols_row
            
            # Detect separator on the detected header row
            if start_row < len(lines):
                header_line = lines[start_row]
                sep = ',' if header_line.count(',') > header_line.count('\t') else '\t'
            else:
                sep = '\t'

            # Find end of data rows (Appleレポート特有のフッター/サマリー探索)
            end_row = len(lines)
            summary_headers = ["country of sale", "storefront name", "currency", "share per converted unit", "royalty"]
            
            # 効率化のため、末尾に近い方から、あるいは一定行数ごとに探索する
            # ただし、フッターが非常に手前にある可能性も考慮し、データ行をスキップしすぎないようにする
            for i in range(start_row + 1, len(lines)):
                # 高速化のため、明らかなデータ行（日付で始まる等）はスキップ
                line = lines[i]
                if not line or line[0].isdigit(): continue
                
                l_low = line.lower()
                if any(k in l_low for k in ["total rows", "total_rows", "row count"]):
                    end_row = i
                    break
                if sum(1 for h in summary_headers if h in l_low) >= 2:
                    end_row = i
                    break
            
            valid_content = "\n".join(lines[start_row:end_row])
            df = pd.read_csv(io.StringIO(valid_content), sep=sep, low_memory=False, on_bad_lines='warn')
            
            # Extract Summary Table
            summary_df = None
            remaining_lines = lines[end_row:]
            for i, line in enumerate(remaining_lines):
                if sum(1 for h in summary_headers if h in line.lower()) >= 2:
                    summary_content = "\n".join(remaining_lines[i:])
                    summary_df = pd.read_csv(io.StringIO(summary_content), sep=sep, low_memory=False, on_bad_lines='warn')
                    break
            
            if summary_df is not None:
                df.attrs['summary_df'] = summary_df
            for k, v in df_metadata.items():
                df[k] = v
                
            return df
            
        elif source_type == "NEXTONE":
            return pd.read_csv(io.StringIO(content_str), sep='\t', skiprows=1, low_memory=False)
            
        elif source_type == "ORCHARD":
            sep = ',' if content_str.count(',') > content_str.count('\t') else '\t'
            return pd.read_csv(io.StringIO(content_str), sep=sep, low_memory=False)
            
        else:
            sep = '\t' if content_str.count('\t') > content_str.count(',') else ','
            return pd.read_csv(io.StringIO(content_str), sep=sep, low_memory=False)

    def _detect_apple_subtype(self, df):
        """Appleレポートのサブタイプを判別する (Financial / Sales / Other)"""
        cols_lower = [c.lower() for c in df.columns]
        
        # 1. Sales Report (ダウンロード売上): "Partner Share", "Sales or Return" 等が特徴的
        sales_indicators = ["partner share", "sales or return", "country of sale", "partner share currency", "extended partner share"]
        if any(ind in cols_lower for ind in sales_indicators):
            return "apple_sales_col"
        
        # 2. Financial Report (ストリーミング): "Net Royalty Total", "Total Royalty Bearing Plays" 等
        fin_indicators = ["net royalty total", "total  royalty bearing plays", "content title"]
        if any(ind in cols_lower for ind in fin_indicators):
            return "apple_fin_col"
        
        # デフォルトはFinancial
        return "apple_fin_col"

    def _apply_mapping(self, df, source_type, filename, platforms_df=None):
        # プラットフォームキーの解決
        col_key = None
        if source_type == "ITUNES":
            col_key = self._detect_apple_subtype(df)
        elif platforms_df is not None:
            # platforms_df から一致するものを探す
            match = platforms_df[platforms_df['name'].str.upper() == source_type.upper()]
            if not match.empty:
                col_key = match.iloc[0]['key']
        
        # フォールバック (互換性維持)
        if not col_key:
            source_col_map = {"ORCHARD": "orchard_col", "NEXTONE": "nextone_col", "YOUTUBE": "youtube_col"}
            col_key = source_col_map.get(source_type, "orchard_col")
        
        new_df = pd.DataFrame(index=df.index)
        new_df['SOURCE'] = source_type
        consumed_cols = set()
        
        for _, row in self.mappings.iterrows():
            u_name = row.get('unified_name')
            s_col = row.get(col_key)
            
            # col_key が存在しない場合のフォールバック
            if s_col is None and source_type == "ITUNES":
                s_col = row.get('itunes_col')
            
            val = None
            if u_name and s_col and s_col in df.columns:
                val = df[s_col].copy()
                consumed_cols.add(s_col)
            elif u_name and s_col == "End Date" and '_extracted_end_date' in df.columns:
                val = df['_extracted_end_date'].copy()
            elif u_name and s_col == "Start Date" and '_extracted_start_date' in df.columns:
                val = df['_extracted_start_date'].copy()
            elif u_name == "売上確定日" and '_extracted_end_date' in df.columns:
                val = df['_extracted_end_date'].copy()
            else:
                val = pd.Series([None] * len(df), index=df.index)
                
            if u_name and pd.notna(row.get('is_numeric')) and row.get('is_numeric'):
                if val.dtype == 'object':
                    # カンマ、円マーク、空白などを除去
                    val = val.astype(str).str.replace('[検,￥¥\\s]', '', regex=True)
                val = pd.to_numeric(val, errors='coerce').fillna(0)
            elif u_name and pd.notna(row.get('is_date')) and row.get('is_date'):
                val = self._normalize_date(val)
            elif u_name:
                val = val.fillna("").astype(str)
                val = val.replace("nan", "")
            
            if u_name:
                new_df[u_name] = val

        # --- カラム名の動的解決 ---
        # マッピング定義から、対象のソース列名に対応する「現在の統一名」を特定する
        all_col_keys = [c for c in self.mappings.columns if c.endswith('_col')]
        
        def find_unified_name(source_col_names):
            """現在のプラットフォームの列設定から、対象のソース列名に対応する統一名を検索する"""
            search_list = [s.upper().strip() for s in source_col_names]
            for _, m in self.mappings.iterrows():
                u = m.get('unified_name')
                val = str(m.get(col_key, "")).upper().strip()
                if val and val in search_list:
                    return u
            return None

        # ソース列名に基づいて動的に統一名を取得
        amount_col = find_unified_name(["NET SHARE ACCOUNT CURRENCY", "Net Royalty Total", "Partner Share", "Extended Partner Share", "Partner Revenue", "総支払額", "使用料合計", "分配額", "売上金額", "収益"]) or "収益"
        currency_col = find_unified_name(["ACCOUNT CURRENCY", "Currency", "Partner Share Currency", "通貨"]) or "通貨"
        qty_col = find_unified_name(["QUANTITY", "Total  Royalty Bearing Plays", "Quantity", "Owned Views", "数量", "利用回数", "回数"]) or "数量"
        country_col = find_unified_name(["SALE COUNTRY", "Storefront Name", "Country Of Sale", "Country", "国コード", "国"]) or "国コード"
        date_col = find_unified_name(["STATEMENT PERIOD", "End Date", "Day", "分配月", "利用月", "利用年月", "分配年月", "売上確定日"]) or "売上確定日"
        
        # 為替レート・JPY列・単価列
        rate_col = find_unified_name(["CURRENCY CONVERSION RATE", "為替レート"]) or "為替レート"
        unit_price_col = find_unified_name(["Share Per Converted Unit", "Non Spatial Available Per Play Royalty", "単価", "分配単価"]) or "ユニット単価"
        # JPY列は名前に "JPY" または "円" または "総支払額" を含む全ての列を対象とする
        jpy_cols = [str(m.get('unified_name')) for _, m in self.mappings.iterrows() if m.get('unified_name') and any(k in str(m.get('unified_name')).upper() for k in ['JPY', '円', '総支払額'])]
        if not jpy_cols: jpy_cols = ["収益(JPY)"]

        # Apple Music Royalty Completion (事前計算)
        if source_type == "ITUNES" and 'summary_df' in df.attrs:
            summary = df.attrs['summary_df']
            c_col = next((c for c in ["Country Of Sale", "Storefront Name"] if c in summary.columns), None)
            curr_col_s = next((c for c in ["Partner Share Currency", "Currency"] if c in summary.columns), None)
            # 対象のカラム（ユニット単価等）をマッピングから特定、またはデフォルト候補を使用
            r_col_candidates = ["Share Per Converted Unit", "Non Spatial Available Per Play Royalty"]
            # マッピング定義からソース列名を取得
            scol = next((str(m.get('apple_fin_col', "")) for _, m in self.mappings.iterrows() if m.get('unified_name') == unit_price_col), "")
            if scol and scol in summary.columns: r_col_candidates.insert(0, scol)
            
            r_col = next((c for c in r_col_candidates if c in summary.columns), None)

            if c_col and curr_col_s and r_col:
                # { (Country, Currency): Rate } のマップ
                rate_map = {}
                for _, r in summary.iterrows():
                    if pd.notna(r[r_col]):
                        try:
                            rate_map[(str(r[c_col]).strip(), str(r[curr_col_s]).strip())] = float(r[r_col])
                        except:
                            continue
                
                # Apple固有のレート補完 (ベクトル化による高速化)
                # 内部的なソース列名から直接解決を試みる
                s_country_col = next((c for c in ["Country Of Sale", "Storefront Name"] if c in df.columns), None)
                s_currency_col = next((c for c in ["Partner Share Currency", "Currency"] if c in df.columns), None)
                s_qty_col = next((c for c in ["Quantity", "Total  Royalty Bearing Plays"] if c in df.columns), None)
                
                if s_country_col and s_currency_col and s_qty_col:
                    # 国と通貨の組み合わせキーを作成
                    comb_keys = df[s_country_col].astype(str).str.strip() + "_" + df[s_currency_col].astype(str).str.strip()
                    flat_rate_map = {f"{k[0]}_{k[1]}": v for k, v in rate_map.items()}
                    mapped_rates = comb_keys.map(flat_rate_map)
                    
                    if amount_col in new_df.columns:
                        amounts = pd.to_numeric(new_df[amount_col], errors='coerce').fillna(0)
                        qtys = pd.to_numeric(df[s_qty_col], errors='coerce').fillna(0)
                        
                        # 補完が必要なインデックス (収益が0かつレートが存在する場合)
                        mask = (amounts == 0) & (mapped_rates.notna())
                        new_df.loc[mask, amount_col] = qtys[mask] * mapped_rates[mask]
                        
                        # レート列の更新 (既存が0の場合のみ)
                        if rate_col in new_df.columns:
                            current_rates = pd.to_numeric(new_df[rate_col], errors='coerce').fillna(0)
                            rate_mask = (current_rates == 0) & (mapped_rates.notna())
                            new_df.loc[rate_mask, rate_col] = mapped_rates[rate_mask]

        # --- JPY Conversion 高速化 (ベクトル化処理) ---
        # jpy_col が見つからなくても、amount_col と currency_col があれば計算を試みる
        if amount_col in new_df.columns and currency_col in new_df.columns:
            # 1. ユニークな (通貨, 日付) のペアを抽出
            dates_series = pd.to_datetime(new_df[date_col], errors='coerce')
            date_keys = dates_series.dt.date.fillna(datetime.date.today())
            currency_keys = new_df[currency_col].astype(str).str.upper().str.strip()
            
            unique_pairs = pd.DataFrame({'curr': currency_keys, 'dt': date_keys}).drop_duplicates()
            
            # 2. レートを事前解決 (新機能があれば一括、なければ一つずつ)
            currency_date_pairs = list(zip(currency_keys, date_keys))
            if hasattr(self.exchange_service, 'get_rates_batch'):
                resolved_rates = self.exchange_service.get_rates_batch(currency_date_pairs)
            else:
                # 古いキャッシュが残っている場合のフォールバック
                resolved_rates = {}
                for curr, dt in set(currency_date_pairs):
                    if curr in ["", "NAN", "NONE", "JPY"]:
                        resolved_rates[(curr, dt)] = 1.0
                    else:
                        resolved_rates[(curr, dt)] = self.exchange_service.get_rate(curr, dt)
            
            # 3. ベクトル化: タプルキーでmapを使って一括適用
            lookup_keys = list(zip(currency_keys, date_keys))
            rate_series = pd.Series([resolved_rates.get(k, 1.0) for k in lookup_keys], index=new_df.index)
            
            # 手動レート(Orchard等)の考慮
            if rate_col in new_df.columns:
                manual_rates = pd.to_numeric(new_df[rate_col], errors='coerce').fillna(0)
                # 10.0以上の手動レートがあれば優先（Apple以外等）
                rate_series = manual_rates.where(manual_rates > 10.0, rate_series)
            
            # 収益(JPY)の計算
            amounts = pd.to_numeric(new_df[amount_col], errors='coerce').fillna(0)
            for j_col in jpy_cols:
                if j_col not in new_df.columns:
                    new_df[j_col] = 0.0
                new_df[j_col] = amounts * rate_series
            if rate_col in new_df.columns:
                new_df[rate_col] = rate_series

        # 備考欄生成の高速化 (iterrowsを避け、to_dictを利用)
        unmapped_cols = [c for c in df.columns if c not in consumed_cols and c != '_extracted_end_date']
        if '備考' not in new_df.columns: new_df['備考'] = ""
        if unmapped_cols:
            df_unmapped = df[unmapped_cols].copy()
            # NaNを除去しつつ辞書形式のリストへ変換
            records = df_unmapped.to_dict('records')
            remarks = [
                json.dumps({k: str(v) for k, v in r.items() if pd.notna(v) and str(v).strip() != ""}, ensure_ascii=False)
                for r in records
            ]
            new_df['備考'] = remarks
        
        return new_df, unmapped_cols

    def _normalize_date(self, s):
        if s is None: return s
        if isinstance(s, pd.Series):
            if s.isna().all(): return s
            # 文字列化して余分な空白を除去
            s_str = s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
            # 8桁の数値形式 (20260227) を YYYY-MM-DD に変換
            s_str = s_str.str.replace(r'^(\d{4})(\d{2})(\d{2})$', r'\1-\2-\3', regex=True)
            # 6桁の数値形式 (202602) を YYYY-MM-01 に変換
            s_str = s_str.str.replace(r'^(\d{4})(\d{2})$', r'\1-\2-01', regex=True)
            # 無効な値（0など）を空文字にする
            s_str = s_str.replace(r'^[0\s-]+$', '', regex=True)
            
            return pd.to_datetime(s_str, errors='coerce').dt.strftime('%Y-%m-%d').fillna("")
        return s
