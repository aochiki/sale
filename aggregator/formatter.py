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
    def __init__(self, mappings_df, exchange_rates=None):
        self.mappings = mappings_df
        self.exchange_dict = {}
        if exchange_rates is not None and not exchange_rates.empty:
            for _, row in exchange_rates.iterrows():
                self.exchange_dict[row["currency_code"].upper()] = float(row["rate_to_jpy"])
        
        self.exchange_service = ExchangeRateService()

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
        
        return "UNKNOWN"

    def format_file(self, file_obj, filename, source_type=None):
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
        
        # 2. Detect source
        if not source_type or source_type == "UNKNOWN":
            source_type = self.detect_source(filename, content_str)
        
        # 3. Read into DataFrame
        try:
            df = self._read_raw_to_df(content_str, source_type)
        except Exception as e:
            logger.error(f"Read error: {e}")
            return None, []

        if df is None or df.empty:
            return None, []
        
        # 4. Apply mapping and normalization
        new_df, unmapped_cols = self._apply_mapping(df, source_type, filename)
        
        return new_df, unmapped_cols

    def _read_raw_to_df(self, content_str, source_type):
        lines = content_str.splitlines()
        
        # Apple Music or UNKNOWN with possible metadata
        if source_type in ["ITUNES", "UNKNOWN"]:
            start_row = 0
            end_date = None
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
            if start_row == 0 and max_cols > 5:
                start_row = max_cols_row
            
            # Detect separator on the detected header row
            if start_row < len(lines):
                header_line = lines[start_row]
                sep = ',' if header_line.count(',') > header_line.count('\t') else '\t'
            else:
                sep = '\t'

            # Find end of data rows
            end_row = len(lines)
            summary_headers = ["country of sale", "storefront name", "currency", "share per converted unit", "royalty"]
            for i in range(start_row + 1, len(lines)):
                l_low = lines[i].lower()
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
            if end_date:
                df['_extracted_end_date'] = end_date
                
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

    def _apply_mapping(self, df, source_type, filename):
        # Appleの場合は3つのサブカラムから適切なものを選ぶ
        if source_type == "ITUNES":
            col_key = self._detect_apple_subtype(df)
        else:
            source_col_map = {"ORCHARD": "orchard_col", "NEXTONE": "nextone_col"}
            col_key = source_col_map.get(source_type, "orchard_col")
        
        new_df = pd.DataFrame(index=df.index)
        consumed_cols = set()
        
        for _, row in self.mappings.iterrows():
            u_name = row.get('unified_name')
            s_col = row.get(col_key)
            
            # col_key が存在しない場合のフォールバック (itunes_col 等、古いスキーマへの互換性)
            if s_col is None and source_type == "ITUNES":
                s_col = row.get('itunes_col')
            
            val = None
            if u_name and s_col and s_col in df.columns:
                val = df[s_col].copy()
                consumed_cols.add(s_col)
            elif u_name == "売上確定日" and '_extracted_end_date' in df.columns:
                val = df['_extracted_end_date'].copy()
            else:
                val = pd.Series([None] * len(df), index=df.index)
                
            if u_name and row.get('is_numeric'):
                if val.dtype == 'object':
                    val = val.astype(str).str.replace(',', '', regex=False)
                val = pd.to_numeric(val, errors='coerce').fillna(0)
            elif u_name and row.get('is_date'):
                val = self._normalize_date(val)
            elif u_name:
                val = val.fillna("").astype(str)
                val = val.replace("nan", "")
            
            if u_name:
                new_df[u_name] = val

        # --- カラム名の動的解決 ---
        # マッピング定義のソース列名から、ユーザーがUIで設定した統一名を逆引きする
        def find_unified_name(source_col_names):
            """元のソース列名リストから、現在の統一名を検索する"""
            for _, m in self.mappings.iterrows():
                for sc in source_col_names:
                    if any(str(m.get(k, "")) == sc for k in ["orchard_col", "nextone_col", "apple_fin_col", "apple_sales_col", "apple_other_col"] if m.get(k)):
                        return m.get('unified_name')
            return None

        # ソース列名に基づいて動的に統一名を取得
        amount_col = find_unified_name(["NET SHARE ACCOUNT CURRENCY", "Net Royalty Total", "Partner Share", "総支払額"]) or "収益"
        currency_col = find_unified_name(["ACCOUNT CURRENCY", "Currency", "Partner Share Currency"]) or "通貨"
        qty_col = find_unified_name(["QUANTITY", "Total  Royalty Bearing Plays", "Quantity", "数量"]) or "数量"
        country_col = find_unified_name(["SALE COUNTRY", "Storefront Name", "Country Of Sale", "国"]) or "国コード"
        date_col = find_unified_name(["STATEMENT PERIOD", "End Date", "分配月"]) or "売上確定日"
        
        # 為替レート・JPY列
        rate_col = find_unified_name(["CURRENCY CONVERSION RATE"]) or "為替レート"
        jpy_col = next((m.get('unified_name') for _, m in self.mappings.iterrows() if m.get('unified_name') and ('JPY' in m.get('unified_name').upper() or '円' in m.get('unified_name'))), "収益(JPY)")

        # Apple Music Royalty Completion (事前計算)
        if source_type == "ITUNES" and 'summary_df' in df.attrs:
            summary = df.attrs['summary_df']
            c_col = next((c for c in ["Country Of Sale", "Storefront Name"] if c in summary.columns), None)
            curr_col_s = next((c for c in ["Partner Share Currency", "Currency"] if c in summary.columns), None)
            r_col = next((c for c in ["Share Per Converted Unit", "Non Spatial Available Per Play Royalty"] if c in summary.columns), None)

            if c_col and curr_col_s and r_col:
                # { (Country, Currency): Rate } のマップ
                rate_map = {}
                for _, r in summary.iterrows():
                    if pd.notna(r[r_col]):
                        try:
                            rate_map[(str(r[c_col]).strip(), str(r[curr_col_s]).strip())] = float(r[r_col])
                        except:
                            continue
                
                # Apple固有のレート補完 (applyは行数が多いため速度に注意が必要だが、ここは計算のみなので比較的速い)
                def calculate_missing_royalty(r):
                    cur_v = float(r.get(amount_col, 0))
                    cur_r = float(r.get(rate_col, 0))
                    rate = rate_map.get((str(r.get(country_col, "")).strip(), str(r.get(currency_col, "")).strip()))
                    if rate is not None:
                        return pd.Series([cur_v if cur_v != 0 else float(r.get(qty_col, 0)) * rate, cur_r if cur_r != 0 else rate])
                    return pd.Series([cur_v, cur_r])

                if all(c in new_df.columns for c in [amount_col, country_col, currency_col]):
                    if rate_col not in new_df.columns: new_df[rate_col] = 0.0
                    new_df[[amount_col, rate_col]] = new_df.apply(calculate_missing_royalty, axis=1)

        # --- JPY Conversion 高速化 (一括事前取得) ---
        if jpy_col in new_df.columns and amount_col in new_df.columns and currency_col in new_df.columns:
            # 1. ユニークな (通貨, 日付) のペアを抽出
            dates_series = pd.to_datetime(new_df[date_col], errors='coerce')
            unique_pairs = new_df[[currency_col]].copy()
            unique_pairs['dt'] = dates_series.dt.date.fillna(datetime.date.today())
            unique_pairs = unique_pairs.drop_duplicates()
            
            # 2. レートを事前解決
            resolved_rates = {}
            for _, row in unique_pairs.iterrows():
                curr = str(row[currency_col]).upper().strip()
                dt = row['dt']
                if curr in ["", "NAN", "NONE", "JPY"]:
                    resolved_rates[(curr, dt)] = 1.0
                else:
                    resolved_rates[(curr, dt)] = self.exchange_service.get_rate(curr, dt)
            
            # 3. マップを使用して一括適用
            def get_final_rate(r):
                curr = str(r.get(currency_col, "JPY")).upper().strip()
                # 既存レートが10より大きければそれを優先 (Orchard等の既存レート)
                f_rate = float(r.get(rate_col, 0))
                if f_rate > 10.0:
                    return f_rate
                
                dt = pd.to_datetime(r.get(date_col), errors='coerce')
                dt_key = dt.date() if not pd.isna(dt) else datetime.date.today()
                return resolved_rates.get((curr, dt_key), 1.0)

            # apply(axis=1) は依然として低速だが、通信がないため大幅に改善する
            # 行数が多い場合は map を使うのがベスト
            new_df['_tmp_date_key'] = dates_series.dt.date.fillna(datetime.date.today())
            rate_series = new_df.apply(lambda x: resolved_rates.get((str(x[currency_col]).upper().strip(), x['_tmp_date_key']), 1.0), axis=1)
            
            # 手動レート(Orchard等)の考慮
            if rate_col in new_df.columns:
                manual_rates = pd.to_numeric(new_df[rate_col], errors='coerce').fillna(0)
                rate_series = [mr if mr > 10.0 else rs for mr, rs in zip(manual_rates, rate_series)]
            
            # 収益(JPY)の計算
            amounts = pd.to_numeric(new_df[amount_col], errors='coerce').fillna(0)
            new_df[jpy_col] = amounts * rate_series
            if rate_col in new_df.columns:
                new_df[rate_col] = rate_series
            
            if '_tmp_date_key' in new_df.columns:
                new_df.drop(columns=['_tmp_date_key'], inplace=True)

        # 備考欄生成の高速化 (apply(axis=1)を避け、辞書リストから一括生成)
        unmapped_cols = [c for c in df.columns if c not in consumed_cols and c != '_extracted_end_date']
        if '備考' not in new_df.columns: new_df['備考'] = ""
        if unmapped_cols:
            # 各行を辞書形式にし、不要なキーを除く
            df_unmapped = df[unmapped_cols].copy()
            remarks = []
            for _, row in df_unmapped.iterrows():
                d = {k: str(v) for k, v in row.items() if pd.notna(v) and str(v).strip() != ""}
                remarks.append(json.dumps(d, ensure_ascii=False) if d else "")
            new_df['備考'] = remarks
        
        return new_df, unmapped_cols

    def _normalize_date(self, s):
        if s is None or (isinstance(s, pd.Series) and s.isna().all()): return s
        return pd.to_datetime(s, errors='coerce').dt.strftime('%Y-%m-01').fillna(s)
