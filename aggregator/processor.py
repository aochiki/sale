import pandas as pd
import logging
import io
import datetime
import fnmatch
import json

class SalesAggregator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def detect_source(self, filename):
        """ファイル名から会社タイプを判定する"""
        fn = filename.lower()
        if fnmatch.fnmatch(fn, "orchard*"):
            return "ORCHARD"
        elif fnmatch.fnmatch(fn, "divsiteall*"):
            return "NEXTONE"
        elif "_zz" in fn:
            return "ITUNES"
        return "UNKNOWN"

    def parse_raw_only(self, file, rules=None):
        """ファイルを解析し、最適な区切り文字とヘッダー行を自動検知してデータフレームを返す"""
        content_sample = ""
        try:
            file.seek(0)
            sample_bytes = file.read(4000)
            for enc in ['utf-8-sig', 'utf-8', 'cp932', 'shift-jis']:
                try:
                    content_sample = sample_bytes.decode(enc)
                    break
                except Exception:
                    continue
        except Exception:
            pass
        
        # 区切り文字の推測（サンプルから最も多い区切り文字を優先）
        if content_sample:
            tabs = content_sample.count('\t')
            commas = content_sample.count(',')
            semis = content_sample.count(';')
            if tabs > commas and tabs > semis:
                target_separators = ['\t', ',', ';']
            elif semis > commas:
                target_separators = [';', ',', '\t']
            else:
                target_separators = [',', '\t', ';']
        else:
            target_separators = ['\t', ',', ';']

        # 試行する行数 (ルール優先、なければ 0-10行をスキャン)
        skiprows_to_try = []
        if rules is not None and not rules.empty:
            for _, rule in rules.iterrows():
                if fnmatch.fnmatch(file.name.lower(), rule['file_pattern'].lower()):
                    skiprows_to_try.append(int(rule['header_row']))
                    break
        
        # フォールバック: 0-10行をすべて試す
        for r in range(11):
            if r not in skiprows_to_try:
                skiprows_to_try.append(r)

        target_encodings = ['utf-8-sig', 'utf-8', 'cp932', 'shift-jis', 'utf-16']
        
        best_df = None
        max_cols = 0

        for sr in skiprows_to_try:
            for enc in target_encodings:
                for separator in target_separators:
                    file.seek(0)
                    try:
                        df = pd.read_csv(file, sep=separator, skiprows=sr, encoding=enc, on_bad_lines='skip', low_memory=False)
                        if df.empty: continue
                        
                        if df.shape[1] >= 2:
                            if df.shape[1] > max_cols:
                                max_cols = df.shape[1]
                                best_df = df
                            # 列数が5以上なら十分な精度と判断して即リターン
                            if df.shape[1] >= 5:
                                return df
                    except Exception:
                        continue
            # このskiprowsで既に十分な結果が得られていたら、残りの行は試さない
            if best_df is not None and max_cols >= 5:
                return best_df
        
        return best_df

    def unify_raw_records(self, raw_df, mappings):
        """保存されている RAW データ (各行のJSON) を一括で統合する (動的処理)"""
        if raw_df.empty: return pd.DataFrame()
        
        all_unified_dfs = []
        # get_raw_data で既に filename, row_index 順にソートされている前提
        for filename in raw_df['filename'].unique():
            file_rows = raw_df[raw_df['filename'] == filename]
            source_type = file_rows['source_type'].iloc[0]
            # もし AutoDetect や UNKNOWN の場合は、ファイル名から動的に再判定を試みる
            if source_type in ["AutoDetect", "UNKNOWN"]:
                detected = self.detect_source(filename)
                if detected != "UNKNOWN":
                    source_type = detected
            
            try:
                # 各行のJSONからデータフレームを復元
                rows_list = [json.loads(r) for r in file_rows['raw_row_json']]
                original_df = pd.DataFrame(rows_list)
                
                # 指定されたマッピングで列を抽出・変換
                unified_df = self._apply_mapping(original_df, source_type, mappings)
                if unified_df is not None:
                    unified_df['SOURCE'] = source_type
                    unified_df['ORIGIN_FILE'] = filename
                    all_unified_dfs.append(unified_df)
            except Exception as e:
                self.logger.error(f"統合エラー ({filename}): {e}")
        
        if not all_unified_dfs: return pd.DataFrame()
        return pd.concat(all_unified_dfs, ignore_index=True)

    def _apply_mapping(self, df, source_type, mappings):
        """RAWデータにマッピングを適用する"""
        if mappings is None or mappings.empty:
            return df

        new_df = pd.DataFrame(index=df.index)
        
        # 統合後のカラム名を一貫させるため、元データのカラム名とマッピング定義を正規化
        df.columns = [str(c).strip() for c in df.columns]
        df_col_map = {c: c for c in df.columns} 

        for _, row in mappings.iterrows():
            unified_name = str(row['unified_name']).strip()
            
            # ソース列名の特定
            source_col = ""
            if source_type.upper() == "ORCHARD":
                source_col = str(row.get('orchard_col', '')).strip()
            elif source_type.upper() == "NEXTONE":
                source_col = str(row.get('nextone_col', '')).strip()
            elif source_type.upper() == "ITUNES":
                # Appleの場合は、現在の3つのサブカラムから値が入っているものを探す
                for key in ["apple_fin_col", "apple_sales_col", "apple_other_col"]:
                    val = str(row.get(key, '')).strip()
                    if val and val in df_col_map:
                        source_col = val
                        break
            
            if source_col and source_col in df_col_map:
                orig_col = df_col_map[source_col]
                val = df[orig_col]
                
                if row.get('is_date'):
                    val = self._normalize_date(val)
                elif row.get('is_numeric'):
                    val = pd.to_numeric(val, errors='coerce').fillna(0)
                
                new_df[unified_name] = val
            else:
                new_df[unified_name] = None

        return new_df

    def _normalize_date(self, series):
        """日付形式を YYYY-MM-01 に統一する"""
        s = series.astype(str).str.strip()
        # YYYYMM (6桁) の対応
        mask_yyyymm = s.str.match(r'^\d{6}$')
        if mask_yyyymm.any():
            s.loc[mask_yyyymm] = s.loc[mask_yyyymm].str[:4] + "-" + s.loc[mask_yyyymm].str[4:6] + "-01"
        
        # Pandasの機能でパース
        dt_series = pd.to_datetime(s, errors='coerce')
        # すべて月初の 01 日に固定
        return dt_series.dt.strftime('%Y-%m-01').fillna(s)
