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
        if fnmatch.fnmatch(filename, "Orchard*"):
            return "ORCHARD"
        elif fnmatch.fnmatch(filename, "DivSiteAll*"):
            return "NEXTONE"
        elif "_ZZ" in filename:
            return "ITUNES"
        return "UNKNOWN"

    def parse_raw_only(self, file, rules=None):
        """ファイルを解析し、最適な区切り文字とヘッダー行を自動検知してデータフレームを返す"""
        content_sample = ""
        try:
            file.seek(0)
            # 最初の数KBを読み込んで区切り文字を推測
            sample_bytes = file.read(4000)
            for enc in ['utf-8-sig', 'utf-8', 'cp932', 'shift-jis']:
                try:
                    content_sample = sample_bytes.decode(enc)
                    break
                except:
                    continue
        except:
            pass
        
        # 区切り文字の推測
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
                        # header=0 (skiprows適用後) で読み込み
                        df = pd.read_csv(file, sep=separator, skiprows=sr, encoding=enc, on_bad_lines='skip', low_memory=False)
                        if df.empty: continue
                        
                        # 有効なカラムが2つ以上あれば候補とする
                        if df.shape[1] >= 2:
                            # 最も列数が多いものを「正解」の可能性が高いと判断して保持
                            if df.shape[1] > max_cols:
                                max_cols = df.shape[1]
                                best_df = df
                                # ルールに基づいた行で成功した場合は即座に返す（レスポンス優先）
                                if sr == skiprows_to_try[0] and rules is not None and not rules.empty:
                                    return df
                    except:
                        continue
        
        return best_df

    def unify_raw_records(self, raw_df, mappings):
        """保存されている RAW データ (各行のJSON) を一括で統合する (動的処理)"""
        if raw_df.empty: return pd.DataFrame()
        
        all_unified_dfs = []
        # get_raw_data で既に filename, row_index 順にソートされている前提
        for filename in raw_df['filename'].unique():
            file_rows = raw_df[raw_df['filename'] == filename]
            source_type = file_rows['source_type'].iloc[0]
            
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
        src_col_key = {
            "ORCHARD": "orchard_col",
            "NEXTONE": "nextone_col",
            "ITUNES": "itunes_col"
        }.get(source_type.upper(), "orchard_col")

        # RAWデータのカラム名は大文字/小文字そのままの可能性があるため、マッピング側と比較しやすくする
        df_col_map = {str(c).strip(): c for c in df.columns} 

        for _, row in mappings.iterrows():
            unified_name = row['unified_name']
            source_col = str(row[src_col_key]).strip()

            if source_col in df_col_map:
                orig_col = df_col_map[source_col]
                val = df[orig_col]
                
                if row['is_date']:
                    val = self._normalize_date(val)
                elif row['is_numeric']:
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
