import pandas as pd
import logging
import io
import datetime

class SalesAggregator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _get_preview(self, file):
        """ファイルの先頭部分を安全に文字列として取得する"""
        file.seek(0)
        content = file.read(10000)
        file.seek(0)
        
        encodings = ['utf-8-sig', 'utf-8', 'utf-16', 'shift-jis', 'cp932']
        for enc in encodings:
            try:
                decoded = content.decode(enc)
                if len(decoded) > 0:
                    return decoded, enc
            except:
                continue
        return content.decode('utf-8', errors='replace'), 'utf-8'

    def process_files(self, uploaded_files):
        """複数のファイルを読み込み、自動判別して統合する"""
        all_dfs = []
        
        for file in uploaded_files:
            try:
                df = self._parse_file(file)
                if df is not None:
                    # カラム名のクリーンアップ (引用符、空白、BOMを除去)
                    df.columns = [str(c).strip().replace('"', '').replace('\ufeff', '').strip().upper() for c in df.columns]
                    
                    df['ORIGIN_FILE'] = file.name
                    df['UPLOADED_AT'] = datetime.datetime.now()
                    
                    # 統一用の固定カラムを追加 (日本語を含む場合も大文字化ルールに従う)
                    # _SALES_AMOUNT_ や _QUANTITY_ などの接頭辞をつけて完全に独立させる
                    
                    for col in ['_NET_REVENUE_', '_QUANTITY_']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    
                    # _DATE_ の正規化 (ベクトル化処理で高速化)
                    if '_DATE_' in df.columns:
                        df['_DATE_'] = self._vectorized_normalize_date(df['_DATE_'])
                        
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(f"ファイル {file.name} の処理中にエラー: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                raise Exception(f"ファイル '{file.name}' の解析に失敗しました。\nエラー内容: {e}")

        if not all_dfs:
            return None

        merged_df = pd.concat(all_dfs, ignore_index=True, sort=False)
        
        # 表示優先順位のカラム (すべて大文字にする)
        priority_cols = ['SOURCE', '_DATE_', '_ARTIST_', '_TRACK_', '_ALBUM_', '_ISRC_', '_QUANTITY_', '_NET_REVENUE_']
        existing_priority = [c for c in priority_cols if c in merged_df.columns]
        other_cols = [c for c in merged_df.columns if c not in existing_priority]
        
        return merged_df[existing_priority + other_cols]

    def _parse_file(self, file):
        """判別キーワードは大文字小文字を問わずチェック、読み込み後はカラムを一旦そのままにし、後に大文字化される"""
        preview, encoding = self._get_preview(file)
        # 判定用に大文字化しておく
        p_up = preview.upper()
        
        # 1. The Orchard
        if "PRODUCT ARTIST" in p_up or "TRACK ARTIST" in p_up or "TRANSACTION DATE" in p_up:
            file.seek(0)
            # Orchard はタブ区切りの .txt であることが多いため、まずはタブで試す
            df = pd.read_csv(file, sep='\t', encoding=encoding)
            # もし1列しか読み込めなかった場合はカンマ区切りを試す
            if df.shape[1] <= 1:
                file.seek(0)
                df = pd.read_csv(file, sep=',', encoding=encoding)
            return self._format_orchard(df)

        # 2. NexTone / Diversity Site
        if "利用月" in preview or "送信日" in preview or "楽曲名" in preview:
            file.seek(0)
            lines = preview.splitlines()
            skip = 0
            for i, line in enumerate(lines):
                if "利用月" in line and "楽曲名" in line:
                    skip = i
                    break
            df = pd.read_csv(file, sep='\t', skiprows=skip, encoding=encoding)
            return self._format_nextone(df)

        # 3. iTunes / Apple Music (Vendor Identifier 等)
        if "VENDOR IDENTIFIER" in p_up or "START DATE" in p_up or "APPLE MUSIC" in p_up or "APPLE IDENTIFIER" in p_up:
            file.seek(0)
            lines = preview.splitlines()
            skip = 0
            for i, line in enumerate(lines):
                l_up = line.upper()
                if "VENDOR IDENTIFIER" in l_up or "STOREFRONT NAME" in l_up or "APPLE IDENTIFIER" in l_up:
                    skip = i
                    break
            
            df = pd.read_csv(file, sep='\t', skiprows=skip, encoding=encoding)
            if not df.empty:
                col0 = df.columns[0]
                mask = df[col0].astype(str).str.contains('Total_Rows|Row Count|Row_Count|Row_count', case=False)
                if mask.any():
                    idx = df[mask].index[0]
                    df = df.iloc[:idx]
            return self._format_itunes(df)
        
        return None

    def _format_itunes(self, df):
        # キー（元の列名）も安全のため大文字でマッチングさせるように remap を修正する
        mapping = {
            'START DATE': '_DATE_',
            'ARTIST/SHOW/DEVELOPER/AUTHOR': '_ARTIST_',
            'ARTIST': '_ARTIST_',
            'TITLE': '_TRACK_',
            'CONTENT TITLE': '_TRACK_',
            'ISRC/ISBN': '_ISRC_',
            'ISRC': '_ISRC_',
            'QUANTITY': '_QUANTITY_',
            'TOTAL  ROYALTY BEARING PLAYS': '_QUANTITY_',
            'PARTNER SHARE': '_NET_REVENUE_',
            'NET ROYALTY TOTAL': '_NET_REVENUE_'
        }
        res = self._remap_and_clean(df, mapping)
        res.insert(0, 'SOURCE', 'iTunes')
        return res

    def _format_nextone(self, df):
        mapping = {
            '利用月': '_DATE_',
            'アーティスト名': '_ARTIST_',
            '楽曲名': '_TRACK_',
            'アルバム名': '_ALBUM_',
            'ISRC': '_ISRC_',
            '数量': '_QUANTITY_',
            '総支払額': '_NET_REVENUE_'
        }
        res = self._remap_and_clean(df, mapping)
        res.insert(0, 'SOURCE', 'NexTone')
        return res

    def _format_orchard(self, df):
        mapping = {
            'TRANSACTION DATE': '_DATE_',
            'PRODUCT ARTIST': '_ARTIST_',
            'TRACK': '_TRACK_',
            'PRODUCT': '_ALBUM_',
            'ISRC': '_ISRC_',
            'QUANTITY': '_QUANTITY_',
            'NET SHARE ACCOUNT CURRENCY': '_NET_REVENUE_'
        }
        res = self._remap_and_clean(df, mapping)
        res.insert(0, 'SOURCE', 'The Orchard')
        return res

    def _vectorized_normalize_date(self, series):
        """列全体を YYYY-MM-01 形式に一括変換する (高速)"""
        # 文字列に変換
        s = series.astype(str).str.strip()
        
        # 1. YYYYMM 形式の判定と変換 (例: 202512 -> 2025-12-01)
        # 6桁の数字のみのものを正規表現で置換
        mask_yyyymm = s.str.match(r'^\d{6}$')
        if mask_yyyymm.any():
            s.loc[mask_yyyymm] = s.loc[mask_yyyymm].str[:4] + "-" + s.loc[mask_yyyymm].str[4:6] + "-01"
            
        # 2. pd.to_datetime を一括適用 (errors='coerce' で不正な形式は NaT に)
        dt_series = pd.to_datetime(s, errors='coerce')
        
        # 3. YYYY-MM-01 形式の文字列に戻す
        # すべて 1日 に固定する
        result = dt_series.dt.strftime('%Y-%m-01')
        
        # 4. 変換できなかったものは元の値を維持する（必要に応じて）
        # result が NaN の箇所を元の s で埋める
        final_result = result.fillna(s)
        
        return final_result

    def _remap_and_clean(self, df, mapping):
        # 処理前にカラム名を一旦大文字に統一してマッチングを確実にする
        res = df.copy()
        res.columns = [c.upper() for c in res.columns]
        
        # マッピング（キーは大文字で定義しておく）
        for original_col, unified_name in mapping.items():
            u_orig = original_col.upper()
            if u_orig in res.columns:
                if unified_name not in res.columns:
                    res[unified_name] = res[u_orig]
                else:
                    res[unified_name] = res[unified_name].fillna(res[u_orig])
        return res
