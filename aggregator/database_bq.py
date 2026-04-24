from google.cloud import bigquery
from google.cloud import storage
from google.cloud import exceptions
import pandas as pd
import datetime
import logging
import json
import os
import tempfile

class DatabaseManager:
    # デフォルトのマッピング設定（消失時の自動復旧用）
    DEFAULT_MAPPINGS = [
        {"unified_name": "売上確定日", "orchard_col": "STATEMENT PERIOD", "nextone_col": "分配月", "apple_fin_col": "End Date", "apple_sales_col": "End Date", "youtube_col": "Day", "is_date": True, "is_numeric": False},
        {"unified_name": "利用発生月", "orchard_col": "TRANSACTION DATE", "nextone_col": "利用月", "apple_fin_col": "End Date", "apple_sales_col": "End Date", "youtube_col": "Day", "is_date": True, "is_numeric": False},
        {"unified_name": "アーティスト名", "orchard_col": "PRODUCT ARTIST", "nextone_col": "アーティスト名", "apple_fin_col": "Artist", "apple_sales_col": "Artist/Show/Developer/Author", "youtube_col": "Artist", "is_date": False, "is_numeric": False},
        {"unified_name": "楽曲名", "orchard_col": "TRACK", "nextone_col": "楽曲名", "apple_fin_col": "Content Title", "apple_sales_col": "Title", "youtube_col": "Asset Title", "is_date": False, "is_numeric": False},
        {"unified_name": "アルバム名", "orchard_col": "PRODUCT", "nextone_col": "アルバム名", "apple_fin_col": "Product", "apple_sales_col": "", "youtube_col": "Album", "is_date": False, "is_numeric": False},
        {"unified_name": "ISRC", "orchard_col": "ISRC", "nextone_col": "ISRC", "apple_fin_col": "ISRC", "apple_sales_col": "ISRC/ISBN", "youtube_col": "ISRC", "is_date": False, "is_numeric": False},
        {"unified_name": "UPC_EAN", "orchard_col": "DISPLAY UPC", "nextone_col": "UPC", "apple_fin_col": "", "apple_sales_col": "UPC", "youtube_col": "UPC", "is_date": False, "is_numeric": False},
        {"unified_name": "ベンダー識別子", "orchard_col": "PRODUCT CODE", "nextone_col": "商品番号", "apple_fin_col": "Vendor Identifier", "apple_sales_col": "Vendor Identifier", "youtube_col": "Custom ID", "is_date": False, "is_numeric": False},
        {"unified_name": "アカウントID", "orchard_col": "ACCOUNT ID", "nextone_col": "", "apple_fin_col": "Apple Identifier", "apple_sales_col": "Apple Identifier", "youtube_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "YouTube動画ID", "orchard_col": "YOUTUBE VIDEO ID", "nextone_col": "", "apple_fin_col": "", "apple_sales_col": "", "youtube_col": "Asset ID", "is_date": False, "is_numeric": False},
        {"unified_name": "配信サービス名", "orchard_col": "STORE", "nextone_col": "DSP名", "apple_fin_col": "Report Type", "apple_sales_col": "", "youtube_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "国コード", "orchard_col": "SALE COUNTRY", "nextone_col": "国", "apple_fin_col": "Storefront Name", "apple_sales_col": "Country Of Sale", "youtube_col": "Country", "is_date": False, "is_numeric": False},
        {"unified_name": "レーベル名", "orchard_col": "LABEL IMPRINT", "nextone_col": "レーベル名", "apple_fin_col": "Label/Studio/Network", "apple_sales_col": "Label/Studio/Network/Developer/Publisher", "youtube_col": "Label", "is_date": False, "is_numeric": False},
        {"unified_name": "数量", "orchard_col": "QUANTITY", "nextone_col": "数量", "apple_fin_col": "Total  Royalty Bearing Plays", "apple_sales_col": "Quantity", "youtube_col": "Owned Views", "is_date": False, "is_numeric": True},
        {"unified_name": "収益", "orchard_col": "NET SHARE ACCOUNT CURRENCY", "nextone_col": "総支払額", "apple_fin_col": "Net Royalty Total", "apple_sales_col": "Partner Share", "youtube_col": "Partner Revenue", "is_date": False, "is_numeric": True},
        {"unified_name": "通貨", "orchard_col": "ACCOUNT CURRENCY", "nextone_col": "", "apple_fin_col": "Currency", "apple_sales_col": "Partner Share Currency", "youtube_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "収益(JPY)", "orchard_col": "", "nextone_col": "", "apple_fin_col": "", "apple_sales_col": "", "youtube_col": "", "is_date": False, "is_numeric": True},
        {"unified_name": "為替レート", "orchard_col": "CURRENCY CONVERSION RATE", "nextone_col": "", "apple_fin_col": "", "apple_sales_col": "", "youtube_col": "", "is_date": False, "is_numeric": True},
        {"unified_name": "ユニット単価", "orchard_col": "", "nextone_col": "", "apple_fin_col": "Share Per Converted Unit", "apple_sales_col": "", "youtube_col": "", "is_date": False, "is_numeric": True},
        {"unified_name": "販売種別", "orchard_col": "TRANSACTION TYPE", "nextone_col": "販売種別", "apple_fin_col": "Media Type", "apple_sales_col": "Sales or Return", "youtube_col": "Asset Type", "is_date": False, "is_numeric": False},
    ]

    DEFAULT_PLATFORMS = [
        {"key": "orchard_col", "name": "ORCHARD"},
        {"key": "nextone_col", "name": "NexTone"},
        {"key": "apple_fin_col", "name": "Apple(Financial)"},
        {"key": "apple_sales_col", "name": "Apple(Sales)"},
        {"key": "youtube_col", "name": "YouTube"},
    ]

    # 統合テーブルは動的スキーマ(またはpandasから自動生成)を許容する方針とします
    # 基本構成として常に存在が期待されるものを定義しておくことも可能ですが、
    # マッピング変更に柔軟に対応するため autodetect に任せます。

    # 日本語カラム名とBigQuery内部用の英数カラム名のマッピング
    COLUMN_NAME_MAP = {
        "売上確定日": "settlement_date",
        "利用発生月": "usage_month",
        "アーティスト名": "artist_name",
        "楽曲名": "track_name",
        "アルバム名": "album_name",
        "ISRC": "isrc",
        "UPC_EAN": "upc_ean",
        "ベンダー識別子": "vendor_id",
        "アカウントID": "account_id",
        "YouTube動画ID": "youtube_video_id",
        "配信サービス名": "service_name",
        "国コード": "country_code",
        "レーベル名": "label_name",
        "数量": "quantity",
        "収益": "revenue",
        "通貨": "currency",
        "収益(JPY)": "revenue_jpy",
        "為替レート": "exchange_rate",
        "ユニット単価": "unit_price",
        "販売種別": "sales_type",
        "SOURCE": "source",
        "FILE_NAME": "file_name",
        "uploaded_at": "uploaded_at",
        "備考": "remarks"
    }

    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = f"music-sales-raw-uploads-32010787277"
        self.client = bigquery.Client(project=project_id, location="asia-northeast1")
        self.storage_client = storage.Client(project=project_id)
        self._ensure_dataset_exists()

    def _to_internal_names(self, df):
        """日本語カラム名を内部用の英語カラム名に変換する"""
        return df.rename(columns=self.COLUMN_NAME_MAP)

    def _to_unified_names(self, df):
        """内部用の英語カラム名を日本語の統一名に戻す"""
        inv_map = {v: k for k, v in self.COLUMN_NAME_MAP.items()}
        return df.rename(columns=inv_map)

    def _ensure_dataset_exists(self):
        """データセットが存在しなければ作成する (東京)"""
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except exceptions.NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-northeast1"
            self.client.create_dataset(dataset)

    def reset_dataset(self):
        """売上データテーブルのみを削除して初期化する（マッピング設定は維持）"""
        target_tables = ["unified_sales_data"]
        for t_name in target_tables:
            table_id = f"{self.project_id}.{self.dataset_id}.{t_name}"
            self.client.delete_table(table_id, not_found_ok=True)
            logging.info(f"Table {table_id} deleted (reset).")

    def save_unified_data(self, df, filename, overwrite=True, progress_callback=None):
        """フォーマット整形済みのDataFrameをそのまま統合テーブルに保存する"""
        table_id = f"{self.project_id}.{self.dataset_id}.unified_sales_data"
        logging.info(f"Targeting BigQuery Table: {table_id}")
        
        # テーブルが存在しない場合に備えて初期スキーマで作成を試みる
        initial_schema = [
            bigquery.SchemaField("FILE_NAME", "STRING"),
            bigquery.SchemaField("SOURCE", "STRING"),
            bigquery.SchemaField("uploaded_at", "TIMESTAMP"),
        ]
        self._ensure_table_exists(table_id, initial_schema)

        if overwrite:
            try:
                logging.info(f"Deleting previous data for {filename} from {table_id}")
                query = f"DELETE FROM `{table_id}` WHERE file_name = @f"
                self.client.query(query, job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("f", "STRING", filename)]
                )).result()
            except exceptions.NotFound:
                pass # テーブル未作成の場合はスキップ

        total_rows = len(df)
        # BigQueryで扱えるようにメタデータをセット
        df['FILE_NAME'] = filename
        if 'SOURCE' not in df.columns:
            df['SOURCE'] = "UNKNOWN"
            
        # 数値列の型不一致(FLOAT vs INTEGER)を防ぐため、整数列はすべてfloatに変換
        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = df[col].astype(float)
            
        now = datetime.datetime.now().isoformat()
        df['uploaded_at'] = now
        
        # BigQueryの列名ルール（英数字とアンダースコア）に厳格に合わせる場合もありますが、
        # 近年のBQは柔軟なためそのまま送信を試みます。

        if progress_callback:
            progress_callback(f"📝 データを変換中... ({total_rows:,} 件)")

        # BigQuery保存用にカラム名を英語に変換
        df_for_bq = self._to_internal_names(df)
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json', encoding='utf-8') as tmp:
            # ndjson出力時にpandasのto_jsonを使用
            # 日付などはそのまま文字列として扱う
            df_for_bq.to_json(tmp.name, orient='records', lines=True, force_ascii=False)
            tmp_path = tmp.name

        if progress_callback:
            progress_callback(f"🚀 クラウド(BigQuery)へロード中...")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )
        try:
            with open(tmp_path, 'rb') as source_file:
                # location をデータセットに合わせて asia-northeast1 に固定
                job = self.client.load_table_from_file(source_file, table_id, job_config=job_config, location="asia-northeast1")
                job.result() # 完了まで待機
                
            if progress_callback:
                progress_callback(f"✅ ロード完了")
                
            logging.info(f"Successfully saved {len(df)} rows for UNIFIED data: {filename} to {table_id}")
            return len(df)
        except Exception as e:
            logging.error(f"Failed to save UNIFIED data: {e}")
            raise
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def delete_unified_data(self, filename):
        """特定のファイルに関連する統合データをすべて削除する"""
        table_id = f"{self.project_id}.{self.dataset_id}.unified_sales_data"
        try:
            query = f"DELETE FROM `{table_id}` WHERE file_name = @f"
            self.client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("f", "STRING", filename)]
            )).result()
            logging.info(f"Successfully deleted unified data for file: {filename}")
            return True
        except Exception as e:
            logging.error(f"Failed to delete unified data for {filename}: {e}")
            return False

    def upload_to_gcs_direct(self, file_obj, filename):
        """ファイルを直接GCSにアップロードする（署名付きURLを使わない）"""
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(filename)
            # BytesIOなどのファイルライクオブジェクトをそのままアップロード
            blob.upload_from_file(file_obj)
            logging.info(f"Directly uploaded to GCS: {filename}")
            return True
        except Exception as e:
            logging.error(f"Failed direct upload to GCS: {e}")
            return False

    def check_file_exists(self, filename):
        if not filename: return False
        fn = filename.strip()
        table_id = f"{self.project_id}.{self.dataset_id}.unified_sales_data"
        query = f"SELECT count(*) as cnt FROM `{table_id}` WHERE file_name = @f"
        try:
            results = self.client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("f", "STRING", fn)]
            )).result()
            for row in results: return row.cnt > 0
        except Exception:
            pass
        return False

    def get_file_history(self):
        """アップロード済みのファイル一覧を全件取得する（軽量クエリ）"""
        table_id = f"{self.project_id}.{self.dataset_id}.unified_sales_data"
        try:
            query = f"SELECT file_name as filename, source as source_type, COUNT(*) as row_count, max(uploaded_at) as uploaded_at FROM `{table_id}` GROUP BY file_name, source ORDER BY uploaded_at DESC"
            df = self.client.query(query).to_dataframe()
            # 英語カラム名を日本語の統一名に戻す
            df = self._to_unified_names(df)
            return df
        except exceptions.NotFound:
            return pd.DataFrame()

    def get_unified_data(self, limit=2000):
        """保存されている統合データを取得する"""
        table_id = f"{self.project_id}.{self.dataset_id}.unified_sales_data"
        try:
            query = f"SELECT * FROM `{table_id}` ORDER BY uploaded_at DESC, FILE_NAME"
            if limit:
                query += f" LIMIT {limit}"
            df = self.client.query(query).to_dataframe()
            # Restore SOURCE since it replaced source_type
            return df
        except exceptions.NotFound:
            return pd.DataFrame()

    def _ensure_table_exists(self, table_id, schema):
        """テーブルが存在しなければ作成する"""
        try:
            self.client.get_table(table_id)
        except exceptions.NotFound:
            table = bigquery.Table(table_id, schema=schema)
            # リージョンを明示して作成
            self.client.create_table(table, exists_ok=True)
            logging.info(f"Created table: {table_id}")

    def save_parsing_rule(self, file_pattern, header_row):
        table_id = f"{self.project_id}.{self.dataset_id}.parsing_rules"
        # スキーマを明示して作成を確実にする
        schema = [
            bigquery.SchemaField("file_pattern", "STRING"),
            bigquery.SchemaField("header_row", "INTEGER"),
        ]
        self._ensure_table_exists(table_id, schema)
        
        df = pd.DataFrame([{'file_pattern': file_pattern, 'header_row': header_row}])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        # リージョンを明示
        self.client.load_table_from_dataframe(df, table_id, job_config=job_config, location="asia-northeast1").result()
        logging.info(f"Saved parsing rule: {file_pattern}")

    def get_parsing_rules(self):
        table_id = f"{self.project_id}.{self.dataset_id}.parsing_rules"
        try:
            return self.client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
        except exceptions.NotFound:
            return pd.DataFrame()

    def delete_parsing_rule(self, pattern):
        table_id = f"{self.project_id}.{self.dataset_id}.parsing_rules"
        query = f"DELETE FROM `{table_id}` WHERE file_pattern = @p"
        try:
            self.client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("p", "STRING", pattern)]
            )).result()
        except exceptions.NotFound:
            pass

    def save_unified_columns_batch(self, df):
        """マッピング辞書全体を一括で保存する（画面から編集されたものを上書き）"""
        table_id = f"{self.project_id}.{self.dataset_id}.unified_columns"
        
        if df.empty:
            logging.info("Empty dataframe passed to save_unified_columns_batch. Skipping.")
            return

        # DataFrameのカラムから動的にスキーマを生成
        schema = []
        for col in df.columns:
            if col == "is_date" or col == "is_numeric":
                schema.append(bigquery.SchemaField(col, "BOOLEAN"))
            else:
                schema.append(bigquery.SchemaField(col, "STRING"))
        
        self._ensure_table_exists(table_id, schema)
        
        # WRITE_TRUNCATE でアトミックに上書き
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema
        )
        self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config, location="asia-northeast1"
        ).result()
        logging.info(f"Saved mapping columns in batch: {len(df)} rows with {len(df.columns)} columns")

    def get_unified_columns(self):
        """マッピング設定を取得する（空の場合はデフォルトを読み込む）"""
        table_id = f"{self.project_id}.{self.dataset_id}.unified_columns"
        try:
            df = self.client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
            if not df.empty:
                df = df.drop_duplicates(subset=['unified_name'], keep='last')

            if df.empty:
                logging.info("Mapping table is empty. Restoring defaults...")
                self.save_unified_columns_batch(pd.DataFrame(self.DEFAULT_MAPPINGS))
                return pd.DataFrame(self.DEFAULT_MAPPINGS)
            return df
        except exceptions.NotFound:
            logging.info("Mapping table not found. Creating with defaults...")
            self.save_unified_columns_batch(pd.DataFrame(self.DEFAULT_MAPPINGS))
            return pd.DataFrame(self.DEFAULT_MAPPINGS)

    def _get_or_init_table(self, table_name, schema, default_initializer):
        """テーブルからデータを取得し、空またはNotFoundの場合はデフォルトで初期化する汎用ヘルパー"""
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        try:
            df = self.client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
            if not df.empty:
                return df
        except exceptions.NotFound:
            pass
        
        # 空またはNotFound: デフォルトで初期化
        default_df = default_initializer()
        self._ensure_table_exists(table_id, schema)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
        self.client.load_table_from_dataframe(default_df, table_id, job_config=job_config, location="asia-northeast1").result()
        return default_df

    def get_master_columns(self):
        """統合項目（縦軸）マスターを取得する"""
        schema = [
            bigquery.SchemaField("unified_name", "STRING"),
            bigquery.SchemaField("is_date", "BOOLEAN"),
            bigquery.SchemaField("is_numeric", "BOOLEAN"),
            bigquery.SchemaField("sort_order", "INTEGER"),
        ]
        def init_master():
            mappings = self.get_unified_columns()
            df = mappings[['unified_name', 'is_date', 'is_numeric']].drop_duplicates()
            df['sort_order'] = range(len(df))
            return df
        
        df = self._get_or_init_table("master_columns", schema, init_master)
        if 'sort_order' not in df.columns:
            df['sort_order'] = range(len(df))
        
        df = df.sort_values('sort_order').reset_index(drop=True)
        return df

    def save_master_columns(self, df):
        """統合項目マスターを保存する"""
        table_id = f"{self.project_id}.{self.dataset_id}.master_columns"
        schema = [
            bigquery.SchemaField("unified_name", "STRING"),
            bigquery.SchemaField("is_date", "BOOLEAN"),
            bigquery.SchemaField("is_numeric", "BOOLEAN"),
            bigquery.SchemaField("sort_order", "INTEGER"),
        ]
        if 'sort_order' not in df.columns:
            df = df.copy()
            df['sort_order'] = range(len(df))
            
        self._ensure_table_exists(table_id, schema)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
        self.client.load_table_from_dataframe(df, table_id, job_config=job_config, location="asia-northeast1").result()

    def get_platforms(self):
        """プラットフォーム（横軸）マスターを取得する"""
        schema = [
            bigquery.SchemaField("key", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("sort_order", "INTEGER"),
        ]
        def init_platforms():
            mappings = self.get_unified_columns()
            excluded = ['unified_name', 'is_date', 'is_numeric', 'uploaded_at']
            existing_keys = [c for c in mappings.columns if c not in excluded]
            if existing_keys:
                default_map = {p['key']: p['name'] for p in self.DEFAULT_PLATFORMS}
                df = pd.DataFrame([
                    {"key": k, "name": default_map.get(k, k.replace('_col', '').capitalize())}
                    for k in existing_keys
                ])
                df['sort_order'] = range(len(df))
                return df
            
            df = pd.DataFrame(self.DEFAULT_PLATFORMS)
            df['sort_order'] = range(len(df))
            return df
        
        df = self._get_or_init_table("platforms", schema, init_platforms)
        if 'sort_order' not in df.columns:
            df['sort_order'] = range(len(df))

        df = df.sort_values('sort_order').reset_index(drop=True)
        return df

    def save_platforms(self, df):
        """プラットフォームマスターを保存する"""
        table_id = f"{self.project_id}.{self.dataset_id}.platforms"
        schema = [
            bigquery.SchemaField("key", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("sort_order", "INTEGER"),
        ]
        if 'sort_order' not in df.columns:
            df = df.copy()
            df['sort_order'] = range(len(df))
            
        self._ensure_table_exists(table_id, schema)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
        self.client.load_table_from_dataframe(df, table_id, job_config=job_config, location="asia-northeast1").result()

    def get_discovered_headers(self, platform_key=None, **kwargs):
        """発見されたヘッダー名リストを取得する"""
        table_id = f"{self.project_id}.{self.dataset_id}.discovered_headers"
        try:
            if platform_key:
                query = f"SELECT * FROM `{table_id}` WHERE platform_key = @pk"
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("pk", "STRING", platform_key)]
                )
                df = self.client.query(query, job_config=job_config).to_dataframe()
            else:
                df = self.client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
            
            # カラム（detected_at/source_file）が存在しない古いテーブルの場合は削除してマイグレーション
            if not df.empty and ("detected_at" not in df.columns or "source_file" not in df.columns):
                logging.info(f"Old schema detected in {table_id}. Deleting for migration...")
                self.client.delete_table(table_id, not_found_ok=True)
                return pd.DataFrame(columns=["platform_key", "header_name", "source_file", "detected_at"])
            
            if not df.empty and "detected_at" in df.columns:
                df = df.sort_values("detected_at", ascending=False)
                
            return df
        except Exception as e:
            # カラム（detected_at等）が存在しないためにエラーが出た場合、テーブルを削除して初期化を促す
            if "detected_at" in str(e) or "source_file" in str(e):
                logging.info(f"Schema mismatch in {table_id}. Deleting table for migration...")
                self.client.delete_table(table_id, not_found_ok=True)
            return pd.DataFrame(columns=["platform_key", "header_name", "source_file", "detected_at"])

    def save_discovered_headers_batch(self, df, overwrite=False):
        """ヘッダーリストを一括保存する"""
        table_id = f"{self.project_id}.{self.dataset_id}.discovered_headers"
        schema = [
            bigquery.SchemaField("platform_key", "STRING"),
            bigquery.SchemaField("header_name", "STRING"),
            bigquery.SchemaField("source_file", "STRING"),
            bigquery.SchemaField("detected_at", "TIMESTAMP"),
        ]
        self._ensure_table_exists(table_id, schema)
        
        if df.empty and not overwrite: return
        
        # 必要な列が欠落している場合の補完
        if "source_file" not in df.columns: df["source_file"] = "(手動/不明)"
        if "detected_at" not in df.columns: 
            df["detected_at"] = datetime.datetime.now(datetime.timezone.utc)
        
        if overwrite:
            combined = df.drop_duplicates()
        else:
            # 既存データとマージして重複排除
            existing = self.get_discovered_headers()
            # 同一ファイル名かつ同一ヘッダーの重複を避ける or 単に最新を維持
            combined = pd.concat([existing, df]).drop_duplicates(subset=["platform_key", "header_name", "source_file"])
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
        self.client.load_table_from_dataframe(combined, table_id, job_config=job_config, location="asia-northeast1").result()

    def delete_unified_column(self, name):
        table_id = f"{self.project_id}.{self.dataset_id}.unified_columns"
        query = f"DELETE FROM `{table_id}` WHERE unified_name = @n"
        try:
            self.client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("n", "STRING", name)]
            )).result()
        except exceptions.NotFound:
            pass

    def save_exchange_rates(self, df):
        """為替レート定義を一括で保存する（WRITE_TRUNCATEでアトミックに上書き）"""
        table_id = f"{self.project_id}.{self.dataset_id}.exchange_rates"
        schema = [
            bigquery.SchemaField("currency_code", "STRING"),
            bigquery.SchemaField("rate_to_jpy", "FLOAT"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]
        self._ensure_table_exists(table_id, schema)
        
        df = df.copy()
        df['updated_at'] = datetime.datetime.now()
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
        self.client.load_table_from_dataframe(df, table_id, job_config=job_config, location="asia-northeast1").result()
        logging.info(f"Saved exchange rates: {len(df)} rows")

    def get_exchange_rates(self):
        """為替レート一覧を取得する（空の場合はデフォルトの空DFを返す）"""
        table_id = f"{self.project_id}.{self.dataset_id}.exchange_rates"
        try:
            return self.client.query(f"SELECT currency_code, rate_to_jpy FROM `{table_id}`").to_dataframe()
        except exceptions.NotFound:
            return pd.DataFrame(columns=["currency_code", "rate_to_jpy"])

    # upload_large_file_via_gcs は RAW_SCHEMA 未定義で動作不能だったため削除済み。
    # 大容量ファイルのアップロードが必要な場合は save_unified_data() を使用してください。

    # --- GCS Methods ---
    def list_gcs_files(self):
        """バケット内のファイル一覧を取得する"""
        try:
            blobs = self.storage_client.list_blobs(self.bucket_name)
            return [{"name": b.name, "size": b.size, "updated": b.updated} for b in blobs]
        except Exception as e:
            logging.error(f"Failed to list GCS files: {e}")
            return []

    def delete_gcs_file(self, filename):
        """GCS上のファイルを削除する"""
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            bucket.blob(filename).delete()
            return True
        except Exception as e:
            logging.error(f"Failed to delete GCS file {filename}: {e}")
            return False

    def get_gcs_blob_io(self, filename):
        """GCS上のファイルをストリームとして読み込むためのIOオブジェクトを返す"""
        import io
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(filename)
        if not blob.exists():
            return None
        byte_stream = io.BytesIO()
        blob.download_to_file(byte_stream)
        byte_stream.seek(0)
        # file-like object として振る舞うために name 属性が必要な場合がある
        byte_stream.name = filename
        return byte_stream

    def rename_gcs_file(self, old_name, new_name):
        """GCS上のファイルをリネームする（コピー＋削除）"""
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(old_name)
            bucket.copy_blob(blob, bucket, new_name)
            blob.delete()
            return True
        except Exception as e:
            logging.error(f"Failed to rename GCS file {old_name} -> {new_name}: {e}")
            return False
