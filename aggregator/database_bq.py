from google.cloud import bigquery
from google.cloud import storage
from google.cloud import exceptions
from google.api_core import retry
import pandas as pd
import datetime
import logging
import json
import time
import os
import tempfile

class DatabaseManager:
    # デフォルトのマッピング設定（消失時の自動復旧用）
    DEFAULT_MAPPINGS = [
        {"unified_name": "売上確定日", "orchard_col": "STATEMENT PERIOD", "nextone_col": "分配月", "itunes_col": "End Date", "is_date": True, "is_numeric": False},
        {"unified_name": "利用発生月", "orchard_col": "TRANSACTION DATE", "nextone_col": "利用月", "itunes_col": "End Date", "is_date": True, "is_numeric": False},
        {"unified_name": "アーティスト名", "orchard_col": "PRODUCT ARTIST", "nextone_col": "アーティスト名", "itunes_col": "Artist", "is_date": False, "is_numeric": False},
        {"unified_name": "楽曲名", "orchard_col": "TRACK", "nextone_col": "楽曲名", "itunes_col": "Content Title", "is_date": False, "is_numeric": False},
        {"unified_name": "アルバム名", "orchard_col": "PRODUCT", "nextone_col": "アルバム名", "itunes_col": "Product", "is_date": False, "is_numeric": False},
        {"unified_name": "ISRC", "orchard_col": "ISRC", "nextone_col": "ISRC", "itunes_col": "ISRC", "is_date": False, "is_numeric": False},
        {"unified_name": "UPC_EAN", "orchard_col": "DISPLAY UPC", "nextone_col": "UPC", "itunes_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "ベンダー識別子", "orchard_col": "PRODUCT CODE", "nextone_col": "商品番号", "itunes_col": "Vendor Identifier", "is_date": False, "is_numeric": False},
        {"unified_name": "原盤アルバムコード", "orchard_col": "", "nextone_col": "原盤/アルバムコード", "itunes_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "アカウントID", "orchard_col": "ACCOUNT ID", "nextone_col": "", "itunes_col": "Apple Identifier", "is_date": False, "is_numeric": False},
        {"unified_name": "YouTube動画ID", "orchard_col": "YOUTUBE VIDEO ID", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "配信サービス名", "orchard_col": "STORE", "nextone_col": "DSP名", "itunes_col": "Report Type", "is_date": False, "is_numeric": False},
        {"unified_name": "サービス詳細", "orchard_col": "SERVICE DETAIL", "nextone_col": "サービス名", "itunes_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "国コード", "orchard_col": "SALE COUNTRY", "nextone_col": "国", "itunes_col": "Storefront Name", "is_date": False, "is_numeric": False},
        {"unified_name": "レーベル名", "orchard_col": "LABEL IMPRINT", "nextone_col": "レーベル名", "itunes_col": "Label/Studio/Network", "is_date": False, "is_numeric": False},
        {"unified_name": "数量", "orchard_col": "QUANTITY", "nextone_col": "数量", "itunes_col": "Total  Royalty Bearing Plays", "is_date": False, "is_numeric": True},
        {"unified_name": "印税額", "orchard_col": "NET SHARE ACCOUNT CURRENCY", "nextone_col": "総支払額", "itunes_col": "Net Royalty Total", "is_date": False, "is_numeric": True},
        {"unified_name": "売上総額", "orchard_col": "GROSS REVENUE ACCOUNT CURRENCY", "nextone_col": "使用料合計", "itunes_col": "", "is_date": False, "is_numeric": True},
        {"unified_name": "通貨", "orchard_col": "ACCOUNT CURRENCY", "nextone_col": "", "itunes_col": "Currency", "is_date": False, "is_numeric": False},
        {"unified_name": "為替レート", "orchard_col": "CURRENCY CONVERSION RATE", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": True},
        {"unified_name": "アルバムバージョン", "orchard_col": "PRODUCT VERSION", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "楽曲バージョン", "orchard_col": "TRACK VERSION", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "トラックアーティスト", "orchard_col": "TRACK ARTIST", "nextone_col": "", "itunes_col": "Artist", "is_date": False, "is_numeric": False},
        {"unified_name": "空間オーディオ判定", "orchard_col": "", "nextone_col": "", "itunes_col": "Spatial Availability Indicator", "is_date": False, "is_numeric": False},
        {"unified_name": "オフライン再生フラグ", "orchard_col": "", "nextone_col": "", "itunes_col": "Offline Indicator", "is_date": False, "is_numeric": False},
        {"unified_name": "販売種別", "orchard_col": "TRANSACTION TYPE", "nextone_col": "販売種別", "itunes_col": "Media Type", "is_date": False, "is_numeric": False},
        {"unified_name": "販売種別詳細", "orchard_col": "TRANSACTION SUBTYPE", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "配信区分", "orchard_col": "", "nextone_col": "配信区分", "itunes_col": "", "is_date": False, "is_numeric": False},
        {"unified_name": "サービスタイプ", "orchard_col": "", "nextone_col": "サービスタイプ", "itunes_col": "", "is_date": False, "is_numeric": False},
    ]

    # 統合テーブルは動的スキーマ(またはpandasから自動生成)を許容する方針とします
    # 基本構成として常に存在が期待されるものを定義しておくことも可能ですが、
    # マッピング変更に柔軟に対応するため autodetect に任せます。

    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = f"music-sales-raw-uploads-32010787277"
        self.client = bigquery.Client(project=project_id, location="asia-northeast1")
        self.storage_client = storage.Client(project=project_id)
        self._ensure_dataset_exists()

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
                query = f"DELETE FROM `{table_id}` WHERE FILE_NAME = @f"
                self.client.query(query, job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("f", "STRING", filename)]
                )).result()
            except exceptions.NotFound:
                pass # テーブル未作成の場合はスキップ

        import tempfile
        import os

        total_rows = len(df)
        df = df.copy()
        
        # BigQueryで扱えるようにdatetime型へ
        now = datetime.datetime.now().isoformat()
        df['uploaded_at'] = now
        
        # BigQueryの列名ルール（英数字とアンダースコア）に厳格に合わせる場合もありますが、
        # 近年のBQは柔軟なためそのまま送信を試みます。

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json', encoding='utf-8') as tmp:
            # ndjson出力時にpandasのto_jsonを使用
            # 日付などはそのまま文字列として扱う
            df.to_json(tmp.name, orient='records', lines=True, force_ascii=False)
            tmp_path = tmp.name

        if progress_callback:
            progress_callback(f"📦 クラウドへの最終転送を開始しています ({total_rows:,} 件)...")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]
        )
        try:
            with open(tmp_path, 'rb') as source_file:
                # location をデータセットに合わせて asia-northeast1 に固定
                job = self.client.load_table_from_file(source_file, table_id, job_config=job_config, location="asia-northeast1")
                job.result() # 完了まで待機
                
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
        query = f"DELETE FROM `{table_id}` WHERE FILE_NAME = @f"
        try:
            query_job = self.client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("f", "STRING", filename)]
            ))
            query_job.result()
            logging.info(f"Successfully deleted unified data for file: {filename}")
            return True
        except Exception as e:
            logging.error(f"Failed to delete unified data for {filename}: {e}")
            return False

    def check_file_exists(self, filename):
        if not filename: return False
        fn = filename.strip()
        table_id = f"{self.project_id}.{self.dataset_id}.unified_sales_data"
        query = f"SELECT count(*) as cnt FROM `{table_id}` WHERE FILE_NAME = @f"
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
            query = f"SELECT FILE_NAME as filename, SOURCE as source_type, COUNT(*) as row_count, max(uploaded_at) as uploaded_at FROM `{table_id}` GROUP BY FILE_NAME, SOURCE ORDER BY uploaded_at DESC"
            return self.client.query(query).to_dataframe()
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
        schema = [
            bigquery.SchemaField("unified_name", "STRING"),
            bigquery.SchemaField("orchard_col", "STRING"),
            bigquery.SchemaField("nextone_col", "STRING"),
            bigquery.SchemaField("itunes_col", "STRING"),
            bigquery.SchemaField("is_date", "BOOLEAN"),
            bigquery.SchemaField("is_numeric", "BOOLEAN"),
        ]
        self._ensure_table_exists(table_id, schema)
        
        # テーブルを一度空にする
        self.client.query(f"DELETE FROM `{table_id}` WHERE true").result()
        if not df.empty:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=schema)
            self.client.load_table_from_dataframe(df, table_id, job_config=job_config, location="asia-northeast1").result()
            logging.info(f"Saved mapping columns in batch: {len(df)} rows")

    def get_unified_columns(self):
        """マッピング設定を取得する（空の場合はデフォルトを読み込む）"""
        table_id = f"{self.project_id}.{self.dataset_id}.unified_columns"
        try:
            df = self.client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
            if df.empty:
                logging.info("Mapping table is empty. Restoring defaults...")
                self.save_unified_columns_batch(pd.DataFrame(self.DEFAULT_MAPPINGS))
                return pd.DataFrame(self.DEFAULT_MAPPINGS)
            return df
        except exceptions.NotFound:
            logging.info("Mapping table not found. Creating with defaults...")
            self.save_unified_columns_batch(pd.DataFrame(self.DEFAULT_MAPPINGS))
            return pd.DataFrame(self.DEFAULT_MAPPINGS)

    def delete_unified_column(self, name):
        table_id = f"{self.project_id}.{self.dataset_id}.unified_columns"
        query = f"DELETE FROM `{table_id}` WHERE unified_name = @n"
        try:
            self.client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("n", "STRING", name)]
            )).result()
        except exceptions.NotFound:
            pass

    def upload_large_file_via_gcs(self, local_path, filename, source_type, overwrite=True, progress_callback=None):
        """
        GCSを経由して大容量ファイルをBigQueryにロードします。
        """
        def notify(msg):
            if progress_callback:
                progress_callback(msg)

        table_id = f"{self.project_id}.{self.dataset_id}.raw_sales_data_v2"
        self._ensure_table_exists(table_id, self.RAW_SCHEMA)

        if overwrite:
            self.delete_raw_data(filename)

        # 1. ローカルファイルをNDJSONに変換 (ストリーミング処理)
        ndjson_path = os.path.join(tempfile.gettempdir(), f"{filename}.ndjson")
        now = datetime.datetime.now().isoformat()
        
        notify(f"🔄 {filename} をNDJSON形式に変換中...")
        try:
            chunks = pd.read_csv(local_path, sep=None, engine='python', chunksize=10000, on_bad_lines='skip')
            
            with open(ndjson_path, 'w', encoding='utf-8') as f:
                row_idx_offset = 0
                for chunk in chunks:
                    for i, row in chunk.iterrows():
                        line = {
                            'filename': filename,
                            'source_type': source_type,
                            'row_index': row_idx_offset + i,
                            'raw_row_json': json.dumps(row.to_dict(), ensure_ascii=False),
                            'uploaded_at': now
                        }
                        f.write(json.dumps(line, ensure_ascii=False) + '\n')
                    row_idx_offset += len(chunk)
                    notify(f"🔄 変換中: {row_idx_offset:,} 行を処理済み...")
            
            # 2. GCSへのレジュームアップロード
            notify(f"☁️ GCSへアップロード中 (レジュームアップロード)...")
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(f"tmp_load/{filename}.ndjson")
            blob.chunk_size = 10 * 1024 * 1024 
            blob.upload_from_filename(ndjson_path, content_type='application/x-ndjson', timeout=600)

            # 3. BigQueryへのロード (指数バックオフ付き)
            notify(f"📊 BigQueryへロード中 (数分かかる場合があります)...")
            gcs_uri = f"gs://{self.bucket_name}/{blob.name}"
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=self.RAW_SCHEMA
            )

            # QuotaExceeded (403) などのリトライ設定
            custom_retry = retry.Retry(
                predicate=retry.if_exception_type(exceptions.Forbidden, exceptions.ServiceUnavailable, exceptions.InternalServerError),
                initial=5.0,
                maximum=60.0,
                multiplier=2.0,
                deadline=600.0
            )

            def run_load_job():
                load_job = self.client.load_table_from_uri(
                    gcs_uri, table_id, job_config=job_config, retry=custom_retry,
                    location="asia-northeast1"
                )
                return load_job.result()

            run_load_job()
            notify(f"✅ {filename} のロードが完了しました。")
            return row_idx_offset

        finally:
            if os.path.exists(ndjson_path):
                os.remove(ndjson_path)
            try:
                bucket.blob(f"tmp_load/{filename}.ndjson").delete()
            except:
                pass

        return True

    # --- GCS Methods ---
    def get_gcs_signed_url(self, filename, content_type="application/octet-stream"):
        """ブラウザから直接アップロードするための署名付きURLを発行する"""
        import google.auth
        from google.auth.transport import requests as auth_requests
        
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(filename)
        
        # Cloud Run (Compute Engine credentials) では signBlob API を使用
        credentials, _ = google.auth.default()
        
        sa_email = getattr(credentials, 'service_account_email', None)
        logging.info(f"Initial SA email check: {sa_email} (ProjectID in DB: {self.project_id})")
        
        if not sa_email or sa_email == 'default':
            # Metadata server から実際のメールアドレスを取得を試みる (Cloud Run環境)
            try:
                import urllib.request
                req = urllib.request.Request(
                    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email",
                    headers={"Metadata-Flavor": "Google"}
                )
                with urllib.request.urlopen(req, timeout=1) as response:
                    sa_email = response.read().decode("utf-8").strip()
                logging.info(f"Metadata server detected SA: {sa_email}")
            except Exception as e:
                # 最終的なフォールバック
                if self.project_id == 'music-sales-project':
                    sa_email = "32010787277-compute@developer.gserviceaccount.com"
                else:
                    sa_email = f"{self.project_id}@appspot.gserviceaccount.com"
                logging.info(f"Fallback detected SA: {sa_email} (Reason: {e})")
        
        # 認証情報をリフレッシュ
        auth_request = auth_requests.Request()
        credentials.refresh(auth_request)
        
        # Consistent Content-Type to avoid SignatureDoesNotMatch
        url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(minutes=30),
            method="PUT",
            content_type=content_type,
            service_account_email=sa_email,
            access_token=credentials.token
        )
        return url

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
    # get_file_history が重複していたので削除
