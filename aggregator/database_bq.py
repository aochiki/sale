import pandas as pd
import datetime
import os
import logging
from google.cloud import bigquery
from google.api_core import exceptions

class DatabaseManager:
    def __init__(self, project_id=None, dataset_id="sales_data"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = None
        if project_id:
            try:
                self.client = bigquery.Client(project=project_id)
                self._ensure_dataset_exists()
            except Exception as e:
                logging.error(f"BigQuery 接続エラー: {e}")

    def _ensure_dataset_exists(self):
        """データセットが存在しなければ作成する"""
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except exceptions.NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US" # デフォルト
            self.client.create_dataset(dataset)
            logging.info(f"データセット {self.dataset_id} を作成しました。")

    def check_file_exists(self, file_name):
        """ファイル名が既に登録されているか確認する"""
        if not self.client: return False
        table_id = f"{self.project_id}.{self.dataset_id}.file_logs"
        query = f"SELECT count(1) as cnt FROM `{table_id}` WHERE file_name = @fname"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("fname", "STRING", file_name)]
        )
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.to_dataframe()
            return results['cnt'].iloc[0] > 0
        except:
            return False

    def save_data(self, df, file_names, overwrite=False):
        """データを BigQuery に保存する（自動カラム追加対応）"""
        if not self.client: raise Exception("BigQuery プロジェクトIDが設定されていません。")
        
        table_id = f"{self.project_id}.{self.dataset_id}.sales_records"
        
        # 1. 既存データの削除 (上書きモード)
        if overwrite:
            for fname in file_names:
                # file_logs から削除
                log_table = f"{self.project_id}.{self.dataset_id}.file_logs"
                delete_log = f"DELETE FROM `{log_table}` WHERE file_name = @fname"
                self.client.query(delete_log, job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("fname", "STRING", fname)]
                ))
                # sales_records から削除
                delete_data = f"DELETE FROM `{table_id}` WHERE ORIGIN_FILE = @fname"
                try:
                    self.client.query(delete_data, job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("fname", "STRING", fname)]
                    ))
                except exceptions.NotFound:
                    pass # テーブルがまだない場合は無視

        # 2. データのアップロード (自動カラム追加)
        job_config = bigquery.LoadJobConfig(
            # テーブルが存在しない場合は作成し、あれば追加
            write_disposition="WRITE_APPEND",
            # 新しいカラムを自動で追加することを許可
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            autodetect=True
        )
        
        # BigQuery は Datetime 型を好むが、Pandas の NaT がある場合は注意が必要
        # 必要に応じて文字列形式にキャストするなどの処理を入れる
        load_job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result() # 完了を待つ

        # 3. ファイルログの記録
        log_df = pd.DataFrame([{"file_name": f, "uploaded_at": datetime.datetime.now()} for f in file_names])
        log_table = f"{self.project_id}.{self.dataset_id}.file_logs"
        self.client.load_table_from_dataframe(log_df, log_table).result()
        
        return True

    def get_all_data(self):
        """保存されている全データを取得する"""
        if not self.client: return pd.DataFrame()
        table_id = f"{self.project_id}.{self.dataset_id}.sales_records"
        try:
            query = f"SELECT * FROM `{table_id}`"
            return self.client.query(query).to_dataframe()
        except exceptions.NotFound:
            return pd.DataFrame()

    def clear_all_data(self):
        """全データを削除（データセット自体を削除して再作成）"""
        if not self.client: return
        self.client.delete_dataset(self.dataset_id, delete_contents=True, not_found_ok=True)
        self._ensure_dataset_exists()
