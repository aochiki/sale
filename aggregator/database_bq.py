from google.cloud import bigquery
from google.cloud import exceptions
import pandas as pd
import datetime
import logging
import json

class DatabaseManager:
    # RAW テーブルのスキーマ定義（一元管理）
    RAW_SCHEMA = [
        bigquery.SchemaField("filename", "STRING"),
        bigquery.SchemaField("source_type", "STRING"),
        bigquery.SchemaField("row_index", "INTEGER"),
        bigquery.SchemaField("raw_row_json", "STRING"),
        bigquery.SchemaField("uploaded_at", "TIMESTAMP"),
    ]

    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id, location="asia-northeast1")
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
        """データセット内のすべてのテーブルを削除して完全に初期化する"""
        tables = self.client.list_tables(f"{self.project_id}.{self.dataset_id}")
        for table in tables:
            self.client.delete_table(table.reference, not_found_ok=True)
        logging.info("Dataset reset complete.")

    def save_raw_data(self, df, filename, source_type, overwrite=True):
        """解析なしで、各行を個別のJSON行として RAW テーブルに保存する"""
        table_id = f"{self.project_id}.{self.dataset_id}.raw_sales_data_v2"
        
        self._ensure_table_exists(table_id, self.RAW_SCHEMA)
        
        if overwrite:
            query = f"DELETE FROM `{table_id}` WHERE filename = @f"
            self.client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("f", "STRING", filename)]
            )).result()

        # 各行をJSONに変換してリスト化
        data_to_load = []
        now = datetime.datetime.now().isoformat()
        for i, row in df.iterrows():
            data_to_load.append({
                'filename': filename,
                'source_type': source_type,
                'row_index': i,
                'raw_row_json': json.dumps(row.to_dict(), ensure_ascii=False),
                'uploaded_at': now
            })
        
        # 分割してアップロード (あまりに巨大な場合はチャンク分けも検討するが、まずは1ファイル単位)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        try:
            self.client.load_table_from_json(data_to_load, table_id, job_config=job_config, location="asia-northeast1").result()
            logging.info(f"Successfully saved {len(data_to_load)} rows for RAW data: {filename}")
            return len(data_to_load)
        except Exception as e:
            logging.error(f"Failed to save RAW individual data: {e}")
            raise

    def delete_raw_data(self, filename):
        """特定のファイルに関連する RAW データをすべて削除する"""
        table_id = f"{self.project_id}.{self.dataset_id}.raw_sales_data_v2"
        query = f"DELETE FROM `{table_id}` WHERE filename = @f"
        try:
            query_job = self.client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("f", "STRING", filename)]
            ))
            query_job.result()
            logging.info(f"Successfully deleted raw data for file: {filename}")
            return True
        except Exception as e:
            logging.error(f"Failed to delete raw data for {filename}: {e}")
            return False

    def get_raw_data(self):
        """保存されているすべての RAW データを取得する"""
        table_id = f"{self.project_id}.{self.dataset_id}.raw_sales_data_v2"
        try:
            # 行順序を維持して取得
            return self.client.query(f"SELECT * FROM `{table_id}` ORDER BY filename, row_index").to_dataframe()
        except exceptions.NotFound:
            return pd.DataFrame()

    def get_unique_headers(self, source_type):
        """RAWデータから、特定の提供元が持つ実際の列名リストを抽出する"""
        table_id = f"{self.project_id}.{self.dataset_id}.raw_sales_data_v2"
        # 1行だけ取得してヘッダーを特定する
        query = f"SELECT raw_row_json FROM `{table_id}` WHERE source_type = @st LIMIT 1"
        try:
            results = self.client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("st", "STRING", source_type)]
            )).result()
            for row in results:
                data = json.loads(row.raw_row_json)
                return sorted(list(data.keys()))
        except (exceptions.NotFound, StopIteration):
            pass
        return []

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

    def save_unified_column(self, unified_name, orchard_col, nextone_col, itunes_col, is_date, is_numeric):
        table_id = f"{self.project_id}.{self.dataset_id}.unified_columns"
        # スキーマを明示して作成を確実にする
        schema = [
            bigquery.SchemaField("unified_name", "STRING"),
            bigquery.SchemaField("orchard_col", "STRING"),
            bigquery.SchemaField("nextone_col", "STRING"),
            bigquery.SchemaField("itunes_col", "STRING"),
            bigquery.SchemaField("is_date", "BOOLEAN"),
            bigquery.SchemaField("is_numeric", "BOOLEAN"),
        ]
        self._ensure_table_exists(table_id, schema)

        # 既存あれば削除
        self.delete_unified_column(unified_name)
        df = pd.DataFrame([{
            'unified_name': unified_name,
            'orchard_col': orchard_col,
            'nextone_col': nextone_col,
            'itunes_col': itunes_col,
            'is_date': is_date,
            'is_numeric': is_numeric
        }])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        # リージョンを明示
        self.client.load_table_from_dataframe(df, table_id, job_config=job_config, location="asia-northeast1").result()
        logging.info(f"Saved unified column: {unified_name}")

    def get_unified_columns(self):
        table_id = f"{self.project_id}.{self.dataset_id}.unified_columns"
        try:
            return self.client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
        except exceptions.NotFound:
            return pd.DataFrame()

    def delete_unified_column(self, name):
        table_id = f"{self.project_id}.{self.dataset_id}.unified_columns"
        query = f"DELETE FROM `{table_id}` WHERE unified_name = @n"
        try:
            self.client.query(query, job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("n", "STRING", name)]
            )).result()
        except exceptions.NotFound:
            pass
