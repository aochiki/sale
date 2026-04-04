import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime
import os

Base = declarative_base()

class FileLog(Base):
    __tablename__ = 'file_logs'
    id = Column(Integer, primary_key=True)
    file_name = Column(String, unique=True, index=True)
    uploaded_at = Column(DateTime, default=datetime.datetime.now)

class DatabaseManager:
    def __init__(self, db_url="sqlite:///sales_data.db"):
        # SQLite の設定を最適化
        if db_url.startswith("sqlite"):
            # check_same_thread=False は Streamlit のようなマルチスレッド環境で必須
            connect_args = {"timeout": 30, "check_same_thread": False}
        else:
            connect_args = {}
            
        self.engine = create_engine(
            db_url, 
            connect_args=connect_args,
            pool_pre_ping=True # 接続生存確認
        )
        
        # WALモード（Write-Ahead Logging）を有効化して読み書きの並列性を向上させる
        if db_url.startswith("sqlite"):
            from sqlalchemy import text
            with self.engine.connect() as conn:
                conn.execute(text("PRAGMA journal_mode=WAL"))
                conn.commit()

        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)
    def check_file_exists(self, file_name):
        """ファイル名が既に登録されているか確認する"""
        session = self.Session()
        exists = session.query(FileLog).filter_by(file_name=file_name).first() is not None
        session.close()
        return exists

    def _add_missing_columns(self, df, table_name):
        """DataFrame にあってテーブルにないカラムを動的に追加する"""
        if not self._table_exists(table_name):
            return # テーブルがなければ to_sql が新規作成するので不要

        inspected = inspect(self.engine)
        existing_cols = [c['name'] for c in inspected.get_columns(table_name)]
        
        # 追加すべきカラムを特定
        new_cols = [c for c in df.columns if c not in existing_cols]
        
        if new_cols:
            from sqlalchemy import text
            with self.engine.begin() as conn:
                for col in new_cols:
                    # SQLite でカラム名に特殊文字（/, スペース等）が含まれる場合のクオート
                    # クォーテーションを付けて安全に ALTER TABLE
                    safe_col = f'"{col}"'
                    try:
                        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {safe_col} TEXT"))
                    except Exception as e:
                        # 既にカラムが存在する場合などのエラーは無視（並列処理対策）
                        pass

    def save_data(self, df, file_names, overwrite=False):
        """統合データを保存する。必要に応じてカラムを自動追加する"""
        table_name = "sales_records"
        
        # 1. カラムの自動調整
        self._add_missing_columns(df, table_name)

        session = self.Session()
        try:
            # 2. 既存ファイルの削除（上書きモードの場合）
            if overwrite:
                for fname in file_names:
                    # FileLogから削除
                    session.query(FileLog).filter_by(file_name=fname).delete()
                    # 実レコードからも削除
                    if self._table_exists(table_name):
                        from sqlalchemy import text
                        session.execute(
                            text(f"DELETE FROM {table_name} WHERE origin_file = :fname"),
                            {"fname": fname}
                        )

            # 3. ファイルログの記録
            for fname in file_names:
                if not session.query(FileLog).filter_by(file_name=fname).first():
                    session.add(FileLog(file_name=fname))
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        # 4. データの保存 (全カラム保持、セッションとは別接続)
        try:
            # chunksize を指定してメモリ負荷を軽減し高速化
            df.to_sql(table_name, self.engine, if_exists='append', index=False, chunksize=10000)
            
            # (任意) 初回時のみ origin_file 等にインデックスを追加
            with self.engine.begin() as conn:
                from sqlalchemy import text
                try:
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_origin_file ON {table_name} (ORIGIN_FILE)"))
                except:
                    pass
            return True
        except Exception as e:
            raise e

    def get_all_data(self):
        """保存されている全データを取得する"""
        table_name = "sales_records"
        if not self._table_exists(table_name):
            return pd.DataFrame()
        return pd.read_sql(table_name, self.engine)

    def get_file_logs(self):
        """アップロード済みのファイル一覧を取得する"""
        return pd.read_sql("file_logs", self.engine)

    def _table_exists(self, table_name):
        inspected = inspect(self.engine)
        return table_name in inspected.get_table_names()

    def clear_all_data(self):
        """全データをリセット（テスト用）"""
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        if self._table_exists("sales_records"):
             from sqlalchemy import text
             with self.engine.begin() as conn:
                 conn.execute(text("DROP TABLE sales_records"))
