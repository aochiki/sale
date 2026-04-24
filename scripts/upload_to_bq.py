import os
import sys
import argparse
import logging
import pandas as pd
from aggregator.database_bq import DatabaseManager
from aggregator.formatter import DataFormatter

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description='CSV/TSVファイルを整形してBigQueryにロードします。')
    parser.add_argument('file_path', help='アップロードするローカルファイルのパス')
    parser.add_argument('--project_id', help='GCPプロジェクトID', default=os.getenv('GOOGLE_CLOUD_PROJECT'))
    parser.add_argument('--dataset_id', help='BigQueryデータセットID', default='sales_aggregator_dataset')
    parser.add_argument('--source_type', help='データソース (ORCHARD, NEXTONE, ITUNES, YOUTUBE)', default=None)

    args = parser.parse_args()

    if not args.project_id:
        logging.error("プロジェクトIDが指定されていません。--project_id か環境変数 GOOGLE_CLOUD_PROJECT を設定してください。")
        sys.exit(1)

    if not os.path.exists(args.file_path):
        logging.error(f"ファイルが見つかりません: {args.file_path}")
        sys.exit(1)

    filename = os.path.basename(args.file_path)
    db_manager = DatabaseManager(project_id=args.project_id, dataset_id=args.dataset_id)

    logging.info(f"開始: {filename} を {args.project_id}.{args.dataset_id} へアップロードします...")

    try:
        # マッピングと為替レートを取得
        mappings = db_manager.get_unified_columns()
        exchange_rates = db_manager.get_exchange_rates()
        platforms_df = db_manager.get_platforms()

        # ファイルを整形
        formatter = DataFormatter(mappings, exchange_rates=exchange_rates)
        with open(args.file_path, 'rb') as f:
            df, unmapped_cols, raw_cols = formatter.format_file(
                f, filename, source_type=args.source_type, platforms_df=platforms_df
            )

        if df is None or df.empty:
            logging.error("❌ ファイルの解析に失敗しました。形式を確認してください。")
            sys.exit(1)

        logging.info(f"解析完了: {len(df):,} 行, {len(df.columns)} カラム")
        if unmapped_cols:
            logging.info(f"未マッピング列: {unmapped_cols}")

        # BigQueryに保存
        row_count = db_manager.save_unified_data(df, filename)
        logging.info(f"🎉 {row_count:,} 件のデータを登録しました。")

    except Exception as e:
        logging.error(f"❌ 予期せぬエラーが発生しました: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
