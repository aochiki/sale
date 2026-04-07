import os
import sys
import argparse
import logging
from aggregator.database_bq import DatabaseManager

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description='GCSを経由して大容量CSV/TSVをBigQueryにロードします。')
    parser.add_argument('file_path', help='アップロードするローカルファイルのパス')
    parser.add_argument('--project_id', help='GCPプロジェクトID', default=os.getenv('GOOGLE_CLOUD_PROJECT'))
    parser.add_argument('--dataset_id', help='BigQueryデータセットID', default='sales_aggregator_dataset')
    parser.add_argument('--source_type', help='データソース (ORCHARD, NEXTONE, ITUNES, etc.)', default='AutoDetect')

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
        success = db_manager.upload_large_file_via_gcs(
            local_path=args.file_path,
            filename=filename,
            source_type=args.source_type,
            overwrite=True
        )
        if success:
            logging.info("🎉 アップロードとロードが完了しました。")
        else:
            logging.error("❌ 処理が失敗しました。")
    except Exception as e:
        logging.error(f"❌ 予期せぬエラーが発生しました: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
