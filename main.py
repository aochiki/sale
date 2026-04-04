import logging

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("売上データの自動ダウンロード・集計処理を開始します。")

    # 1. The Orchardからのデータダウンロード
    logger.info("The Orchardのデータダウンロードを開始...")
    # TODO: orchard_scraperの実装呼び出し

    # 2. NexToneからのデータダウンロード
    logger.info("NexToneのデータダウンロードを開始...")
    # TODO: nextone_scraperの実装呼び出し

    # 3. iTunes (Apple Music)からのデータダウンロード
    logger.info("iTunes (Apple Music)のデータダウンロードを開始...")
    # TODO: itunes_apiクライアントの実装呼び出し

    # 4. データの集計・結合処理
    logger.info("データの集計・統合処理を開始...")
    # TODO: aggregatorの実装呼び出し

    logger.info("全ての処理が完了しました。")

if __name__ == "__main__":
    main()
