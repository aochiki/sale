import os
import pandas as pd
from aggregator.database_bq import DatabaseManager
from dotenv import load_dotenv

load_dotenv()
project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'nippo-app-491512')
db = DatabaseManager(project_id=project_id, dataset_id='sales_aggregator_dataset')

# マッピングデータの作成 (拡張版)
mappings = [
    # --- 基本項目 ---
    {"unified_name": "売上確定日", "orchard_col": "STATEMENT PERIOD", "nextone_col": "", "itunes_col": "End Date", "is_date": True, "is_numeric": False},
    {"unified_name": "利用発生月", "orchard_col": "TRANSACTION DATE", "nextone_col": "利用発生月", "itunes_col": "End Date", "is_date": True, "is_numeric": False},
    {"unified_name": "アーティスト名", "orchard_col": "PRODUCT ARTIST", "nextone_col": "アーティスト名", "itunes_col": "Artist", "is_date": False, "is_numeric": False},
    {"unified_name": "楽曲名", "orchard_col": "TRACK", "nextone_col": "楽曲名", "itunes_col": "Title", "is_date": False, "is_numeric": False},
    {"unified_name": "アルバム名", "orchard_col": "PRODUCT", "nextone_col": "アルバム名", "itunes_col": "Album Title", "is_date": False, "is_numeric": False},
    
    # --- コード類 ---
    {"unified_name": "ISRC", "orchard_col": "ISRC", "nextone_col": "ISRC", "itunes_col": "ISRC", "is_date": False, "is_numeric": False},
    {"unified_name": "UPC_EAN", "orchard_col": "DISPLAY UPC", "nextone_col": "UPC/EAN", "itunes_col": "UPC", "is_date": False, "is_numeric": False},
    {"unified_name": "ベンダー識別子", "orchard_col": "PRODUCT CODE", "nextone_col": "ベンダー識別子", "itunes_col": "Vendor Identifier", "is_date": False, "is_numeric": False},
    {"unified_name": "アカウントID", "orchard_col": "ACCOUNT ID", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
    
    # --- 配信先 ---
    {"unified_name": "配信サービス名", "orchard_col": "STORE", "nextone_col": "配信サービス名", "itunes_col": "Provider", "is_date": False, "is_numeric": False},
    {"unified_name": "サービス詳細", "orchard_col": "SERVICE DETAIL", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
    {"unified_name": "国コード", "orchard_col": "SALE COUNTRY", "nextone_col": "国コード", "itunes_col": "Country", "is_date": False, "is_numeric": False},
    {"unified_name": "レーベル名", "orchard_col": "LABEL IMPRINT", "nextone_col": "レーベル名", "itunes_col": "Label", "is_date": False, "is_numeric": False},
    
    # --- 実績 ---
    {"unified_name": "数量", "orchard_col": "QUANTITY", "nextone_col": "数量", "itunes_col": "Quantity", "is_date": False, "is_numeric": True},
    {"unified_name": "印税額", "orchard_col": "NET SHARE ACCOUNT CURRENCY", "nextone_col": "印税額", "itunes_col": "Royalty", "is_date": False, "is_numeric": True},
    {"unified_name": "売上総額", "orchard_col": "GROSS REVENUE ACCOUNT CURRENCY", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": True},
    {"unified_name": "通貨", "orchard_col": "ACCOUNT CURRENCY", "nextone_col": "通貨", "itunes_col": "Currency", "is_date": False, "is_numeric": False},
    {"unified_name": "為替レート", "orchard_col": "CURRENCY CONVERSION RATE", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": True},
    
    # --- 属性・詳細 ---
    {"unified_name": "アルバムバージョン", "orchard_col": "PRODUCT VERSION", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
    {"unified_name": "楽曲バージョン", "orchard_col": "TRACK VERSION", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
    {"unified_name": "トラックアーティスト", "orchard_col": "TRACK ARTIST", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
    {"unified_name": "YouTube動画ID", "orchard_col": "YOUTUBE VIDEO ID", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
    {"unified_name": "販売種別", "orchard_col": "TRANSACTION TYPE", "nextone_col": "販売種別", "itunes_col": "Sale Type", "is_date": False, "is_numeric": False},
    {"unified_name": "販売種別詳細", "orchard_col": "TRANSACTION SUBTYPE", "nextone_col": "", "itunes_col": "", "is_date": False, "is_numeric": False},
]

df_mappings = pd.DataFrame(mappings)

print("Updating detailed unified_columns in BigQuery...")
db.save_unified_columns_batch(df_mappings)
print("Update complete!")
