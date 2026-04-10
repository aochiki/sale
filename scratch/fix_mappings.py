from aggregator.database_bq import DatabaseManager
import os

project_id = 'music-sales-project'
dataset_id = 'sales_aggregator_dataset'

db = DatabaseManager(project_id=project_id, dataset_id=dataset_id)

# 修正するマッピング定義
# unified_name (化けている可能性のあるもの) をキーにして、正しい名前に変換
corrections = [
    # { 'unified_name': '曲名', 'orchard_col': 'TRACK', 'nextone_col': '楽曲名', 'itunes_col': 'Content Title', 'is_date': False, 'is_numeric': False },
    # { 'unified_name': 'アーティスト名', 'orchard_col': 'PRODUCT ARTIST', 'nextone_col': 'アーティスト名', 'itunes_col': 'Artist', 'is_date': False, 'is_numeric': False },
    # { 'unified_name': '数量', 'orchard_col': 'QUANTITY', 'nextone_col': '数量', 'itunes_col': 'Total  Royalty Bearing Plays', 'is_date': False, 'is_numeric': True },
]

# 現在の状況を取得
mappings = db.get_unified_columns()
print(f"Current mappings:\n{mappings}")

# 特定のカラムを正しい名前に修正して再保存
# 文字化けしているものを特定して上書き
for _, row in mappings.iterrows():
    u = row['unified_name']
    new_u = u
    
    # 文字列の特徴から判定 (文字化けはCP932/Latin1の問題)
    if 'Ȗ' in u or 'y' in u or 'TRACK' == row['orchard_col']:
        new_u = '曲名'
    elif 'eB' in u or 'PRODUCT ARTIST' == row['orchard_col']:
        new_u = 'アーティスト名'
    elif '\t' in u or 'QUANTITY' == row['orchard_col']:
        new_u = '数量'
    elif 'ISRC' == row['unified_name']:
        new_u = 'ISRC'
    
    print(f"Updating: '{u}' -> '{new_u}'")
    db.save_unified_column(
        new_u, 
        row['orchard_col'], 
        row['nextone_col'], 
        row['itunes_col'], 
        row['is_date'], 
        row['is_numeric']
    )
    
    # もし名前が変わった場合、古い（化けた）方のレコードを削除する必要がある場合がある
    # save_unified_column は new_u で削除するので、古い名前(u)が異なる場合は別途削除
    if u != new_u:
        db.delete_unified_column(u)

print("Normalization complete.")
