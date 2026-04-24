import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from aggregator.formatter import DataFormatter
from aggregator.database_bq import DatabaseManager
from aggregator.ai_query import parse_natural_language_query
import io
import datetime
import logging
import os
import json
import time
import uuid
from dotenv import load_dotenv
load_dotenv()

# --- Page Config ---
st.set_page_config(
    page_title="売上データ統合システム", 
    page_icon="📊",
    layout="wide", 
    initial_sidebar_state="collapsed"
)

# --- Premium Style ---
st.markdown("""
<style>
    .block-container { padding-left: 5rem; padding-right: 5rem; }
    .stApp { background-color: #fcfcfc; }
    h1 { font-weight: 800; color: #1a1a1a; }
    .stTabs [data-baseweb="tab"] { font-weight: 600; }
    div[data-testid="stExpander"] { background-color: white; border-radius: 12px; }
</style>
""", unsafe_allow_html=True)

# --- Database & Processor Logic ---
@st.cache_resource
def get_db_v3(project_id):
    dataset_id = "sales_aggregator_dataset"
    return DatabaseManager(project_id=project_id, dataset_id=dataset_id)

@st.cache_resource
def get_exchange_service():
    import importlib
    import aggregator.exchange_service
    importlib.reload(aggregator.exchange_service)
    from aggregator.exchange_service import ExchangeRateService
    return ExchangeRateService()

@st.cache_data(ttl=300)
def fetch_unified_data(project_id):
    return get_db_v3(project_id).get_unified_data()

@st.cache_data(ttl=60)
def fetch_mappings(project_id):
    return get_db_v3(project_id).get_unified_columns()

@st.cache_data(ttl=60)
def fetch_rules(project_id):
    return get_db_v3(project_id).get_parsing_rules()

@st.cache_data(ttl=60)
def fetch_exchange_rates(project_id):
    return get_db_v3(project_id).get_exchange_rates()

@st.cache_data(ttl=60)
def fetch_master_columns(project_id):
    return get_db_v3(project_id).get_master_columns()

@st.cache_data(ttl=60)
def fetch_platforms(project_id):
    return get_db_v3(project_id).get_platforms()

@st.cache_data(ttl=60)
def fetch_discovered_headers_v2(project_id):
    return get_db_v3(project_id).get_discovered_headers()

@st.cache_data(ttl=60)
def fetch_file_history(project_id):
    return get_db_v3(project_id).get_file_history()

def clear_app_cache():
    st.cache_data.clear()
    st.cache_resource.clear()

st.title("📊 売上データ管理システム")
st.caption("Standardized Data Aggregation & AI Analysis")
st.markdown("---")

# --- Initial State & Config ---
if 'project_id' not in st.session_state:
    st.session_state.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'music-sales-project')

if 'gemini_api_key' not in st.session_state:
    st.session_state.gemini_api_key = os.getenv('GEMINI_API_KEY', '')

project_id = st.session_state.project_id
gemini_api_key = st.session_state.gemini_api_key

db_manager = None
if project_id:
    db_manager = get_db_v3(project_id)
    unified_df = fetch_unified_data(project_id)
    raw_mappings = fetch_mappings(project_id) # DBから全量取得
    rules = fetch_rules(project_id)
    master_cols_df = fetch_master_columns(project_id)
    platforms_df = fetch_platforms(project_id)
    discovered_headers_df = fetch_discovered_headers_v2(project_id)

    # 重要：マッピング情報を現在のマスター項目のみに厳格に絞り込む (古い項目の混入を防ぐ)
    mappings = master_cols_df[['unified_name', 'is_date', 'is_numeric']].merge(
        raw_mappings.drop(columns=['is_date', 'is_numeric'], errors='ignore'),
        on='unified_name',
        how='left'
    )
    # 型に応じて適切に埋める (真偽値に "" は入れられないため)
    for col in mappings.columns:
        if mappings[col].dtype == 'bool' or mappings[col].dtype == 'boolean':
            mappings[col] = mappings[col].fillna(False)
        else:
            mappings[col] = mappings[col].fillna("")

    # ワークスペース用の状態管理 (設定変更後は削除して再初期化される)
    if "mappings_df" not in st.session_state:
        # 1. 最新の項目リスト（master_cols_df）をベースにする
        base_df = master_cols_df[['unified_name', 'is_date', 'is_numeric']].copy()
        # 2. 既存のマッピング（mappings）をマージして内容を引き継ぐ
        df = base_df.merge(mappings.drop(columns=['is_date', 'is_numeric'], errors='ignore'), on='unified_name', how='left')
        
        # 最新のプラットフォーム構成に合わせて列を調整
        p_keys = platforms_df['key'].tolist()
        # 足りないプラットフォーム列を空文字で補完
        for col in p_keys:
            if col not in df.columns: df[col] = ""
        # 不要なプラットフォーム列を除去、およびマスター項目のみに絞り込み
        cols_to_keep = ['unified_name', 'is_date', 'is_numeric'] + p_keys
        df = df[[c for c in df.columns if c in cols_to_keep]]
        
        # 文字列型の列の欠損値を空文字に設定
        for c in df.columns:
            if df[c].dtype == object: df[c] = df[c].fillna("")
            
        # インデックス初期化
        st.session_state.mappings_df = df.reset_index(drop=True)
    
    current_mappings = st.session_state.mappings_df

tab_view, tab_flexible, tab_ai, tab_upload, tab_settings = st.tabs([
    "📋 売上データ閲覧", "📊 自由集計", "🤖 AI集計", "📥 RAWデータ追加", "⚙️ システム管理"
])

# --- 1. 閲覧タブ ---
with tab_view:
    if not project_id:
        st.info("💡 「システム管理」タブで GCP Project ID を設定してください。")
        st.stop()
    
    if unified_df.empty:
        st.info("データがありません。RAWデータをアップロードしてください。")
    else:
        c1, c2 = st.columns(2)
        month_col = next((c for c in unified_df.columns if not mappings.empty and mappings[mappings['unified_name']==c]['is_date'].any()), None)
        month_list = ["すべて"] + sorted(unified_df[month_col].dropna().unique().tolist(), reverse=True) if month_col else ["すべて"]
        sel_m = c1.selectbox("📅 対象月", month_list)
        sel_s = c2.selectbox("📡 ソース", ["すべて"] + sorted(unified_df['SOURCE'].unique().tolist()))
        
        filtered = unified_df.copy()
        if sel_m != "すべて": filtered = filtered[filtered[month_col] == sel_m]
        if sel_s != "すべて": filtered = filtered[filtered['SOURCE'] == sel_s]
        
        # 構成管理の順序に従って列を並べ替え
        display_cols = master_cols_df['unified_name'].tolist()
        existing_cols = [c for c in display_cols if c in filtered.columns]
        other_cols = [c for c in filtered.columns if c not in display_cols]
        filtered = filtered[existing_cols + other_cols]
        
        # 数値列の型変換と書式設定
        num_cols = master_cols_df[master_cols_df['is_numeric'] == True]['unified_name'].tolist()
        format_dict = {}
        for nc in num_cols:
            if nc in filtered.columns:
                filtered[nc] = pd.to_numeric(filtered[nc], errors='coerce').fillna(0)
                format_dict[nc] = "{:,.0f}"

        st.dataframe(filtered.style.format(format_dict), use_container_width=True, hide_index=True)
        st.download_button("📥 CSVダウンロード", filtered.to_csv(index=False), f"unified_{datetime.datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

# --- 2. 自由集計タブ ---
with tab_flexible:
    if unified_df.empty:
        st.info("集計可能なデータがありません。")
    else:
        st.subheader("📊 ピボット集計")
        attr_cols = [m['unified_name'] for _, m in mappings.iterrows() if not m['is_date'] and not m['is_numeric']]
        num_cols = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
        
        c1, c2, c3 = st.columns(3)
        row_axis = c1.selectbox("縦軸", [None] + attr_cols + ['SOURCE'])
        col_axis = c2.selectbox("横軸", [None] + attr_cols + ['SOURCE'])
        val_axis = c3.selectbox("集計値", num_cols if num_cols else [None])
        
        if val_axis:
            try:
                pivot_df = unified_df.pivot_table(index=row_axis, columns=col_axis, values=val_axis, aggfunc='sum', margins=True, margins_name="合計")
                st.dataframe(pivot_df.style.format("{:,.0f}"), use_container_width=True)
            except Exception:
                st.info("集計できませんでした。軸の組み合わせを確認してください。")

# --- 3. AI集計タブ ---
with tab_ai:
    if unified_df.empty:
        st.info("集計可能なデータがありません。")
    else:
        st.subheader("🤖 AI自然言語集計")
        user_query = st.text_area("✍️ 集計の要望を入力", placeholder="「アーティストごとの売上合計」など...")
        if st.button("🚀 AI解析を実行", type="primary") and user_query:
            with st.spinner("AIが意図を解析中..."):
                all_cols = [m['unified_name'] for _, m in mappings.iterrows()] + (['SOURCE'] if 'SOURCE' in unified_df.columns else [])
                num_cols_ai = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
                parsed = parse_natural_language_query(project_id, user_query, all_cols, num_cols_ai, api_key=gemini_api_key)
                if parsed and "error" not in parsed:
                    st.success("✅ AIによる解析が完了しました。")
                    
                    # 1. フィルタリング
                    filtered_df = unified_df.copy()
                    filters = parsed.get("filters", {})
                    if filters:
                        import unicodedata
                        def normalize_text(t):
                            if not isinstance(t, str): t = str(t)
                            # 全角英数字を半角に、カタカナを全角に、などの正規化 (NFKC)
                            return unicodedata.normalize('NFKC', t).lower()

                        with st.expander("🔍 適用されたフィルター", expanded=False):
                            for col, val in filters.items():
                                if col in filtered_df.columns and val:
                                    st.write(f"- **{col}**: {val}")
                                    norm_val = normalize_text(val)
                                    # データ側も正規化して比較
                                    mask = filtered_df[col].astype(str).apply(normalize_text).str.contains(norm_val, na=False)
                                    filtered_df = filtered_df[mask]
                    
                    # 2. ピボット集計
                    row_axis = parsed.get("row_axis")
                    col_axis = parsed.get("col_axis")
                    value_axis = parsed.get("value_axis", [])
                    
                    # value_axis がリストでない場合の補正
                    if isinstance(value_axis, str): value_axis = [value_axis]
                    # 有効な数値列のみに絞る
                    value_axis = [v for v in value_axis if v in filtered_df.columns]
                    
                    if not value_axis:
                        st.warning("集計対象となる数値項目が見つかりませんでした。要望を詳しく入力してみてください。")
                    else:
                        try:
                            # 数値列の型変換（念のため）
                            for v in value_axis:
                                filtered_df[v] = pd.to_numeric(filtered_df[v], errors='coerce').fillna(0)
                            
                            # ピボットテーブル作成 または 単純合計
                            if not row_axis and not col_axis:
                                # 軸がない場合は単純合計を表示
                                st.subheader("📊 合計結果")
                                total_vals = filtered_df[value_axis].sum().to_frame().T
                                total_vals.index = ["合計"]
                                st.dataframe(total_vals.style.format("{:,.0f}"), use_container_width=True)
                                pivot_results = total_vals
                            else:
                                # 軸がある場合はピボットテーブル
                                pivot_results = filtered_df.pivot_table(
                                    index=row_axis if row_axis else None,
                                    columns=col_axis if col_axis else None,
                                    values=value_axis,
                                    aggfunc='sum',
                                    margins=True,
                                    margins_name="合計"
                                )
                                st.subheader("📊 集計結果")
                                st.dataframe(pivot_results.style.format("{:,.0f}"), use_container_width=True)
                            
                            # ダウンロードボタン
                            csv = pivot_results.to_csv().encode('utf-8-sig')
                            st.download_button("📥 集計結果をCSVで保存", csv, "ai_analysis_result.csv", "text/csv")
                            
                        except Exception as e:
                            st.error(f"集計処理中にエラーが発生しました: {e}")
                            st.info("AIの解析結果:")
                            st.json(parsed)
                else:
                    st.error(f"解析エラー: {parsed.get('error') if parsed else 'Unknown error'}")

# --- 4. RAWデータ追加タブ ---
with tab_upload:
    st.subheader("📥 売上データのアップロード")
    st.caption("ファイルをアップロードして共通フォーマットへ自動変換・登録します。")
    
    if '_up_uuid' not in st.session_state: st.session_state._up_uuid = uuid.uuid4().hex[:8]
    uid = st.session_state._up_uuid
    temp_data_path = f"up_data_{uid}.bin"
    temp_tag_path = f"up_tag_{uid}.txt"

    # Fallback to standard uploader for simplicity in this script
    std_file = st.file_uploader("ここにファイルをドラッグ＆ドロップまたは選択してください", type=["csv", "tsv", "txt", "txt.gz"])
    if std_file:
        file_key = f"uploaded_{std_file.name}_{std_file.size}"
        if st.session_state.get('last_up_key') != file_key:
            with st.status("⌛ アップロード中...") as stat:
                db_manager.upload_to_gcs_direct(std_file, temp_data_path)
                tag_io = io.BytesIO(std_file.name.encode('utf-8'))
                db_manager.upload_to_gcs_direct(tag_io, temp_tag_path)
                st.session_state.last_up_key = file_key
                st.session_state.reg_state = 'idle'
                st.session_state.reg_preview = None
                stat.update(label="✅ アップロード完了。解析を開始してください。", state="complete")
                st.rerun()

    if 'reg_state' not in st.session_state: st.session_state.reg_state = 'idle'
    if 'reg_preview' not in st.session_state: st.session_state.reg_preview = None

    try:
        tag_io = db_manager.get_gcs_blob_io(temp_tag_path)
        if tag_io:
            detected_fn = tag_io.read().decode('utf-8').strip()
            
            # 自動解析ロジック: プレビューがまだなければ自動で実行
            if st.session_state.reg_state == 'idle':
                with st.status(f"⌛ **{detected_fn}** を自動解析中...") as stat:
                    formatter = DataFormatter(
                        mappings, 
                        exchange_rates=fetch_exchange_rates(project_id),
                        exchange_service=get_exchange_service()
                    )
                    df, unmapped_cols, raw_cols = formatter.format_file(db_manager.get_gcs_blob_io(temp_data_path), detected_fn, platforms_df=platforms_df)
                    if df is not None:
                        st.session_state.reg_preview = (df, unmapped_cols, detected_fn)
                        st.session_state.reg_state = 'preview'
                        stat.update(label="✅ 解析完了。内容を確認してください。", state="complete")
                        st.rerun()
        else:
            # ファイルが存在しない場合は状態をリセット
            if st.session_state.reg_state != 'idle':
                st.session_state.reg_state = 'idle'
                st.session_state.reg_preview = None
                st.rerun()
        
        # 安全装置: プレビューデータがないのに preview 状態ならリセット
        if st.session_state.reg_state == 'preview' and not st.session_state.reg_preview:
            st.session_state.reg_state = 'idle'
            st.rerun()
            
        if st.session_state.reg_state == 'preview' and st.session_state.reg_preview:
                df, unmapped, fn = st.session_state.reg_preview
                st.markdown(f"### 🔍 **{fn}** のプレビュー")
                
                # 重複チェックの警告表示
                if db_manager.check_file_exists(fn):
                    st.warning(f"⚠️ **{fn}** は既に登録されています。再度登録すると既存のデータは上書きされます。")
                
                st.dataframe(df.head(5), use_container_width=True)
                c1, c2 = st.columns([1, 1])
                if c1.button("🚀 この内容で登録を確定する", type="primary", use_container_width=True):
                    with st.status("🚀 送信中...") as stat:
                        row_count = db_manager.save_unified_data(df, fn)
                        db_manager.delete_gcs_file(temp_data_path)
                        db_manager.delete_gcs_file(temp_tag_path)
                        st.session_state.reg_state = 'idle'
                        st.session_state.reg_preview = None
                        stat.update(label=f"✅ {row_count:,} 件の登録が完了しました", state="complete")
                        clear_app_cache()
                        time.sleep(1); st.rerun()
                
                if c2.button("✖️ キャンセル", use_container_width=True):
                    db_manager.delete_gcs_file(temp_data_path)
                    db_manager.delete_gcs_file(temp_tag_path)
                    st.session_state.reg_state = 'idle'
                    st.session_state.reg_preview = None
                    st.success("キャンセルしました。")
                    time.sleep(1); st.rerun()
    except Exception as e:
        if "get_rates_batch" in str(e):
            st.warning("⚠️ システムの更新を反映させるため、以下のボタンを押してキャッシュをクリアしてください。")
            if st.button("🔄 キャッシュを強制クリアして再試行", type="primary"):
                clear_app_cache()
                st.rerun()
        
        st.error(f"❌ 処理中にエラーが発生しました: {e}")
        logging.error(f"Error in data registration: {e}")
        if st.button("🔄 状態をリセットする"):
            st.session_state.reg_state = 'idle'
            st.session_state.reg_preview = None
            st.rerun()

    st.divider()
    st.subheader("📋 アップロード済みファイル履歴")
    history_df = fetch_file_history(project_id)
    if history_df.empty:
        st.info("アップロード済みのファイルはありません。")
    else:
        # 書式設定（日時を見やすく）
        if 'uploaded_at' in history_df.columns:
            history_df['uploaded_at'] = pd.to_datetime(history_df['uploaded_at']).dt.strftime('%Y/%m/%d %H:%M')
        
        # 削除用のファイル選択
        del_target = st.selectbox("🗑️ 削除するファイルを選択", ["--- 選択してください ---"] + history_df['filename'].tolist())
        if del_target != "--- 選択してください ---":
            if st.button(f"🚨 {del_target} を削除する", type="secondary"):
                with st.spinner(f"{del_target} を削除中..."):
                    if db_manager.delete_unified_data(del_target):
                        st.success(f"✅ {del_target} を削除しました。")
                        clear_app_cache()
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("削除に失敗しました。")

        st.dataframe(
            history_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "filename": "ファイル名",
                "source_type": "ソース",
                "row_count": st.column_config.NumberColumn("件数", format="%d"),
                "uploaded_at": "アップロード日時"
            }
        )

# --- 5. システム管理タブ ---
with tab_settings:
    st.subheader("⚙️ システム管理")
    
    with st.expander("🛠️ メンテナンスツール", expanded=True):
        st.warning("システムが不安定な場合や、プログラムの更新を反映させたい場合に実行してください。")
        if st.button("🔄 システムキャッシュを完全にクリアする", type="primary", use_container_width=True):
            clear_app_cache()
            st.success("キャッシュをクリアしました。再読み込みしています...")
            time.sleep(1)
            st.rerun()
    
    st.divider()
    
    st.session_state.gemini_api_key = st.text_input("Gemini API Key", value=gemini_api_key, type="password")
    st.session_state.project_id = st.text_input("GCP Project ID", value=project_id)
    
    if st.button("💾 基本設定を保存"):
        # .env ファイルに保存する
        with open(".env", "w", encoding="utf-8") as f:
            f.write(f"GEMINI_API_KEY={st.session_state.gemini_api_key}\n")
            f.write(f"GOOGLE_CLOUD_PROJECT={st.session_state.project_id}\n")
        st.success("✅ 設定を .env ファイルに保存しました。")
        time.sleep(1)
        st.rerun()

    st.divider()
    st.subheader("🗺️ 3. マッピング・ルール定義 (表内で自動重複排除)")
    st.info("プラットフォーム別のタブで、各項目のマッピングを行います。選択済みのヘッダーはリストから自動的に消えます。")

    # コールバック関数の定義
    def on_mapping_change(pk, row_idx, widget_key):
        new_val = st.session_state[widget_key]
        st.session_state.mappings_df.at[row_idx, pk] = new_val

    p_tabs = st.tabs(platforms_df['name'].tolist() + ["➕ 構成管理", "📋 全体俯瞰"])

    # 各プラットフォーム
    for i, (_, p) in enumerate(platforms_df.iterrows()):
        with p_tabs[i]:
            pkey = p['key']
            pname = p['name']
            candidates = sorted(discovered_headers_df[discovered_headers_df['platform_key'] == pkey]['header_name'].unique().tolist())
            
            # ヘッダー
            c1, c2 = st.columns([2, 3])
            c1.markdown("**【 統合カラム名 】**")
            c2.markdown(f"**【 {pname} のヘッダー 】**")
            
            used = set(current_mappings[pkey].replace('', None).dropna().tolist())
            updated = False
            for idx, row in current_mappings.iterrows():
                u_name = row['unified_name']
                curr_v = str(row[pkey]) if pd.notna(row[pkey]) else ""
                options = [""] + sorted([h for h in candidates if h not in (used - {curr_v}) or h == curr_v])
                if curr_v and curr_v not in options: options.append(curr_v)
                
                r1, r2 = st.columns([2, 3]); r1.write(f"🏷️ **{u_name}**")
                w_key = f"sel_v5_{pkey}_{u_name}" # インデックスではなく項目名をキーに使用
                r2.selectbox(
                    f"sel_{pkey}_{u_name}", 
                    options=options, 
                    index=options.index(curr_v) if curr_v in options else 0, 
                    key=w_key, 
                    label_visibility="collapsed",
                    on_change=on_mapping_change,
                    args=(pkey, idx, w_key)
                )
                st.markdown('<div style="margin-top:-15px; border-bottom:1px solid #eee;"></div>', unsafe_allow_html=True)
            
            if st.button(f"💾 {pname} の設定を保存", key=f"save_{pkey}"):
                with st.spinner("保存中..."):
                    db_manager.save_unified_columns_batch(st.session_state.mappings_df)
                    # セッション状態もクリア
                    if "mappings_df" in st.session_state: del st.session_state.mappings_df
                    clear_app_cache()
                    st.success("保存完了")
                    st.rerun()

    with p_tabs[len(platforms_df)]:
        st.markdown("#### ➕ 統合項目の構成管理")
        
        c1, c2 = st.columns([1, 1])
        if c1.button("🔄 デフォルト項目を同期", help="定義ファイル(database_bq.py)にある最新のデフォルト項目を現在の設定に追加します"):
            default_df = pd.DataFrame(db_manager.DEFAULT_MAPPINGS)
            missing = default_df[~default_df['unified_name'].isin(master_cols_df['unified_name'])]
            if not missing.empty:
                new_master = pd.concat([master_cols_df, missing[['unified_name','is_date','is_numeric']]]).drop_duplicates()
                db_manager.save_master_columns(new_master)
                # マッピング表も更新
                combined_map = new_master.merge(current_mappings.drop(columns=['is_date','is_numeric']), on='unified_name', how='left')
                # String型の列のみ空文字で埋める（Boolean型のエラー回避）
                for c in combined_map.columns:
                    if combined_map[c].dtype == object: combined_map[c] = combined_map[c].fillna("")
                
                for _, m_row in missing.iterrows():
                    idx = combined_map[combined_map['unified_name'] == m_row['unified_name']].index
                    for col in combined_map.columns:
                        if col in m_row and m_row[col]: combined_map.loc[idx, col] = m_row[col]
                
                db_manager.save_unified_columns_batch(combined_map)
                # セッション状態をクリア
                if "mappings_df" in st.session_state: del st.session_state.mappings_df
                clear_app_cache(); st.success(f"{len(missing)} 個の項目を追加しました"); st.rerun()
            else:
                st.info("すべてのデフォルト項目は既に登録済みです。")

        master_edit = st.data_editor(
            master_cols_df, 
            num_rows="dynamic", 
            use_container_width=True, 
            key="master_ed_v7",
            column_config={
                "sort_order": st.column_config.NumberColumn("表示順", help="小さい数字ほど上に表示されます", format="%d"),
                "unified_name": st.column_config.TextColumn("統合項目名", required=True),
                "is_date": st.column_config.CheckboxColumn("日付"),
                "is_numeric": st.column_config.CheckboxColumn("数値")
            }
        )
        if st.button("💾 統合項目の構成を保存"):
            # 入力された数値でソートして保存
            db_manager.save_master_columns(master_edit.sort_values("sort_order"))
            new_map = master_edit[['unified_name','is_date','is_numeric']].merge(current_mappings.drop(columns=['is_date','is_numeric']), on='unified_name', how='left')
            # String型の列のみ空文字で埋める
            for c in new_map.columns:
                if new_map[c].dtype == object: new_map[c] = new_map[c].fillna("")
            
            db_manager.save_unified_columns_batch(new_map)
            # セッション状態を破棄して再ロードを促す
            if "mappings_df" in st.session_state: del st.session_state.mappings_df
            clear_app_cache(); st.success("保存完了"); st.rerun()

        st.divider()
        st.markdown("#### 📱 2. プラットフォームの表示順・構成管理")
        st.caption("「表示順」の数値を書き換えて保存することで、タブや列の並び順を変更できます。")
        
        platforms_edit = st.data_editor(
            platforms_df, 
            num_rows="dynamic", 
            use_container_width=True, 
            key="platforms_ed_v2",
            column_config={
                "sort_order": st.column_config.NumberColumn("表示順", help="小さい数字ほど左（上）に表示されます", format="%d"),
                "key": st.column_config.TextColumn("内部キー (xxx_col)", required=True),
                "name": st.column_config.TextColumn("表示名", required=True)
            }
        )
        if st.button("💾 プラットフォーム構成を保存"):
            # 入力された数値でソートして保存
            sorted_platforms = platforms_edit.sort_values("sort_order")
            db_manager.save_platforms(sorted_platforms)
            
            # マッピングテーブルの列構成も同期させる
            new_p_keys = sorted_platforms['key'].tolist()
            updated_map = current_mappings.copy()
            # 不要な列を削除
            for col in [c for c in updated_map.columns if c.endswith('_col') and c not in new_p_keys]:
                updated_map = updated_map.drop(columns=[col])
            # 新しい列を追加
            for col in new_p_keys:
                if col not in updated_map.columns: updated_map[col] = ""
            
            db_manager.save_unified_columns_batch(updated_map)
            # セッション状態を破棄
            if "mappings_df" in st.session_state: del st.session_state.mappings_df
            clear_app_cache(); st.success("プラットフォーム構成を保存しました"); st.rerun()

    # 全体俯瞰
    with p_tabs[len(platforms_df)+1]:
        st.dataframe(current_mappings, use_container_width=True)

    st.divider()
    st.subheader("🔍 4. 検出済みヘッダーの管理（マッピング辞書）")
    st.caption("各プラットフォームのファイルから検知された項目名のリストです。ここに登録された項目が、上のマッピング表の選択肢に現れます。")
    
    # --- 新設：ヘッダー検知ツール ---
    with st.expander("📂 ファイルからヘッダーを一括読み込み（事前登録ツール）", expanded=False):
        st.info("ファイルを読み込ませることで、売上データの登録を行わずに、マッピング用の項目名だけを抽出・保存できます。")
        
        c1, c2 = st.columns([2, 1])
        # GCS上のファイルまたは直接アップロード
        scan_target = c1.selectbox("スキャン対象ファイルを選択", ["--- 直接アップロード ---"] + [f["name"] for f in db_manager.list_gcs_files() if not f["name"].startswith("up_data_")])
        new_file = None
        if scan_target == "--- 直接アップロード ---":
            new_file = c1.file_uploader("辞書構築用ファイルをアップロード", type=["csv", "tsv", "txt", "txt.gz"], key="dict_up")
        
        target_platform = c2.selectbox("プラットフォーム", platforms_df['name'].tolist(), help="検知した項目をどのプラットフォームに紐付けるか選択してください")
        
        if st.button("🚀 ヘッダーを抽出して辞書に記録する", use_container_width=True):
            with st.spinner("解析中..."):
                file_io = None
                fname = ""
                if new_file: 
                    file_io = new_file
                    fname = new_file.name
                elif scan_target != "--- 直接アップロード ---":
                    file_io = db_manager.get_gcs_blob_io(scan_target)
                    fname = scan_target
                
                if file_io:
                    formatter = DataFormatter(mappings, exchange_rates=fetch_exchange_rates(project_id))
                    # ユーザーが選択したプラットフォームを解析のヒントとして渡す
                    source_hint = "UNKNOWN"
                    match_row = platforms_df[platforms_df['name'] == target_platform]
                    if not match_row.empty:
                        # 内部キー(nextone_col等)から種別(NEXTONE等)を推測
                        pkey = match_row.iloc[0]['key'].upper()
                        if "NEXTONE" in pkey: source_hint = "NEXTONE"
                        elif "ORCHARD" in pkey: source_hint = "ORCHARD"
                        elif "APPLE" in pkey: source_hint = "ITUNES"
                        elif "YOUTUBE" in pkey: source_hint = "YOUTUBE"

                    # ヘッダーのみを抽出するための簡易読み込み
                    _, _, raw_cols = formatter.format_file(file_io, fname, source_type=source_hint, platforms_df=platforms_df)
                    if raw_cols:
                        pkey = platforms_df[platforms_df['name'] == target_platform].iloc[0]['key']
                        h_df = pd.DataFrame([{
                            "platform_key": pkey, 
                            "header_name": str(h).strip(),
                            "source_file": fname,
                            "detected_at": datetime.datetime.now(datetime.timezone.utc)
                        } for h in raw_cols])
                        db_manager.save_discovered_headers_batch(h_df)
                        st.success(f"✅ {len(raw_cols)} 件の項目を {target_platform} の辞書に追加しました。")
                        clear_app_cache(); st.rerun()
                    else:
                        st.error("ヘッダーの抽出に失敗しました。ファイル形式を確認してください。")

    # --- 既存の辞書データ編集 ---
    with st.expander("📝 辞書データを表示・編集する"):
        # 必要な列が df に存在するか確認しながらマージ
        h_master = discovered_headers_df.copy()
        if not h_master.empty:
            h_master = h_master.merge(platforms_df[['key','name']], left_on='platform_key', right_on='key', how='left')
        
        # 表示項目の整理 (カラムが存在しない場合のフォールバック)
        cols_to_show = ['platform_key','name','source_file','header_name','detected_at']
        for c in cols_to_show:
            if c not in h_master.columns: h_master[c] = ""
            
        h_edit = st.data_editor(
            h_master[cols_to_show], 
            num_rows="dynamic", 
            use_container_width=True, 
            column_config={
                "platform_key": st.column_config.SelectboxColumn("キー", options=platforms_df['key'].tolist(), required=True),
                "name": st.column_config.TextColumn("プラットフォーム", disabled=True),
                "source_file": st.column_config.TextColumn("検知元ファイル", required=True),
                "header_name": st.column_config.TextColumn("ヘッダー項目名", required=True),
                "detected_at": st.column_config.DatetimeColumn("検知日時", disabled=True, format="YYYY/MM/DD HH:mm")
            },
            key="header_dictionary_v6"
        )
        if st.button("💾 辞書を保存", key="save_header_v6"):
            # 保存時は必須カラムのみ抽出
            save_h_df = h_edit[['platform_key', 'header_name', 'source_file']]
            db_manager.save_discovered_headers_batch(save_h_df, overwrite=True)
            clear_app_cache(); st.success("辞書を更新しました。"); st.rerun()

    st.divider()
    if st.button("🔄 システム全体のキャッシュをクリア"):
        clear_app_cache(); st.rerun()
