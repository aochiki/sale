import streamlit as st
import pandas as pd
from aggregator.processor import SalesAggregator
from aggregator.database_bq import DatabaseManager
import os
import time
import uuid

# --- Page Config ---
st.set_page_config(page_title="売上データ管理 (BigQuery版)", page_icon="📊", layout="wide")

# --- Premium Style ---
st.markdown("""
<style>
    .stApp { background-color: #f8f9fa; }
    .stTabs [data-baseweb="tab-list"] { gap: 20px; }
    .stTabs [data-baseweb="tab"] { height: 50px; font-weight: 600; }
    h1 { color: #1e3a8a; }
    .stDataFrame { border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); }
</style>
""", unsafe_allow_html=True)

# --- Logic ---
@st.cache_resource
def get_db(project_id):
    return DatabaseManager(project_id=project_id, dataset_id="sales_aggregator_dataset")

default_project_id = os.getenv('GOOGLE_CLOUD_PROJECT', st.session_state.get('project_id', ''))
project_id = st.session_state.get('project_id', default_project_id)

if not project_id:
    st.title("📊 売上データ管理システム")
    st.warning("GCPプロジェクトIDを設定してください。")
    st.stop()

db_manager = get_db(project_id)
processor = SalesAggregator()

# --- Custom Component Initialization ---
import streamlit.components.v1 as components
parent_dir = os.path.dirname(os.path.abspath(__file__))
# aggregator/dropzone_component を読み込む
build_path = os.path.join(parent_dir, "aggregator", "dropzone_component")
_dropzone = components.declare_component("my_dropzone", path=build_path)

# --- Data Loading ---
@st.cache_data(ttl=300)
def load_all_data(pid):
    raw = db_manager.get_raw_data(limit=2000)
    maps = db_manager.get_unified_columns()
    rules = db_manager.get_parsing_rules()
    history = db_manager.get_file_history()
    return raw, maps, rules, history

raw_df, mappings, rules, all_history = load_all_data(project_id)
unified_df = pd.DataFrame()
if not raw_df.empty and not mappings.empty:
    unified_df = processor.unify_raw_records(raw_df, mappings)

# --- UI Layout ---
st.title("📊 売上データ管理 (BigQuery版)")

tab_upload, tab_view, tab_rules, tab_mapping, tab_settings = st.tabs([
    "📥 データの追加", "📋 売上一覧", "🛠️ 解析ルール設定", "🔗 項目マッピング", "⚙️ 設定"
])

# 1. データの追加 (デフォルト)
with tab_upload:
    st.subheader("📥 大容量データのアップロード")

    # セッション間で一貫したGCSパスを維持
    if '_up_key' not in st.session_state:
        st.session_state._up_key = f"up_{uuid.uuid4().hex[:8]}"
    temp_name = f"tmp_load/{st.session_state._up_key}_latest.csv"

    # カスタムコンポーネントの表示
    signed_url = db_manager.get_gcs_signed_url(temp_name)
    
    # コンポーネントからの戻り値を受け取る (リロードなしで更新される)
    result = _dropzone(signed_url=signed_url, key="dropzone_v1")

    # リザルト（JavaScriptからの通知）に基づいた処理
    target_fn = ""
    is_uploaded = False
    
    if result:
        target_fn = result.get("filename", "")
        is_uploaded = result.get("status") == "done"

    if target_fn:
        st.info(f"📁 検出されたファイル名: **{target_fn}**")
        fn_clean = target_fn.strip()
        if db_manager.check_file_exists(fn_clean):
            st.warning(f"⚠️ 注意: '{fn_clean}' は既にBigQueryに登録されています。")
            allow_overwrite = st.checkbox("既存データを上書きして再登録する", value=False)
        else:
            st.success(f"✅ '{fn_clean}' は新規登録可能です。")
            allow_overwrite = True
    else:
        st.info("上の青い枠内にファイルをドロップしてください。")

    # 3. BigQueryへの登録実行
    can_submit = target_fn and is_uploaded
    if target_fn and is_uploaded and db_manager.check_file_exists(target_fn.strip()):
        can_submit = allow_overwrite
    
    if st.button("🚀 BigQueryへの登録を開始する", type="primary", use_container_width=True, disabled=not can_submit):
        with st.status("📦 BigQueryへデータをロード中...", expanded=True) as stat:
            try:
                import tempfile
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                    stat.write("📥 検証用データを取得...")
                    blob = db_manager.storage_client.bucket(db_manager.bucket_name).blob(temp_name)
                    blob.download_to_filename(tmp.name)
                    temp_local_path = tmp.name

                def update_progress(msg):
                    stat.update(label=msg)
                    stat.write(msg)

                db_manager.upload_large_file_via_gcs(
                    local_path=temp_local_path,
                    filename=target_fn,
                    source_type="AutoDetect",
                    overwrite=True, # 上記 checkbox で判定済みのため
                    progress_callback=update_progress
                )
                
                db_manager.delete_gcs_file(temp_name)
                stat.update(label="✅ 登録が完了しました！", state="complete")
                
                # 完了後にリフレッシュ
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"登録エラー: {e}")
            finally:
                if 'temp_local_path' in locals() and os.path.exists(temp_local_path):
                    os.remove(temp_local_path)

    st.divider()
    if not all_history.empty:
        st.write("#### 📋 取り込み済みファイル履歴")
        for _, h in all_history.iterrows():
            fn = h['filename']
            c1, c2, c3 = st.columns([5, 2, 1])
            c1.text(f"📄 {fn}")
            c2.caption(f"📅 {h['uploaded_at']}")
            if c3.button("🗑️", key=f"del_{fn}"):
                db_manager.delete_raw_data(fn)
                st.cache_data.clear()
                st.rerun()

# 2. 売上一覧
with tab_view:
    st.subheader("📋 統合売上データ (プレビュー)")
    if unified_df.empty: 
        st.info("表示できるデータがありません。「データの追加」タブからアップロードしてください。")
    else:
        total_rows = len(unified_df)
        st.caption(f"💡 現在、最新の {min(total_rows, 100)} 件を表示しています。")
        st.dataframe(unified_df.head(100), use_container_width=True, hide_index=True)
        st.download_button("📥 データをCSVとしてダウンロード", unified_df.to_csv(index=False), "unified.csv", "text/csv")

# 3. 他のタブ
with tab_rules:
    st.subheader("🛠️ 解析ルール設定")
    with st.form("add_rule"):
        col1, col2, col3 = st.columns([3, 2, 1])
        r_pattern = col1.text_input("ファイル名パターン (例: Orchard*)")
        h_row = col2.number_input("ヘッダー行", min_value=0, step=1)
        if st.form_submit_button("追加"):
            if r_pattern:
                db_manager.save_parsing_rule(r_pattern, h_row)
                st.cache_data.clear()
                st.rerun()
    if not rules.empty:
        for _, r in rules.iterrows():
            c1, c2, c3 = st.columns([3, 2, 1])
            c1.text(r['file_pattern'])
            c2.text(f"行: {r['header_row']}")
            if c3.button("🗑️", key=f"del_rule_{r['file_pattern']}"):
                db_manager.delete_parsing_rule(r['file_pattern'])
                st.cache_data.clear()
                st.rerun()

with tab_mapping:
    st.subheader("🔗 項目マッピング設定")
    if not mappings.empty:
        st.dataframe(mappings, use_container_width=True, hide_index=True)
    with st.expander("➕ マッピングの追加/削除"):
        with st.form("add_mapping"):
            c1, c2, c3 = st.columns(3)
            u_name = c1.text_input("統一項目名")
            is_date = c2.checkbox("日付")
            is_num = c3.checkbox("数値")
            c4, c5, c6 = st.columns(3)
            o_col = c4.text_input("Orchard列")
            n_col = n_col = c5.text_input("NexTone列")
            i_col = i_col = c6.text_input("iTunes列")
            if st.form_submit_button("保存"):
                if u_name:
                    db_manager.save_unified_column(u_name, o_col, n_col, i_col, is_date, is_num)
                    st.cache_data.clear()
                    st.rerun()
        if not mappings.empty:
            del_item = st.selectbox("削除選択", [""] + mappings['unified_name'].tolist())
            if st.button("🗑️ 選択した項目を削除") and del_item:
                db_manager.delete_unified_column(del_item)
                st.cache_data.clear()
                st.rerun()

with tab_settings:
    st.subheader("⚙️ 設定・初期化")
    if st.button("🔥 全データを完全に初期化する"):
        db_manager.reset_dataset()
        st.cache_data.clear()
        st.rerun()
