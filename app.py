import streamlit as st
import pandas as pd
from aggregator.processor import SalesAggregator
from aggregator.database_bq import DatabaseManager
import io
import os
import time

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
</style>
""", unsafe_allow_html=True)

# --- Database & Processor Logic ---
@st.cache_resource
def get_db(project_id):
    dataset_id = "sales_aggregator_dataset"
    return DatabaseManager(project_id=project_id, dataset_id=dataset_id)

@st.cache_data(ttl=300)
def fetch_raw_data(project_id):
    return get_db(project_id).get_raw_data()

@st.cache_data(ttl=600)
def fetch_mappings(project_id):
    return get_db(project_id).get_unified_columns()

@st.cache_data(ttl=600)
def fetch_rules(project_id):
    return get_db(project_id).get_parsing_rules()

def clear_app_cache():
    st.cache_data.clear()

# --- App Layout ---
default_project_id = os.getenv('GOOGLE_CLOUD_PROJECT', st.session_state.get('project_id', ''))
st.title("📊 売上データ管理システム")
st.markdown("---")

with st.expander("⚙️ システム設定", expanded=not default_project_id):
    project_id = st.text_input("GCP Project ID", value=default_project_id)
    if project_id:
        st.session_state['project_id'] = project_id
        db_manager = get_db(project_id)
        processor = SalesAggregator()
    else:
        st.stop()

tab_view, tab_flexible, tab_upload, tab_settings = st.tabs(["📋 売上一覧", "📊 自由集計", "📥 データの追加", "⚙️ 設定"])

raw_df = fetch_raw_data(project_id)
mappings = fetch_mappings(project_id)
unified_df = pd.DataFrame()
if not raw_df.empty and not mappings.empty:
    unified_df = processor.unify_raw_records(raw_df, mappings)

with tab_view:
    if raw_df.empty: st.info("データがありません。")
    elif unified_df.empty: st.warning("マッピング設定を確認してください。")
    else:
        c1, c2 = st.columns(2)
        month_col = next((c for c in unified_df.columns if not mappings.empty and mappings[mappings['unified_name']==c]['is_date'].any()), None)
        month_list = ["すべて"] + sorted(unified_df[month_col].dropna().unique().tolist(), reverse=True) if month_col else ["すべて"]
        sel_m = c1.selectbox("📅 対象月", month_list)
        sel_s = c2.selectbox("🌍 ソース", ["すべて"] + sorted(unified_df['SOURCE'].unique().tolist()), key="source_sel")
        
        filtered = unified_df.copy()
        if sel_m != "すべて": filtered = filtered[filtered[month_col] == sel_m]
        if sel_s != "すべて": filtered = filtered[filtered['SOURCE'] == sel_s]
        st.dataframe(filtered, use_container_width=True, hide_index=True)

with tab_flexible:
    if unified_df.empty: st.info("集計可能なデータがありません。")
    else:
        attr_cols = [m['unified_name'] for _, m in mappings.iterrows() if not m['is_numeric'] and not m['is_date']]
        num_cols = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
        cc1, cc2, cc3 = st.columns(3)
        axis_options = attr_cols + (['SOURCE'] if 'SOURCE' in unified_df.columns else [])
        row_axis = cc1.selectbox("タテ軸", axis_options, index=0 if axis_options else None)
        col_axis = cc2.selectbox("ヨコ軸", ["(なし)"] + axis_options, index=0)
        val_cols = cc3.multiselect("値", num_cols, default=num_cols[:1] if num_cols else [])
        if val_cols and row_axis:
            try:
                p_cols = col_axis if col_axis != "(なし)" else None
                pivot_res = unified_df.pivot_table(index=row_axis, columns=p_cols, values=val_cols, aggfunc='sum', margins=True, margins_name="合計")
                st.dataframe(pivot_res.style.format("{:,.0f}"), use_container_width=True)
            except Exception as e: st.error(f"集計エラー: {e}")

# --- 3. データの追加 (標準アップローダー化) ---
with tab_upload:
    st.subheader("📥 データのアップロード")
    st.caption("アップロードが完了すると、自動的に登録（重複時は警告）が開始されます。")

    # 標準アップローダー (1GBまで対応)
    uploaded_file = st.file_uploader("CSV/TSV/TXT ファイルを選択してください（1GBまで対応）", type=["csv", "tsv", "txt"], key="main_uploader")

    if uploaded_file:
        fn = uploaded_file.name
        st.markdown(f"📦 **ファイル選択済み:** `{fn}` ({uploaded_file.size/1024/1024:.1f} MB)")
        
        # 重複チェック
        is_existing = not raw_df.empty and fn in raw_df['filename'].unique()
        
        auto_start = False
        # すでにこのセッションで処理済みの場合は自動実行しない
        proc_flag = f"done_{fn}_{uploaded_file.size}"
        if proc_flag in st.session_state:
            st.success(f"✅ `{fn}` は登録完了しました。")
            if st.button("🔄 別のファイルを登録する"):
                del st.session_state[proc_flag]
                st.rerun()
        else:
            if is_existing:
                st.warning(f"⚠️ `{fn}` は既に登録されています。上書きしますか？")
                if st.button("🔥 内容を上書きして登録する", type="primary"):
                    auto_start = True
            else:
                # 新規ファイルなら自動！
                auto_start = True
                st.info("🚀 データベースへの登録を自動で開始します。そのままお待ちください...")

            if auto_start:
                with st.status(f"⚡ {fn} を処理中...") as status:
                    try:
                        status.update(label=f"🔍 データの構造を解析しています...")
                        rules = fetch_rules(project_id)
                        
                        # メモリ上から直接パース
                        df = processor.parse_raw_only(uploaded_file, rules=rules)
                        
                        if df is not None:
                            row_count = len(df)
                            status.update(label=f"📊 {row_count:,} 件のデータを検出しました。保存中...")
                            
                            s_type = processor.detect_source(fn)
                            db_manager.save_raw_data(df, fn, s_type, overwrite=True)
                            
                            status.update(label=f"✅ {fn} ({row_count:,}件) の登録が完了しました！", state="complete")
                            st.session_state[proc_flag] = True
                            st.toast(f"登録完了: {fn}", icon="✅")
                            clear_app_cache()
                            time.sleep(2)
                            st.rerun()
                        else: st.error("ファイルの解析に失敗しました。解析ルールを確認してください。")
                    except Exception as e: st.error(f"登録エラー: {e}")

    st.divider()
    st.markdown("#### 📋 取り込み済み履歴")
    if not raw_df.empty:
        history = raw_df.groupby('filename').agg({'row_index':'count', 'source_type':'first'}).reset_index()
        for i, row in history.iterrows():
            with st.container(border=True):
                ca, cb, cc = st.columns([4, 1, 1])
                ca.write(f"📄 **{row['filename']}** ({row['source_type']})")
                cb.write(f"{row['row_index']:,} 件")
                if cc.button("🗑️", key=f"hist_del_{i}"):
                    if db_manager.delete_raw_data(row['filename']):
                        clear_app_cache()
                        st.rerun()
    else: st.info("履歴はありません。")

with tab_settings:
    st.subheader("⚙️ 管理")
    if st.button("🔥 データベース全体の初期化", type="primary"):
        db_manager.reset_dataset()
        clear_app_cache()
        st.rerun()
