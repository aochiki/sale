import streamlit as st
import pandas as pd
from aggregator.processor import SalesAggregator
from aggregator.database_bq import DatabaseManager
import io
import datetime
import logging
import os
import json

# --- Page Config ---
st.set_page_config(
    page_title="売上データ統合システム (RAW Dynamic)", 
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

@st.cache_data(ttl=600)
def fetch_headers(project_id, source_type):
    return get_db(project_id).get_unique_headers(source_type)

def clear_app_cache():
    st.cache_data.clear()

# --- App Layout ---
default_project_id = os.getenv('GOOGLE_CLOUD_PROJECT', st.session_state.get('project_id', ''))
st.title("📊 売上データ管理システム")
st.caption("RAWデータ保存 & 表示時動的統合モデル")
st.markdown("---")

with st.expander("⚙️ システム設定", expanded=not default_project_id):
    project_id = st.text_input("GCP Project ID", value=default_project_id)
    if project_id:
        st.session_state['project_id'] = project_id
        db_manager = get_db(project_id)
        processor = SalesAggregator()
    else:
        st.stop()

tab_view, tab_upload, tab_settings = st.tabs(["📋 売上データ閲覧", "📥 RAWデータ追加", "⚙️ システム管理"])

# --- 1. 閲覧タブ (動的統合) ---
with tab_view:
    raw_df = fetch_raw_data(project_id)
    mappings = fetch_mappings(project_id)
    
    if raw_df.empty:
        st.info("データがありません。RAWデータをアップロードしてください。")
    else:
        with st.status("🔄 データを動的に統合中...", expanded=False):
            unified_df = processor.unify_raw_records(raw_df, mappings)
        
        if unified_df.empty:
            st.warning("マッピング設定に基づいて統合されたデータがありません。設定を確認してください。")
            st.dataframe(raw_df.head())
        else:
            # フィルタリング
            c1, c2, c3 = st.columns(3)
            with c1:
                # 日付項目の特定 (マッピングから is_date=True のものを探す)
                month_col = next((c for c in unified_df.columns if not mappings.empty and mappings[mappings['unified_name']==c]['is_date'].any()), None)
                month_list = ["すべて"] + sorted(unified_df[month_col].dropna().unique().tolist(), reverse=True) if month_col else ["すべて"]
                sel_m = st.selectbox("📅 対象月", month_list)
            with c2:
                sel_s = st.selectbox("🌍 ソース", ["すべて"] + sorted(unified_df['SOURCE'].unique().tolist()))
            
            filtered = unified_df.copy()
            if sel_m != "すべて": filtered = filtered[filtered[month_col] == sel_m]
            if sel_s != "すべて": filtered = filtered[filtered['SOURCE'] == sel_s]
            
            st.dataframe(filtered, use_container_width=True, hide_index=True)

# --- 2. アップロードタブ (RAW保存) ---
with tab_upload:
    st.subheader("📥 RAWデータの取り込み")
    # すでにアップロード済みのファイル名リストを取得
    all_raw = db_manager.get_raw_data()
    existing_filenames = set(all_raw['filename'].unique()) if not all_raw.empty else set()

    files = st.file_uploader("ファイルをドロップ", accept_multiple_files=True)
    
    # 重複チェックの警告を表示
    duplicate_files = [f.name for f in files if f.name in existing_filenames] if files else []
    if duplicate_files:
        st.warning(f"⚠️ 以下の {len(duplicate_files)} 件のファイルは既に登録されています。\n" + 
                   ", ".join(duplicate_files[:5]) + ("..." if len(duplicate_files) > 5 else ""))
        over = st.checkbox("既存ファイルを上書きして保存", value=False)
    else:
        over = st.checkbox("既存ファイルを上書き", value=True)
    
    if files and st.button("🚀 データベースに保存", type="primary"):
        rules = fetch_rules(project_id)
        success_details = []
        error_count = 0
        skip_count = 0
        with st.status("データ処理中...") as status:
            for f in files:
                # 重複かつ上書きチェックなしの場合はスキップ
                if f.name in existing_filenames and not over:
                    st.info(f"スキップ (既に存在します): {f.name}")
                    skip_count += 1
                    continue
                    
                try:
                    df = processor.parse_raw_only(f, rules=rules)
                    if df is not None:
                        s_type = processor.detect_source(f.name)
                        row_count = db_manager.save_raw_data(df, f.name, s_type, overwrite=over)
                        success_details.append({"file": f.name, "rows": row_count})
                    else:
                        st.error(f"解析失敗: {f.name}")
                        error_count += 1
                except Exception as e:
                    st.error(f"エラー ({f.name}): {e}")
                    error_count += 1
            
            label = f"✅ {len(success_details)} 件のファイルを処理しました"
            if error_count > 0: label += f" / ❌ {error_count} 件失敗"
            if skip_count > 0: label += f" / ⏭️ {skip_count} 件スキップ"
            status.update(label=label, state="complete" if error_count == 0 else "error")
        
        if success_details:
            st.success("🎉 データの取り込みが完了しました！")
            for item in success_details:
                st.write(f"🔹 **{item['file']}**: {item['rows']:,} 件のデータを取り込み完了")
            
            st.toast(f"✅ {len(success_details)} 件のファイルを保存しました。", icon="🚀")
            clear_app_cache()
            if st.button("🔄 画面を更新してデータを確認する"):
                st.rerun()

# --- 3. 管理タブ (リセット & マッピング) ---
with tab_settings:
    st.subheader("🔗 統合マッピング定義")
    st.info("RAWデータに含まれるヘッダーをドロップダウンから選択して、統合項目を定義します。")
    
    # RAWデータからヘッダーを取得
    h_orchard = ["(未設定)"] + fetch_headers(project_id, "ORCHARD")
    h_nextone = ["(未設定)"] + fetch_headers(project_id, "NEXTONE")
    h_itunes = ["(未設定)"] + fetch_headers(project_id, "ITUNES")
    
    if 'editing_col' not in st.session_state: st.session_state.editing_col = None
    cur_mappings = fetch_mappings(project_id)
    edit_item = cur_mappings[cur_mappings['unified_name'] == st.session_state.editing_col].iloc[0] if st.session_state.editing_col else None

    with st.form("mapping_form"):
        u_name = st.text_input("統合項目名", value=st.session_state.editing_col if st.session_state.editing_col else "")
        c2, c3, c4 = st.columns(3)
        # ドロップダウン化
        idx_o = h_orchard.index(edit_item['orchard_col']) if edit_item is not None and edit_item['orchard_col'] in h_orchard else 0
        idx_n = h_nextone.index(edit_item['nextone_col']) if edit_item is not None and edit_item['nextone_col'] in h_nextone else 0
        idx_i = h_itunes.index(edit_item['itunes_col']) if edit_item is not None and edit_item['itunes_col'] in h_itunes else 0
        
        o_col = c2.selectbox("Orchard 列名", h_orchard, index=idx_o)
        n_col = c3.selectbox("NexTone 列名", h_nextone, index=idx_n)
        i_col = c4.selectbox("iTunes 列名", h_itunes, index=idx_i)
        
        is_d = st.checkbox("日付として処理 (YYYY-MM-01に統一)", value=bool(edit_item['is_date']) if edit_item is not None else False)
        is_n = st.checkbox("数値として処理", value=bool(edit_item['is_numeric']) if edit_item is not None else False)
        
        if st.form_submit_button("💾 保存"):
            if u_name:
                try:
                    db_manager.save_unified_column(u_name, 
                        o_col if o_col != "(未設定)" else "",
                        n_col if n_col != "(未設定)" else "",
                        i_col if i_col != "(未設定)" else "",
                        is_d, is_n)
                    st.session_state.editing_col = None
                    clear_app_cache()
                    st.toast(f"マッピングを保存しました: {u_name}", icon="✅")
                    import time
                    time.sleep(2)
                    st.rerun()
                except Exception as e:
                    logging.error(f"Mapping save error: {e}")
                    st.error(f"マッピング保存エラー: {e}")

    # マッピング一覧
    if not cur_mappings.empty:
        for i, m in cur_mappings.iterrows():
            with st.container(border=True):
                col_t, col_b = st.columns([4, 1])
                col_t.write(f"📁 **{m['unified_name']}** (O: {m['orchard_col']}, N: {m['nextone_col']}, I: {m['itunes_col']})")
                if col_b.button("📝 編集", key=f"edit_{i}"):
                    st.session_state.editing_col = m['unified_name']
                    st.rerun()

    st.divider()
    st.subheader("📄 解析ルールの設定")
    with st.form("rule_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([3, 1, 1])
        pat = c1.text_input("ファイル名パターン")
        hr = c2.number_input("ヘッダー行目", min_value=1, value=1)
        if c3.form_submit_button("➕ ルール追加"):
            if pat:
                try:
                    logging.info(f"Attempting to add parsing rule: {pat}")
                    # ユニーク性を担保するため既存があれば削除
                    db_manager.delete_parsing_rule(pat)
                    db_manager.save_parsing_rule(pat, hr - 1)
                    clear_app_cache()
                    st.toast(f"追加完了: {pat}", icon="➕")
                    import time
                    time.sleep(2)
                    st.rerun()
                except Exception as e:
                    logging.error(f"Rule addition error: {e}")
                    st.error(f"ルール追加エラー: {e}")

    # 解析ルールの一覧表示
    cur_rules = fetch_rules(project_id)
    if not cur_rules.empty:
        st.write("📋 現在登録されている解析ルール")
        for idx, row in cur_rules.iterrows():
            with st.container(border=True):
                r1, r2, r3 = st.columns([3, 1, 1])
                r1.write(f"パターン: `{row['file_pattern']}`")
                r2.write(f"ヘッダー: {row['header_row'] + 1}行目")
                if r3.button("🗑️ 削除", key=f"del_rule_{idx}"):
                    db_manager.delete_parsing_rule(row['file_pattern'])
                    clear_app_cache()
                    st.toast(f"削除しました: {row['file_pattern']}", icon="🗑️")
                    import time
                    time.sleep(2)
                    st.rerun()

    st.divider()
    st.subheader("⚠️ データベースの管理")
    with st.expander("💣 危険な操作"):
        st.warning("この操作は取り消せません。すべてのデータと設定が消去されます。")
        if st.button("🔥 データベースを完全にリセットする", type="primary"):
            db_manager.reset_dataset()
            clear_app_cache()
            st.success("リセット完了。ページを更新してください。")
            st.rerun()
