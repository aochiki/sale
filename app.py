import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from aggregator.processor import SalesAggregator
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

# --- Page Config ---
st.set_page_config(
    page_title="売上データ統合システム (AI & Auto-Upload)", 
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

def clear_app_cache():
    st.cache_data.clear()

st.title("📊 売上データ管理システム")
st.caption("Auto-Detect Upload & AI Aggregation")
st.markdown("---")

# --- Initial State & Config ---
load_dotenv()
if 'project_id' not in st.session_state:
    st.session_state.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', '').strip()
if 'gemini_api_key' not in st.session_state:
    st.session_state.gemini_api_key = os.getenv('GEMINI_API_KEY', '')

project_id = st.session_state.project_id
gemini_api_key = st.session_state.gemini_api_key

db_manager = None
processor = SalesAggregator()
rules = pd.DataFrame()
raw_df = pd.DataFrame()
mappings = pd.DataFrame()
unified_df = pd.DataFrame()

if project_id:
    db_manager = get_db(project_id)
    rules = fetch_rules(project_id)

tab_view, tab_flexible, tab_ai, tab_upload, tab_settings = st.tabs([
    "📋 売上データ閲覧", "📊 自由集計", "🤖 AI集計", "📥 RAWデータ追加", "⚙️ システム管理"
])

# --- 共通データの取得 ---
if project_id:
    db_manager = get_db(project_id)
    raw_df = fetch_raw_data(project_id)
    mappings = fetch_mappings(project_id)
    rules = fetch_rules(project_id)

    if not raw_df.empty and not mappings.empty:
        with st.status("🔄 データを動的に統合中...", expanded=False):
            unified_df = processor.unify_raw_records(raw_df, mappings)

# --- 1. 閲覧タブ ---
with tab_view:
    if not project_id:
        st.info("💡 「⚙️ システム管理」タブで GCP Project ID を設定してください。")
        st.stop()
    
    if raw_df.empty:
        st.info("データがありません。RAWデータをアップロードしてください。")
    elif unified_df.empty:
        st.warning("マッピング定義に基づいて統合されたデータがありません。")
    else:
        # 簡易フィルタ
        c1, c2 = st.columns(2)
        month_col = next((c for c in unified_df.columns if not mappings.empty and mappings[mappings['unified_name']==c]['is_date'].any()), None)
        month_list = ["すべて"] + sorted(unified_df[month_col].dropna().unique().tolist(), reverse=True) if month_col else ["すべて"]
        sel_m = c1.selectbox("📅 対象月", month_list)
        sel_s = c2.selectbox("🌍 ソース", ["すべて"] + sorted(unified_df['SOURCE'].unique().tolist()))
        
        filtered = unified_df.copy()
        if sel_m != "すべて": filtered = filtered[filtered[month_col] == sel_m]
        if sel_s != "すべて": filtered = filtered[filtered['SOURCE'] == sel_s]
        
        st.dataframe(filtered, use_container_width=True, hide_index=True)
        st.download_button("📥 ダウンロード", filtered.to_csv(index=False), f"unified_{datetime.datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

# --- 2. 自由集計タブ ---
with tab_flexible:
    if not project_id:
        st.info("💡 「⚙️ システム管理」タブで GCP Project ID を設定してください。")
        st.stop()
        
    if unified_df.empty:
        st.info("集計可能なデータがありません。")
    else:
        st.subheader("📊 ピボット集計")
        attr_cols = [m['unified_name'] for _, m in mappings.iterrows() if not m['is_numeric'] and not m['is_date']]
        num_cols = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
        
        c1, c2, c3 = st.columns(3)
        row_axis = c1.selectbox("縦軸", [None] + attr_cols + ['SOURCE'])
        col_axis = c2.selectbox("横軸", [None] + attr_cols + ['SOURCE'])
        val_axis = c3.selectbox("集計値", num_cols if num_cols else [None])
        
        if val_axis:
            try:
                pivot_df = unified_df.pivot_table(
                    index=row_axis, columns=col_axis, values=val_axis,
                    aggfunc='sum', margins=True, margins_name="合計"
                )
                st.dataframe(pivot_df.style.format("{:,.0f}"), use_container_width=True)
            except:
                st.info("集計できませんでした。")

# --- 3. AI集計タブ ---
with tab_ai:
    if not project_id:
        st.info("💡 「⚙️ システム管理」タブで GCP Project ID を設定してください。")
        st.stop()

    if unified_df.empty:
        st.info("集計可能なデータがありません。")
    else:
        st.subheader("🤖 AI集計")
        st.caption("「かりゆし58の曲ごとの売上を表示」など。")
        
        date_col = next((m['unified_name'] for _, m in mappings.iterrows() if m['is_date']), None)
        flex_df_ai = unified_df.copy()
        if date_col:
            months = sorted(flex_df_ai[date_col].dropna().unique().tolist())
            c1, c2 = st.columns(2)
            start_m_ai = c1.selectbox("🚩 開始月", months, index=0, key="ai_start")
            end_m_ai = c2.selectbox("🏁 終了月", months, index=len(months)-1, key="ai_end")
            flex_df_ai = flex_df_ai[(flex_df_ai[date_col] >= start_m_ai) & (flex_df_ai[date_col] <= end_m_ai)].copy()

        user_query = st.chat_input("AIに集計をお願いする...")
        if user_query:
            st.chat_message("user").write(user_query)
            with st.chat_message("assistant"):
                with st.spinner("AIが意図を解析中..."):
                    attr_cols_ai = [m['unified_name'] for _, m in mappings.iterrows() if not m['is_numeric'] and not m['is_date']]
                    num_cols_ai = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
                    all_cols = attr_cols_ai + (['SOURCE'] if 'SOURCE' in unified_df.columns else [])
                    
                    gemini_key = st.session_state.get('gemini_api_key')
                    if not gemini_key:
                        st.error("Gemini APIキーが設定されていません。")
                        st.stop()
                    
                    parsed = parse_natural_language_query(project_id, user_query, all_cols, num_cols_ai, api_key=gemini_key)
                
                if not parsed:
                    st.error("AIからの応答がありませんでした。")
                elif "error" in parsed:
                    st.error(f"AI解析エラー: {parsed['error']}")
                else:
                    with st.expander("🔍 AIの解析結果を確認"): st.json(parsed)
                    
                    try:
                        f_df = flex_df_ai.copy()
                        filters = parsed.get("filters", {})
                        if filters:
                            import unicodedata
                            for col, val in filters.items():
                                if col in f_df.columns:
                                    val_str = str(val)
                                    val_nfkc = unicodedata.normalize('NFKC', val_str)
                                    
                                    # Create a mask that checks both the original and normalized value
                                    # .str.contains uses minimal memory compared to creating a whole new String column
                                    import re
                                    escaped_val = re.escape(val_str)
                                    escaped_nfkc = re.escape(val_nfkc)
                                    pattern = f"{escaped_val}|{escaped_nfkc}"
                                    
                                    f_df = f_df[f_df[col].astype(str).str.contains(pattern, na=False, case=False, regex=True)]
                        
                        def clean(a):
                            if isinstance(a, list): return [str(i).strip() for i in a if i]
                            return str(a).strip() if a else None

                        r_axis = clean(parsed.get("row_axis"))
                        c_axis = clean(parsed.get("col_axis"))
                        v_axis = [str(v).strip() for v in parsed.get("value_axis", []) if v]
                        if not v_axis and num_cols_ai: v_axis = [num_cols_ai[0]]
                        
                        def check(cols, df):
                            if not cols: return []
                            c_list = cols if isinstance(cols, list) else [cols]
                            return [x for x in c_list if x not in df.columns]

                        missing = check(r_axis, f_df) + check(c_axis, f_df) + check(v_axis, f_df)
                        if missing:
                            st.warning(f"項目欠損: {', '.join(missing)}")
                        elif not v_axis:
                            st.warning("集計対象（数値）が見つかりません。")
                        else:
                            if not r_axis and not c_axis:
                                st.write("### 📋 合計")
                                st.dataframe(f_df[v_axis].sum().to_frame(name='合計').style.format("{:,.0f}"))
                            else:
                                pivot_res = f_df.pivot_table(index=r_axis, columns=c_axis, values=v_axis, aggfunc='sum', margins=True, margins_name="合計")
                                st.dataframe(pivot_res.style.format("{:,.0f}"), use_container_width=True)
                    except Exception as e:
                        st.error(f"集計エラー: {e}")
                        st.exception(e)

# --- 4. RAWデータ追加 (V3方式 高機能版) ---
with tab_upload:
    if not project_id:
        st.info("💡 「⚙️ システム管理」タブで GCP Project ID を設定してください。")
        st.stop()
        
    st.subheader("📥 大容量データのアップロード")
    st.caption("1. ファイルをドロップ ➔ 2. 送信完了後、下のボタンを押して登録 (1GBまで対応)")

    if '_up_uuid' not in st.session_state:
        st.session_state._up_uuid = uuid.uuid4().hex[:8]
    uid = st.session_state._up_uuid
    temp_data_path = f"up_data_{uid}.bin"
    temp_tag_path = f"up_tag_{uid}.txt"

    try:
        data_signed_url = db_manager.get_gcs_signed_url(temp_data_path)
        tag_signed_url = db_manager.get_gcs_signed_url(temp_tag_path)

        upload_html = f"""
        <div id="drop-zone" style="border:2px dashed #94a3b8; border-radius:12px; background:#f8fafc; padding:35px; text-align:center; cursor:pointer; transition: 0.3s;">
            <div id="status" style="font-weight:600; color:#475569; font-family:sans-serif;">ここにファイルをドロップ</div>
            <div id="bar-wrap" style="display:none; margin:15px auto; width:80%; background:#e2e8f0; height:8px; border-radius:4px; overflow:hidden;">
                <div id="bar" style="width:0%; height:100%; background:#3b82f6; transition:width .2s;"></div>
            </div>
            <div id="hint" style="font-size:0.8rem; color:#94a3b8; margin-top:10px; font-family:sans-serif;">(自動でファイル名を認識します)</div>
            <input type="file" id="file-in" style="display:none;" autocomplete="off">
        </div>
        <script>
        const zone=document.getElementById('drop-zone'), input=document.getElementById('file-in'),
              status=document.getElementById('status'), bar=document.getElementById('bar'), wrap=document.getElementById('bar-wrap');
        zone.onclick=()=>input.click();
        input.onchange=()=>{{ if(input.files[0]) upload(input.files[0]); }};
        zone.ondragover=e=>{{ e.preventDefault(); zone.style.background='#eff6ff'; zone.style.borderColor='#3b82f6'; }};
        zone.ondragleave=()=>{{ zone.style.background='#f8fafc'; zone.style.borderColor='#94a3b8'; }};
        zone.ondrop=e=>{{ e.preventDefault(); if(e.dataTransfer.files[0]) upload(e.dataTransfer.files[0]); }};

        async function upload(file) {{
            status.innerText = file.name + ' を送信中...';
            wrap.style.display='block';
            const xhr=new XMLHttpRequest();
            xhr.open('PUT', '{data_signed_url}');
            xhr.setRequestHeader('Content-Type', 'application/octet-stream');
            xhr.upload.onprogress=e=>{{
                const p=Math.round(e.loaded/e.total*100);
                bar.style.width=p+'%';
            }};
            xhr.onload=async ()=>{{
                if(xhr.status===200) {{
                    status.innerText = '本体完了。ファイル名を記録中...';
                    const tagXhr = new XMLHttpRequest();
                    tagXhr.open('PUT', '{tag_signed_url}');
                    tagXhr.setRequestHeader('Content-Type', 'application/octet-stream');
                    tagXhr.onload = () => {{
                        if (tagXhr.status === 200) {{
                            status.innerText = '✅ 送信完了！「' + file.name + '」の登録準備完了';
                            wrap.style.display='none';
                        }}
                    }};
                    tagXhr.send(file.name);
                }} else {{ status.innerText='送信エラー: ' + xhr.status; }}
            }};
            xhr.send(file);
        }}
        </script>
        """
        components.html(upload_html, height=200)
    except Exception as e:
        st.error(f"署名付きURLの取得に失敗しました: {e}")

    if st.button("🚀 BigQueryへの登録を開始する", type="primary", use_container_width=True):
        with st.status("⌛ 処理中...") as stat:
            try:
                tag_io = db_manager.get_gcs_blob_io(temp_tag_path)
                if not tag_io:
                    st.warning("アップロードが完了していません。")
                else:
                    detected_fn = tag_io.read().decode('utf-8').strip()
                    blob_io = db_manager.get_gcs_blob_io(temp_data_path)
                    df = processor.parse_raw_only(blob_io, rules=rules)
                    if df is not None:
                        db_manager.save_raw_data(df, detected_fn, processor.detect_source(detected_fn), overwrite=True)
                        db_manager.delete_gcs_file(temp_data_path)
                        db_manager.delete_gcs_file(temp_tag_path)
                        stat.update(label=f"✅ {detected_fn} を登録しました", state="complete")
                        clear_app_cache()
                        time.sleep(1); st.rerun()
                    else: stat.update(label="❌ 解析失敗", state="error")
            except Exception as e: st.error(f"エラー: {e}")

    st.divider()
    st.markdown("#### 📋 取り込み済み履歴 (最新10件)")
    history_df = db_manager.get_file_history()
    if not history_df.empty:
        for _, h in history_df.head(10).iterrows():
            with st.container(border=True):
                c1, c2, c3 = st.columns([4, 2, 1])
                c1.write(f"📄 **{h['filename']}**")
                c2.caption(f"📊 {h['row_count']:,} 件 | 📅 {h['uploaded_at']}")
                if c3.button("🗑️ 削除", key=f"del_h_{h['filename']}"):
                    db_manager.delete_raw_data(h['filename'])
                    clear_app_cache(); st.rerun()

# --- 5. 管理タブ (マッピング管理) ---
with tab_settings:
    st.subheader("⚙️ システム管理")
    # マッピング・ルール・接続設定の表示
    st.info("マッピングやルールの編集はここで行います。")
    new_api_key = st.text_input("Gemini API Key", value=gemini_api_key, type="password", autocomplete="new-password")
    if st.button("💾 設定を保存"):
        st.session_state.gemini_api_key = new_api_key.strip()
        st.rerun()
    if st.button("💣 データベースリセット"):
        db_manager.reset_dataset(); st.rerun()
