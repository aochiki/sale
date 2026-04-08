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

# --- App Layout ---
default_project_id = os.getenv('GOOGLE_CLOUD_PROJECT', st.session_state.get('project_id', ''))
st.title("📊 売上データ管理システム")
st.caption("Auto-Detect Upload & AI Aggregation")
st.markdown("---")

with st.expander("⚙️ システム設定", expanded=not default_project_id):
    project_id = st.text_input("GCP Project ID", value=default_project_id)
    if project_id:
        st.session_state['project_id'] = project_id
        db_manager = get_db(project_id)
        processor = SalesAggregator()
    else:
        st.stop()

tab_view, tab_flexible, tab_ai, tab_upload, tab_settings = st.tabs([
    "📋 売上データ閲覧", "📊 自由集計", "🤖 AI集計", "📥 RAWデータ追加", "⚙️ システム管理"
])

# --- 共通データの取得 ---
raw_df = fetch_raw_data(project_id)
mappings = fetch_mappings(project_id)
rules = fetch_rules(project_id)
unified_df = pd.DataFrame()

if not raw_df.empty and not mappings.empty:
    with st.status("🔄 データを動的に統合中...", expanded=False):
        unified_df = processor.unify_raw_records(raw_df, mappings)

# --- 1. 閲覧タブ ---
with tab_view:
    if raw_df.empty:
        st.info("データがありません。RAWデータをアップロードしてください。")
    elif unified_df.empty:
        st.warning("マッピング設定に基づいて統合されたデータがありません。設定を確認してください。")
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
        st.download_button("📥 データをCSVとしてダウンロード", filtered.to_csv(index=False), f"unified_{datetime.datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

# --- 2. 自由集計タブ ---
with tab_flexible:
    if unified_df.empty:
        st.info("集計可能なデータがありません。")
    else:
        st.subheader("📊 ピボットテーブル集計")
        attr_cols = [m['unified_name'] for _, m in mappings.iterrows() if not m['is_numeric'] and not m['is_date']]
        num_cols = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
        
        c1, c2, c3 = st.columns(3)
        row_axis = c1.selectbox("縦軸 (行)", [None] + attr_cols + ['SOURCE'])
        col_axis = c2.selectbox("横軸 (列)", [None] + attr_cols + ['SOURCE'])
        val_axis = c3.selectbox("集計値", num_cols if num_cols else [None])
        
        if val_axis:
            try:
                pivot_df = unified_df.pivot_table(
                    index=row_axis, columns=col_axis, values=val_axis,
                    aggfunc='sum', margins=True, margins_name="合計"
                )
                st.dataframe(pivot_df.style.format("{:,.0f}"), use_container_width=True)
            except:
                st.info("選択した項目の組み合わせで集計できませんでした。")

# --- 3. AI集計タブ ---
with tab_ai:
    if unified_df.empty:
        st.info("集計可能なデータがありません。")
    else:
        st.subheader("🤖 自然言語によるAI集計")
        st.caption("「かりゆし58の曲ごとの売上を表示」など、入力してください。")
        
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
                    parsed = parse_natural_language_query(project_id, user_query, all_cols, num_cols_ai)
                    
                    if parsed:
                        with st.expander("🔍 AIの解析結果"): st.json(parsed)
                        try:
                            f_df = flex_df_ai.copy()
                            for col, val in parsed.get("filters", {}).items():
                                if col in f_df.columns:
                                    f_df = f_df[f_df[col].astype(str).str.contains(str(val), na=False, case=False)]
                            
                            r = parsed.get("row_axis"); c = parsed.get("col_axis"); vs = parsed.get("value_axis", [])
                            if not vs and num_cols_ai: vs = [num_cols_ai[0]]
                            
                            if not vs: st.warning("集計対象が見つかりません。")
                            elif not r and not c:
                                st.write(f"### 📋 合計: {', '.join(vs)}")
                                st.dataframe(f_df[vs].sum().to_frame(name='合計').style.format("{:,.0f}"))
                            else:
                                pivot_res = f_df.pivot_table(index=r, columns=c, values=vs, aggfunc='sum', margins=True, margins_name="合計")
                                st.dataframe(pivot_res.style.format("{:,.0f}"), use_container_width=True)
                        except Exception as e: st.error(f"集計エラー: {e}")
                    else: st.error("AI解析に失敗しました。")

# --- 4. RAWデータ追加 (V3方式 復元版) ---
with tab_upload:
    st.subheader("📥 大容量データのアップロード")
    st.caption("1. ファイルを枠内にドロップ ➔ 2. 送信完了後、下のボタンを押して登録")

    # セッションごとに固定のプレフィックス
    if '_up_uuid' not in st.session_state:
        st.session_state._up_uuid = uuid.uuid4().hex[:8]
    uid = st.session_state._up_uuid
    temp_data_path = f"up_data_{uid}.bin"
    temp_tag_path = f"up_tag_{uid}.txt"

    try:
        data_signed_url = db_manager.get_gcs_signed_url(temp_data_path)
        tag_signed_url = db_manager.get_gcs_signed_url(temp_tag_path)

        upload_html = f"""
        <div id="drop-zone" style="border:2px dashed #94a3b8; border-radius:12px; background:#f8fafc; padding:35px; text-align:center; cursor:pointer;">
            <div id="status" style="font-weight:600; color:#475569; font-family:sans-serif;">ここにファイルをドロップ</div>
            <div id="bar-wrap" style="display:none; margin:15px auto; width:80%; background:#e2e8f0; height:8px; border-radius:4px; overflow:hidden;">
                <div id="bar" style="width:0%; height:100%; background:#3b82f6; transition:width .2s;"></div>
            </div>
            <div id="hint" style="font-size:0.8rem; color:#94a3b8; margin-top:10px; font-family:sans-serif;">(自動でファイル名を認識します・1GBまで対応)</div>
            <input type="file" id="file-in" style="display:none;">
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
                            status.innerText = '✅ 送信完了！「' + file.name + '」の登録準備が整いました';
                            wrap.style.display='none';
                        }} else {{ status.innerText = 'エラー: ' + tagXhr.status; }}
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

    # 重複チェックの確認
    if 'dup_target' in st.session_state:
        target = st.session_state.dup_target
        st.warning(f"⚠️ {target} は既に登録されています。上書きしますか？")
        c1, c2 = st.columns(2)
        if c1.button("🔥 上書きして登録", type="primary", use_container_width=True):
            with st.status("🔄 上書き登録中...") as force_stat:
                try:
                    blob_io = db_manager.get_gcs_blob_io(temp_data_path)
                    df = processor.parse_raw_only(blob_io, rules=rules)
                    if df is not None:
                        db_manager.save_raw_data(df, target, processor.detect_source(target), overwrite=True)
                        db_manager.delete_gcs_file(temp_data_path)
                        db_manager.delete_gcs_file(temp_tag_path)
                        del st.session_state.dup_target
                        force_stat.update(label=f"✅ {target} を上書き登録しました", state="complete")
                        clear_app_cache()
                        time.sleep(1); st.rerun()
                except Exception as e: st.error(f"エラー: {e}")
        if c2.button("🚫 キャンセル", use_container_width=True):
            del st.session_state.dup_target; st.rerun()
        st.stop()

    if st.button("🚀 BigQueryへの登録を開始する", type="primary", use_container_width=True):
        with st.status("⌛ 準備を確認中...") as stat:
            try:
                tag_io = db_manager.get_gcs_blob_io(temp_tag_path)
                if not tag_io:
                    st.warning("アップロードが完了していません。")
                    st.stop()
                detected_fn = tag_io.read().decode('utf-8').strip()
                
                # 履歴取得
                all_history = db_manager.get_file_history()
                if not all_history.empty and (detected_fn in all_history['filename'].values):
                    st.session_state.dup_target = detected_fn
                    st.rerun()

                stat.update(label=f"📦 {detected_fn} を処理中...")
                blob_io = db_manager.get_gcs_blob_io(temp_data_path)
                df = processor.parse_raw_only(blob_io, rules=rules)
                if df is not None:
                    db_manager.save_raw_data(df, detected_fn, processor.detect_source(detected_fn), overwrite=True)
                    db_manager.delete_gcs_file(temp_data_path)
                    db_manager.delete_gcs_file(temp_tag_path)
                    stat.update(label=f"✅ {detected_fn} を登録しました", state="complete")
                    clear_app_cache()
                    time.sleep(1); st.rerun()
                else: stat.update(label="❌ 解析失敗。形式を確認してください。", state="error")
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

# --- 5. 管理タブ (V3/V4 マッピング管理) ---
with tab_settings:
    st.subheader("⚙️ システム管理")
    
    # マッピング管理
    st.markdown("#### 🔗 統合マッピング定義")
    orch_cols = db_manager.get_headers_by_pattern("Orchard%")
    next_cols = db_manager.get_headers_by_pattern("DivSiteAll%")
    itunes_cols = db_manager.get_headers_by_pattern("%_ZZ%")
    
    if 'edit_mapping' not in st.session_state: st.session_state.edit_mapping = None
    edit_data = st.session_state.edit_mapping

    with st.form("mapping_form"):
        u_name = st.text_input("統合項目名", value=edit_data['unified_name'] if edit_data else "")
        c1, c2, c3 = st.columns(3)
        def get_idx(lst, val):
            try: return ([""] + lst).index(val)
            except: return 0
        
        o_col = c1.selectbox("Orchard 列", [""] + (orch_cols or []), index=get_idx(orch_cols, edit_data['orchard_col']) if edit_data else 0)
        n_col = c2.selectbox("NexTone 列", [""] + (next_cols or []), index=get_idx(next_cols, edit_data['nextone_col']) if edit_data else 0)
        i_col = c3.selectbox("iTunes 列", [""] + (itunes_cols or []), index=get_idx(itunes_cols, edit_data['itunes_col']) if edit_data else 0)
        
        is_d = st.checkbox("日付項目", value=edit_data['is_date'] if edit_data else False)
        is_n = st.checkbox("数値項目", value=edit_data['is_numeric'] if edit_data else False)
        
        if st.form_submit_button("💾 マッピングを保存"):
            if u_name:
                db_manager.save_unified_column(u_name, o_col, n_col, i_col, is_d, is_n)
                st.session_state.edit_mapping = None
                clear_app_cache(); st.rerun()

    cur_mappings = fetch_mappings(project_id)
    for i, m in cur_mappings.iterrows():
        with st.container(border=True):
            ct, ce, cd = st.columns([4, 1, 1])
            ct.write(f"📁 **{m['unified_name']}** (O:{m['orchard_col']} / N:{m['nextone_col']} / I:{m['itunes_col']})")
            if ce.button("📝 編集", key=f"ed_{i}"):
                st.session_state.edit_mapping = m.to_dict(); st.rerun()
            if cd.button("🗑️ 削除", key=f"dl_{i}"):
                db_manager.delete_unified_column(m['unified_name'])
                clear_app_cache(); st.rerun()

    st.divider()
    # ルール管理
    st.markdown("#### 📄 解析ルールの設定")
    with st.form("rule_add"):
        c1, c2, c3 = st.columns([3, 1, 1])
        pat = c1.text_input("ファイル名キーワード")
        hr = c2.number_input("ヘッダー開始行", min_value=1, value=1)
        if c3.form_submit_button("➕ 追加"):
            if pat:
                db_manager.save_parsing_rule(pat, hr - 1)
                clear_app_cache(); st.rerun()
                
    for idx, row in rules.iterrows():
        with st.container(border=True):
            r1, r2, r3 = st.columns([3, 1, 1])
            r1.write(f"パターン: `{row['file_pattern']}`")
            r2.write(f"ヘッダー: {row['header_row']+1}行目")
            if r3.button("🗑️ 削除", key=f"dr_{idx}"):
                db_manager.delete_parsing_rule(row['file_pattern'])
                clear_app_cache(); st.rerun()

    st.divider()
    if st.button("💣 データベースを完全にリセットする", type="primary"):
        db_manager.reset_dataset(); clear_app_cache(); st.rerun()
