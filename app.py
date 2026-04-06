import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from aggregator.processor import SalesAggregator
from aggregator.database_bq import DatabaseManager
import io
import datetime
import logging
import os
import json
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
    .stTabs [data-baseweb="tab"] { font-weight: 600; }
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
st.markdown("---")

with st.expander("⚙️ システム設定", expanded=not default_project_id):
    project_id = st.text_input("GCP Project ID", value=default_project_id)
    if project_id:
        st.session_state['project_id'] = project_id
        db_manager = get_db(project_id)
        processor = SalesAggregator()
    else:
        st.stop()

tab_view, tab_flexible, tab_upload, tab_settings = st.tabs(["📋 売上データ閲覧", "📊 自由集計", "📥 RAWデータ追加", "⚙️ システム管理"])

# --- 共通データの取得 ---
raw_df = fetch_raw_data(project_id)
mappings = fetch_mappings(project_id)
unified_df = pd.DataFrame()
if not raw_df.empty and not mappings.empty:
    with st.status("🔄 データを動的に統合中...", expanded=False):
        unified_df = processor.unify_raw_records(raw_df, mappings)

# --- 1. 閲覧タブ ---
with tab_view:
    if raw_df.empty:
        st.info("データがありません。RAWデータをアップロードしてください。")
    elif unified_df.empty:
        st.warning("マッピング設定に基づいて統合されたデータがありません。")
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            month_col = next((c for c in unified_df.columns if not mappings.empty and mappings[mappings['unified_name']==c]['is_date'].any()), None)
            month_list = ["すべて"] + sorted(unified_df[month_col].dropna().unique().tolist(), reverse=True) if month_col else ["すべて"]
            sel_m = st.selectbox("📅 対象月", month_list)
        with c2:
            sel_s = st.selectbox("🌍 ソース", ["すべて"] + sorted(unified_df['SOURCE'].unique().tolist()))
        
        filtered = unified_df.copy()
        if sel_m != "すべて": filtered = filtered[filtered[month_col] == sel_m]
        if sel_s != "すべて": filtered = filtered[filtered['SOURCE'] == sel_s]
        
        st.dataframe(filtered, use_container_width=True, hide_index=True)

# --- 2. 自由集計タブ ---
with tab_flexible:
    if unified_df.empty:
        st.info("集計可能なデータがありません。")
    else:
        st.subheader("📊 ダイナミック・ピボットレポート")
        attr_cols = [m['unified_name'] for _, m in mappings.iterrows() if not m['is_numeric'] and not m['is_date']]
        num_cols = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
        date_col = next((m['unified_name'] for _, m in mappings.iterrows() if m['is_date']), None)
        
        flex_df = unified_df.copy()
        with st.expander("🛠️ 集計軸の設定", expanded=True):
            cc1, cc2, cc3 = st.columns(3)
            axis_options = attr_cols + (['SOURCE'] if 'SOURCE' in unified_df.columns else [])
            row_axis = cc1.selectbox("タテ軸 (行)", axis_options, index=0 if axis_options else None)
            col_list = ["(なし)"] + axis_options
            col_axis = cc2.selectbox("ヨコ軸 (列)", col_list, index=0)
            val_cols = cc3.multiselect("表示する値 (集計対象)", num_cols, default=num_cols[:1] if num_cols else [])

        if val_cols and row_axis:
            try:
                p_cols = col_axis if col_axis != "(なし)" else None
                pivot_res = flex_df.pivot_table(index=row_axis, columns=p_cols, values=val_cols, aggfunc='sum', margins=True, margins_name="合計")
                st.dataframe(pivot_res.style.format("{:,.0f}"), use_container_width=True)
            except Exception as e:
                st.error(f"集計エラー: {e}")

# --- 3. アップロードタブ (インテリジェント自動化) ---
with tab_upload:
    st.subheader("📥 RAWデータ追加")
    st.caption("アップロード完了後、新規ファイルなら自動で取り込みが開始されます。")

    # --- アップロード処理 (JS: 自動リダイレクト版) ---
    import uuid as _uuid
    if '_upload_slot' not in st.session_state:
        st.session_state._upload_slot = f"_up_{_uuid.uuid4().hex[:8]}"
    
    temp_blob_name = st.session_state._upload_slot

    try:
        signed_url = db_manager.get_gcs_signed_url(temp_blob_name)
        upload_html = f"""
        <div id="drop-zone" style="font-family:sans-serif; border:2px dashed #a0c4ff; border-radius:12px; background:#f0f7ff; padding:40px; text-align:center; cursor:pointer;">
            <div id="icon" style="font-size:3rem;">📂</div>
            <div id="label" style="font-weight:bold; margin:10px 0;">ここにファイルをドラッグ＆ドロップ</div>
            <div id="hint" style="font-size:0.8rem; color:#666;">またはクリックしてファイルを選択</div>
            <div id="prog-wrap" style="display:none; margin-top:20px;">
                <div style="background:#eee; height:10px; border-radius:5px; overflow:hidden;">
                    <div id="prog-bar" style="width:0%; height:100%; background:#007bff; transition:width .2s;"></div>
                </div>
                <div id="prog-txt" style="font-size:0.8rem; margin-top:5px;">0%</div>
            </div>
            <input type="file" id="file-pick" style="display:none;">
        </div>
        <script>
        (function() {{
            const zone=document.getElementById('drop-zone'), pick=document.getElementById('file-pick'),
                  icon=document.getElementById('icon'), label=document.getElementById('label'),
                  hint=document.getElementById('hint'), wrap=document.getElementById('prog-wrap'),
                  bar=document.getElementById('prog-bar'), txt=document.getElementById('prog-txt');
            
            zone.onclick=()=>pick.click();
            pick.onchange=()=>{{ if(pick.files[0]) upload(pick.files[0]); }};
            zone.ondragover=e=>{{ e.preventDefault(); zone.style.background='#e3f2fd'; }};
            zone.ondragleave=()=>zone.style.background='#f0f7ff';
            zone.ondrop=e=>{{ e.preventDefault(); if(e.dataTransfer.files[0]) {{ pick.files=e.dataTransfer.files; upload(pick.files[0]); }} }};

            function upload(file) {{
                zone.onclick=null; zone.style.cursor='default';
                label.innerText=file.name; hint.innerText='送信中...'; wrap.style.display='block';
                const xhr=new XMLHttpRequest();
                xhr.open('PUT', '{signed_url}');
                xhr.setRequestHeader('Content-Type', 'application/octet-stream');
                xhr.upload.onprogress=e=>{{
                    const p=Math.round(e.loaded/e.total*100);
                    bar.style.width=p+'%'; txt.innerText=p+'% ('+(e.loaded/1024/1024).toFixed(1)+'/'+(e.total/1024/1024).toFixed(1)+'MB)';
                }};
                xhr.onload=()=>{{
                    if(xhr.status===200) {{
                        icon.innerText='✅'; label.innerText='アップロード完了！';
                        hint.innerText='自動的にデータベースへ登録を開始します...';
                        const jumpUrl = '?up_blob={temp_blob_name}&fn=' + encodeURIComponent(file.name);
                        // window.top.location.href を使って確実に親画面を遷移させる
                        setTimeout(() => {{
                            try {{
                                window.top.location.href = jumpUrl;
                            }} catch(e) {{
                                // サンドボックス制限などで失敗した場合はリンクを表示
                                hint.innerHTML = '<a href="' + jumpUrl + '" target="_top" style="display:inline-block; margin-top:10px; padding:10px 20px; background:#28a745; color:white; text-decoration:none; border-radius:5px; font-weight:bold;">登録を進める（ここをクリック）</a>';
                            }}
                        }}, 1500);
                    }} else {{ label.innerText='エラー: '+xhr.status; }}
                }};
                xhr.send(file);
            }}
        }})();
        </script>
        """
        components.html(upload_html, height=230)
    except Exception as e:
        st.error(f"準備エラー: {e}")

    # --- インテリジェント自動取り込みロジック ---
    qp = st.query_params
    if "up_blob" in qp and "fn" in qp:
        b_name, r_name = qp["up_blob"], qp["fn"]
        st.markdown(f"📦 **ファイル受信完了:** `{r_name}`")
        
        # 重複チェック
        is_existing = not raw_df.empty and r_name in raw_df['filename'].unique()
        
        auto_run = False
        if is_existing:
            st.warning(f"⚠️ `{r_name}` は既にデータベースに存在します。内容を上書きしますか？")
            c1, c2 = st.columns([1, 4])
            if c1.button("🔥 上書き登録", type="primary"):
                auto_run = True
            if c2.button("キャンセル"):
                st.query_params.clear()
                st.rerun()
        else:
            # 新規なら自動実行を許可
            auto_run = True
            st.info("🔄 新規ファイルとして自動登録を開始します。そのままお待ちください...")

        if auto_run:
            with st.status(f"🚀 {r_name} を処理中...") as status:
                try:
                    if db_manager.rename_gcs_file(b_name, r_name):
                        blob_io = db_manager.get_gcs_blob_io(r_name)
                        rules = fetch_rules(project_id)
                        df = processor.parse_raw_only(blob_io, rules=rules)
                        if df is not None:
                            s_type = processor.detect_source(r_name)
                            row_count = db_manager.save_raw_data(df, r_name, s_type, overwrite=True)
                            db_manager.delete_gcs_file(r_name)
                            status.update(label=f"✅ {r_name} ({row_count:,}件) の登録が完了しました", state="complete")
                            st.toast(f"登録完了: {r_name}", icon="✅")
                            # 完了後にパラメータを消してリロード
                            st.query_params.clear()
                            clear_app_cache()
                            time.sleep(2)
                            st.rerun()
                        else: st.error("解析に失敗しました。ファイル形式を確認してください。")
                    else: st.error("GCS上のファイル名変更に失敗しました。")
                except Exception as e: st.error(f"処理エラー: {e}")

    st.divider()
    st.markdown("#### 📋 取り込み済みデータ一覧")
    if not raw_df.empty:
        summary = raw_df.groupby('filename').agg({'source_type':'first', 'row_index':'count'}).reset_index()
        for i, row in summary.iterrows():
            with st.container(border=True):
                cc1, cc2, cc3 = st.columns([4, 1, 1])
                cc1.write(f"📄 **{row['filename']}** ({row['source_type']})")
                cc2.write(f"{row['row_index']:,} 件")
                if cc3.button("🗑️", key=f"del_{i}"):
                    if db_manager.delete_raw_data(row['filename']):
                        clear_app_cache()
                        st.rerun()
    else: st.info("データがありません。")

# --- 4. 管理タブ ---
with tab_settings:
    st.subheader("⚙️ システム管理")
    cur_mappings = fetch_mappings(project_id)
    with st.expander("🔗 カラムマッピング定義"):
        st.dataframe(cur_mappings, use_container_width=True)
        st.info("※マッピングの編集は以前のバージョンで行ってください。")

    st.divider()
    if st.button("🔥 データベース全体を初期化する", type="primary"):
        db_manager.reset_dataset()
        clear_app_cache()
        st.rerun()
