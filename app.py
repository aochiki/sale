import streamlit as st
import pandas as pd
from aggregator.processor import SalesAggregator
from aggregator.database_bq import DatabaseManager
import os
import time
import uuid

# --- Page Config ---
st.set_page_config(page_title="売上データ管理", page_icon="📊", layout="wide")

# --- Premium Style ---
st.markdown("""
<style>
    .stApp { background-color: #f8f9fa; }
    .stTabs [data-baseweb="tab-list"] { gap: 20px; }
    .stTabs [data-baseweb="tab"] { height: 50px; font-weight: 600; }
    h1 { color: #1e3a8a; }
</style>
""", unsafe_allow_html=True)

# --- Logic ---
@st.cache_resource
def get_db(project_id):
    return DatabaseManager(project_id=project_id, dataset_id="sales_aggregator_dataset")

default_project_id = os.getenv('GOOGLE_CLOUD_PROJECT', st.session_state.get('project_id', ''))
project_id = st.session_state.get('project_id', default_project_id)

if not project_id:
    st.warning("GCPプロジェクトIDを設定してください。")
    st.stop()

db_manager = get_db(project_id)
processor = SalesAggregator()

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

tab_upload, tab_view, tab_rules, tab_mapping, tab_settings = st.tabs([
    "📥 データの追加", "📋 売上一覧", "🛠️ 解析ルール設定", "🔗 項目マッピング", "⚙️ 設定"
])

# --- 1. データの追加 ---
with tab_upload:
    st.subheader("📥 大容量データのアップロード")
    st.caption("1. ファイルを枠内にドロップ ➔ 2. 送信完了後、下のボタンを押して登録")

    # セッションごとに固定のプレフィックス
    if '_up_uuid' not in st.session_state:
        st.session_state._up_uuid = uuid.uuid4().hex[:8]
    uid = st.session_state._up_uuid
    temp_data_path = f"tmp_data_{uid}.bin"
    
    try:
        data_signed_url = db_manager.get_gcs_signed_url(temp_data_path)
        # 万が一の時のために「名札ファイル」用URLも発行
        # ブラウザ側でこれを使って空のファイルを PUT する
        def get_tag_path(fn): return f"tags/{uid}_{fn}.tag"
        
        import streamlit.components.v1 as components
        upload_html = f"""
        <div id="drop-zone" style="border:2px dashed #94a3b8; border-radius:12px; background:#f8fafc; padding:35px; text-align:center; cursor:pointer;">
            <div id="status" style="font-weight:600; color:#475569;">ここにファイルをドロップ</div>
            <div id="bar-wrap" style="display:none; margin:15px auto; width:80%; background:#e2e8f0; height:8px; border-radius:4px; overflow:hidden;">
                <div id="bar" style="width:0%; height:100%; background:#3b82f6; transition:width .2s;"></div>
            </div>
            <div id="hint" style="font-size:0.8rem; color:#94a3b8; margin-top:10px;">(お名前と進行状況がハッキリ分かります)</div>
            <input type="file" id="file-in" style="display:none;" accept=".csv,.txt,.tsv">
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
            xhr.onload=()=>{{
                if(xhr.status===200) {{ 
                    status.innerText='✅ 送信成功！「' + file.name + '」を登録開始できます';
                    // セーフティ: URLパラメータの更新を試みる
                    try {{
                        const url = new URL(window.top.location.href);
                        url.searchParams.set('up_fn', file.name);
                        window.top.history.replaceState({{}}, '', url);
                    }} catch(e) {{
                        console.log('Param update blocked, relying on tag file.');
                    }}
                }} else {{ status.innerText='エラー: ' + xhr.status; }}
            }};
            xhr.send(file);
        }}
        </script>
        """
        components.html(upload_html, height=200)
    except Exception as e: st.error(f"準備エラー: {e}")

    # 名前を特定する（URLから、ダメならクラウド上のファイル名を探す）
    detected_fn = st.query_params.get("up_fn", "")

    if st.button("🚀 登録を完了する", type="primary", use_container_width=True):
        if not detected_fn:
            # フォールバック: GCS 上の名札ファイルを直接探すことも可能ですが、
            # まずは URL パラメータ方式をメインとしつつ、エラー時はメッセージを出します。
            st.error("ファイル名が特定できませんでした。もう一度ドロップしていただくか、ブラウザを更新してください。")
        else:
            with st.status("🚀 取り込み開始...") as stat:
                try:
                    stat.update(label=f"📁 「{detected_fn}」をクラウドから読み込んでいます...")
                    blob_io = db_manager.get_gcs_blob_io(temp_data_path)
                    if blob_io:
                        stat.update(label="🔍 データの形式を検証しています...")
                        df = processor.parse_raw_only(blob_io, rules=rules)
                        if df is not None:
                            total = len(df)
                            def update_prog(msg): stat.update(label=msg)
                            db_manager.save_raw_data(df, detected_fn, "AutoDetect", overwrite=True, progress_callback=update_prog)
                            db_manager.delete_gcs_file(temp_data_path)
                            stat.update(label=f"✅ {detected_fn} ({total:,}件) の登録が完了しました！", state="complete")
                            st.cache_data.clear()
                            time.sleep(1.5)
                            st.rerun()
                        else: stat.update(label="❌ 解析失敗。形式が正しくありません。", state="error")
                    else: stat.update(label="❌ アップロードされたデータが見つかりません。", state="error")
                except Exception as e: stat.update(label=f"❌ エラーが発生しました: {e}", state="error")

    # 取り込み済み履歴を詳細化
    st.divider()
    if not all_history.empty:
        st.write("#### 📋 取り込み済みファイル履歴")
        for _, h in all_history.head(10).iterrows():
            c1, c2, c3, c4 = st.columns([4, 1.5, 2, 0.5])
            c1.text(f"📄 {h['filename']}")
            c2.write(f"📊 {h['row_count']:,} 件")
            
            # 日時を読みやすく整形 (uploaded_at が Timestamp 型の場合)
            try:
                dt_str = h['uploaded_at'].strftime('%Y-%m-%d %H:%M')
            except:
                dt_str = str(h['uploaded_at'])[:16]
            c3.caption(f"📅 {dt_str}")
            
            if c4.button("🗑️", key=f"del_{h['filename']}"):
                with st.spinner("削除中..."):
                    db_manager.delete_raw_data(h['filename'])
                    st.cache_data.clear()
                    st.rerun()

# --- 2. 売上一覧 ---
with tab_view:
    if unified_df.empty: st.info("表示できるデータがありません。")
    else: st.dataframe(unified_df, use_container_width=True, hide_index=True)

# --- 3. 解析ルール設定 ---
with tab_rules:
    st.subheader("🛠️ 解析ルール設定")
    with st.form("rule_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([3, 1, 1])
        pat = c1.text_input("キーワード (例: orchard)")
        hr = c2.number_input("ヘッダー開始行", min_value=1, value=1)
        if c3.form_submit_button("➕ 追加"):
            if pat:
                db_manager.save_parsing_rule(pat, hr - 1)
                st.cache_data.clear()
                st.rerun()
    
    cur_rules = db_manager.get_parsing_rules()
    for idx, row in cur_rules.iterrows():
        with st.container(border=True):
            r1, r2, r3 = st.columns([3, 1, 1])
            r1.write(f"キーワード: `{row['file_pattern']}`")
            r2.write(f"ヘッダー: {row['header_row']+1}行目")
            if r3.button("🗑️", key=f"del_rule_{idx}"):
                db_manager.delete_parsing_rule(row['file_pattern'])
                st.cache_data.clear()
                st.rerun()

# --- 4. 項目マッピング ---
with tab_mapping:
    st.subheader("🔗 項目マッピング設定")
    cur_mappings = db_manager.get_unified_columns()
    with st.form("mapping_form", clear_on_submit=True):
        u_name = st.text_input("共通項目名")
        c1, c2, c3 = st.columns(3)
        o_col = c1.text_input("Orchard 列名")
        n_col = c2.text_input("NexTone 列名")
        i_col = c3.text_input("iTunes 列名")
        is_d = st.checkbox("日付項目として扱う")
        is_n = st.checkbox("数値項目として扱う")
        if st.form_submit_button("💾 保存"):
            if u_name:
                db_manager.save_unified_column(u_name, o_col, n_col, i_col, is_d, is_n)
                st.cache_data.clear()
                st.rerun()

    for i, m in cur_mappings.iterrows():
        with st.container(border=True):
            col_t, col_b = st.columns([4, 1])
            col_t.write(f"📁 **{m['unified_name']}** (O: {m['orchard_col']}, N: {m['nextone_col']}, I: {m['itunes_col']})")
            if col_b.button("🗑️", key=f"del_map_{i}"):
                db_manager.delete_unified_column(m['unified_name'])
                st.cache_data.clear()
                st.rerun()

# --- 5. 設定 ---
with tab_settings:
    st.subheader("⚙️ 設定")
    st.write(f"Project ID: `{project_id}`")
    if st.button("🔥 全データを初期化 (取り込み履歴・設定すべて)"):
        db_manager.reset_dataset()
        st.cache_data.clear()
        st.rerun()
