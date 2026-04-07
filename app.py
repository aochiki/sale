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
    raw = db_manager.get_raw_data()
    maps = db_manager.get_unified_columns()
    rules = db_manager.get_parsing_rules()
    return raw, maps, rules

raw_df, mappings, rules = load_all_data(project_id)
unified_df = pd.DataFrame()
if not raw_df.empty and not mappings.empty:
    unified_df = processor.unify_raw_records(raw_df, mappings)

tab_view, tab_upload, tab_settings = st.tabs(["📋 売上一覧", "📥 データの追加", "⚙️ 設定"])

with tab_view:
    if unified_df.empty: st.info("表示できるデータがありません。")
    else:
        st.dataframe(unified_df, use_container_width=True, hide_index=True)

# --- 3. データの追加 (GCS直接送信 + ネイティブ発火) ---
with tab_upload:
    st.subheader("📥 大容量データのアップロード")
    st.caption("1. ファイルを枠内にドロップ ➔ 2. 送信完了後、下のボタンを押して登録")

    # セッション固有のテンポラリファイル名
    if '_temp_fn' not in st.session_state:
        st.session_state._temp_fn = f"_up_{uuid.uuid4().hex[:8]}_latest.csv"
    
    temp_name = st.session_state._temp_fn

    try:
        signed_url = db_manager.get_gcs_signed_url(temp_name)
        
        # JSでGCSへ直接送信（413エラーを回避）
        upload_html = f"""
        <div id="drop-zone" style="border:2px dashed #3b82f6; border-radius:12px; background:#eff6ff; padding:40px; text-align:center; cursor:pointer;">
            <div id="icon" style="font-size:2.5rem;">☁️</div>
            <div id="status" style="font-weight:600; margin-top:10px; color:#1e40af;">ここにファイルをドロップ</div>
            <div id="bar-wrap" style="display:none; margin:15px auto; width:80%; background:#d1d5db; height:10px; border-radius:5px; overflow:hidden;">
                <div id="bar" style="width:0%; height:100%; background:#2563eb; transition:width .2s;"></div>
            </div>
            <div id="hint" style="font-size:0.8rem; color:#6b7280; margin-top:10px;">(1GBまでのCSV/TSVに対応)</div>
            <input type="file" id="file-in" style="display:none;">
        </div>
        <script>
        const zone=document.getElementById('drop-zone'), input=document.getElementById('file-in'),
              status=document.getElementById('status'), bar=document.getElementById('bar'), wrap=document.getElementById('bar-wrap');
        
        zone.onclick=()=>input.click();
        input.onchange=()=>{{ if(input.files[0]) upload(input.files[0]); }};
        zone.ondragover=e=>{{ e.preventDefault(); zone.style.background='#dbeafe'; }};
        zone.ondragleave=()=>zone.style.background='#eff6ff';
        zone.ondrop=e=>{{ e.preventDefault(); if(e.dataTransfer.files[0]) upload(e.dataTransfer.files[0]); }};

        function upload(file) {{
            status.innerText = file.name + ' を送信中...';
            // 親ウィンドウにファイル名を伝えるための仕掛け（クエリパラメータは使わず、単純なリセット通知）
            wrap.style.display='block';
            const xhr=new XMLHttpRequest();
            xhr.open('PUT', '{signed_url}');
            xhr.setRequestHeader('Content-Type', 'application/octet-stream');
            xhr.upload.onprogress=e=>{{
                const p=Math.round(e.loaded/e.total*100);
                bar.style.width=p+'%';
            }};
            xhr.onload=()=>{{
                if(xhr.status===200) {{
                    status.innerText='✅ 送信完了！下のボタンを押してください';
                }} else {{ status.innerText='エラー: ' + xhr.status; }}
            }};
            xhr.send(file);
        }}
        </script>
        """
        import streamlit.components.v1 as components
        components.html(upload_html, height=220)
    except Exception as e: st.error(f"準備エラー: {e}")

    st.markdown("---")
    
    # Python側の発火ボタン
    # JSとPythonの橋渡しとして、GCS上のファイル存在を物理的に確認する
    if st.button("🚀 登録を完了する", type="primary", use_container_width=True):
        with st.status("📦 データをデータベースへ移行しています...") as stat:
            try:
                # GCSから読み込み
                blob_io = db_manager.get_gcs_blob_io(temp_name)
                if blob_io:
                    df = processor.parse_raw_only(blob_io, rules=rules)
                    if df is not None:
                        row_count = len(df)
                        stat.update(label=f"📊 {row_count:,} 件のデータを保存中...")
                        
                        # ファイル名は "latest_import" 等にするか、何らかの方法で特定
                        # 今回はシンプルに、オリジナルのファイル名を入力させるのではなく統合を優先
                        target_fn = f"uploaded_at_{time.strftime('%Y%m%d_%H%M%S')}.csv"
                        db_manager.save_raw_data(df, target_fn, "AutoDetect", overwrite=True)
                        db_manager.delete_gcs_file(temp_name)
                        
                        stat.update(label=f"✅ {row_count:,} 件の登録が正常に完了しました！", state="complete")
                        st.cache_data.clear()
                        time.sleep(2)
                        st.rerun()
                    else: st.error("データの解析に失敗しました。形式を確認してください。")
                else: st.error("アップロードされたファイルが見つかりません。ドロップが完了したか確認してください。")
            except Exception as e: st.error(f"登録エラー: {e}")

    st.divider()
    if not raw_df.empty:
        st.write("#### 📋 取り込み履歴")
        history = raw_df['filename'].unique()
        for h in history:
            c1, c2 = st.columns([5, 1])
            c1.text(f"📄 {h}")
            if c2.button("🗑️", key=f"del_{h}"):
                db_manager.delete_raw_data(h)
                st.cache_data.clear()
                st.rerun()

with tab_settings:
    st.subheader("⚙️ 設定")
    if st.button("🔥 全データを初期化"):
        db_manager.reset_dataset()
        st.cache_data.clear()
        st.rerun()
