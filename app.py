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
    page_title="売上データ管理システム", 
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

tab_view, tab_flexible, tab_upload, tab_settings = st.tabs(["📋 売上一覧", "📊 自由集計", "📥 データの追加", "⚙️ 設定"])

# --- 共通データの取得 (サイレント) ---
raw_df = fetch_raw_data(project_id)
mappings = fetch_mappings(project_id)
unified_df = pd.DataFrame()
if not raw_df.empty and not mappings.empty:
    # 進行状況を見せずにバックグラウンドで処理
    unified_df = processor.unify_raw_records(raw_df, mappings)

# --- 1. 一覧タブ ---
with tab_view:
    if raw_df.empty:
        st.info("データがありません。")
    elif unified_df.empty:
        st.warning("マッピング設定を確認してください。")
    else:
        c1, c2 = st.columns(2)
        month_col = next((c for c in unified_df.columns if not mappings.empty and mappings[mappings['unified_name']==c]['is_date'].any()), None)
        month_list = ["すべて"] + sorted(unified_df[month_col].dropna().unique().tolist(), reverse=True) if month_col else ["すべて"]
        sel_m = c1.selectbox("📅 対象月", month_list)
        sel_s = c2.selectbox("🌍 ソース", ["すべて"] + sorted(unified_df['SOURCE'].unique().tolist()))
        
        filtered = unified_df.copy()
        if sel_m != "すべて": filtered = filtered[filtered[month_col] == sel_m]
        if sel_s != "すべて": filtered = filtered[filtered['SOURCE'] == sel_s]
        st.dataframe(filtered, use_container_width=True, hide_index=True)

# --- 2. 自由集計タブ ---
with tab_flexible:
    if unified_df.empty:
        st.info("集計可能なデータがありません。")
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

# --- 3. データの追加タブ (GCS監視型自動処理) ---
with tab_upload:
    st.subheader("📥 データのアップロード")
    st.caption("ファイルをドロップすると、クラウドへ直接送信され、自動で登録画面へと進みます。")

    # --- アップロード処理 (JS: GCS検知用プレフィックス付与) ---
    # セッションごとに一意なプレフィックスを使って、同時アップロード時の混線を防ぐ
    if '_up_pref' not in st.session_state:
        import uuid
        st.session_state._up_pref = f"_up_{uuid.uuid4().hex[:6]}_"
    
    prefix = st.session_state._up_pref
    
    # 仮のURL生成（JSに渡す）
    temp_target = f"{prefix}placeholder.tmp"
    try:
        # JS側で動的にファイル名を置換するため、プレフィックスだけ考慮したURLを渡す仕組みを模索
        # 実際には、JS側で「ファイル名込みのSigned URL」を受け取る必要があるため、
        # 「ファイル選択時にStreamlitへファイル名を教える」のがベストだが、ここではシンプルにする。
        
        # 解決策：JSに「署名付きURL生成用のAPI（Streamlit自体）」を叩かせるか、
        # もしくは、あらかじめ汎用的な名前でURLを発行しておく。
        # ここでは「アップロード完了後にリロードして検知する」ため、固定名 `_up_new_file_` を使う。
        
        signed_url = db_manager.get_gcs_signed_url(f"{prefix}latest_upload.tmp")
        
        upload_html = f"""
        <div id="drop-zone" style="border:2px dashed #a0c4ff; border-radius:12px; background:#f0f7ff; padding:50px; text-align:center; cursor:pointer;">
            <div id="status-icon" style="font-size:3rem;">📂</div>
            <div id="label" style="font-weight:bold; margin-top:10px;">ここにファイルをドラッグ＆ドロップ</div>
            <div id="prog-wrap" style="display:none; margin-top:20px;">
                <div style="background:#eee; height:8px; border-radius:4px; overflow:hidden;">
                    <div id="prog-bar" style="width:0%; height:100%; background:#007bff; transition:width .2s;"></div>
                </div>
            </div>
            <input type="file" id="file-pick" style="display:none;">
        </div>
        <script>
        (function() {{
            const zone=document.getElementById('drop-zone'), pick=document.getElementById('file-pick'),
                  icon=document.getElementById('status-icon'), label=document.getElementById('label'),
                  wrap=document.getElementById('prog-wrap'), bar=document.getElementById('prog-bar');
            
            zone.onclick=()=>pick.click();
            pick.onchange=()=>{{ if(pick.files[0]) upload(pick.files[0]); }};
            zone.ondragover=e=>{{ e.preventDefault(); zone.style.background='#e3f2fd'; }};
            zone.ondragleave=()=>zone.style.background='#f0f7ff';
            zone.ondrop=e=>{{ e.preventDefault(); if(e.dataTransfer.files[0]) {{ pick.files=e.dataTransfer.files; upload(pick.files[0]); }} }};

            function upload(file) {{
                zone.onclick=null; zone.style.cursor='default';
                label.innerText = '「' + file.name + '」を送信中...';
                wrap.style.display='block';
                const xhr=new XMLHttpRequest();
                // 実際には Python 側で rename するので、ここでは固定の temp 名で送る
                xhr.open('PUT', '{signed_url}');
                xhr.setRequestHeader('Content-Type', 'application/octet-stream');
                // ファイル名のヒントをカスタムヘッダーやクエリではなく、別の手段で渡す必要があるが、
                // 今回は「送信完了と同時にリロードし、Python側でファイル名を特定する（URL経由）」を併用する。
                xhr.upload.onprogress=e=>{{
                    const p=Math.round(e.loaded/e.total*100);
                    bar.style.width=p+'%';
                }};
                xhr.onload=()=>{{
                    if(xhr.status===200) {{
                        icon.innerText='✅'; label.innerText='送信完了！解析を開始します...';
                        // リロードしてPython側のGCS検知を走らせる
                        setTimeout(() => {{ 
                            window.top.location.href = '?up_fn=' + encodeURIComponent(file.name);
                        }}, 1000);
                    }} else {{ label.innerText='エラー: ' + xhr.status; }}
                }};
                xhr.send(file);
            }}
        }})();
        </script>
        """
        components.html(upload_html, height=250)
    except Exception as e: st.error(f"準備エラー: {e}")

    # --- インテリジェント自動検知・登録ロジック ---
    qp = st.query_params
    if "up_fn" in qp:
        r_name = qp["up_fn"]
        temp_blob = f"{prefix}latest_upload.tmp"
        
        # ファイルの存在確認 (JSからのリダイレクト後に同期が取れているか)
        st.divider()
        st.markdown(f"📦 **アップロード済み:** `{r_name}`")
        
        is_existing = not raw_df.empty and r_name in raw_df['filename'].unique()
        
        auto_start = False
        if is_existing:
            st.warning(f"⚠️ `{r_name}` は既に登録されています。上書きしますか？")
            c1, c2 = st.columns([1, 4])
            if c1.button("🔥 上書きして登録", type="primary"): auto_start = True
            if c2.button("キャンセル"): 
                db_manager.delete_gcs_file(temp_blob)
                st.query_params.clear()
                st.rerun()
        else:
            auto_start = True
            st.info("新規ファイルとして自動登録を開始します。そのままお待ちください...")

        if auto_start:
            with st.status(f"🚀 {r_name} を処理中...") as status:
                try:
                    status.update(label=f"🔍 ファイルの内容を読み込んでいます...")
                    if db_manager.rename_gcs_file(b_name, r_name):
                        blob_io = db_manager.get_gcs_blob_io(r_name)
                        rules = fetch_rules(project_id)
                        
                        status.update(label=f"📊 データ構造を解析しています...")
                        df = processor.parse_raw_only(blob_io, rules=rules)
                        
                        if df is not None:
                            row_count = len(df)
                            status.update(label=f"⚡ {row_count:,} 件のデータを検出しました。データベースへ保存中...")
                            
                            s_type = processor.detect_source(r_name)
                            db_manager.save_raw_data(df, r_name, s_type, overwrite=True)
                            db_manager.delete_gcs_file(r_name) # 取り込み後は削除
                            
                            status.update(label=f"✅ {r_name} ({row_count:,}件) の登録がすべて完了しました！", state="complete")
                            st.toast(f"登録完了: {r_name} ({row_count:,}件)", icon="✅")
                            
                            # 完了後にパラメータを消してリロード
                            st.query_params.clear()
                            clear_app_cache()
                            time.sleep(2.5) # 完了メッセージを見せるための待機
                            st.rerun()
                        else: st.error("解析に失敗しました。ファイル形式を確認してください。")
                    else: st.error("ファイル名確定に失敗したか、ファイルが見つかりません。")
                except Exception as e: st.error(f"システムエラー: {e}")

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
    else: st.info("取り込み済みのデータはありません。")

with tab_settings:
    st.subheader("⚙️ 管理設定")
    if st.button("🔥 データベース全体の初期化", type="primary"):
        db_manager.reset_dataset()
        clear_app_cache()
        st.rerun()
