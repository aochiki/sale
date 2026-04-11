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
def fetch_unified_data(project_id):
    return get_db(project_id).get_unified_data()

@st.cache_data(ttl=60)
def fetch_mappings(project_id):
    return get_db(project_id).get_unified_columns()

@st.cache_data(ttl=60)
def fetch_rules(project_id):
    return get_db(project_id).get_parsing_rules()

def clear_app_cache():
    st.cache_data.clear()
    st.cache_resource.clear()

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
processor = None # Unused now
rules = pd.DataFrame()
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
    unified_df = fetch_unified_data(project_id)
    mappings = fetch_mappings(project_id)
    rules = fetch_rules(project_id)

    # 初回マッピング定義がない場合のデフォルト作成
    if mappings.empty:
        default_cols = [
            # 基本項目
            "売上確定日", "利用発生月", "アーティスト名", "楽曲名", "アルバム名",
            # コード類
            "ISRC", "UPC_EAN", "ベンダー識別子",
            # 配信先
            "配信サービス名", "国コード", "レーベル名",
            # 実績・属性
            "コース_プラン名", "数量", "印税額", "通貨", "空間オーディオ判定", "オフライン再生フラグ", "販売種別"
        ]
        mappings = pd.DataFrame({
            "unified_name": default_cols,
            "orchard_col": ["" for _ in default_cols],
            "nextone_col": ["" for _ in default_cols],
            "itunes_col": ["" for _ in default_cols],
            "is_date": [c in ["売上確定日", "利用発生月"] for c in default_cols],
            "is_numeric": [c in ["数量", "印税額"] for c in default_cols]
        })

# --- 1. 閲覧タブ ---
with tab_view:
    if not project_id:
        st.info("💡 「⚙️ システム管理」タブで GCP Project ID を設定してください。")
        st.stop()
    
    if unified_df.empty:
        st.info("データがありません。RAWデータをアップロードしてください。")
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
        
    st.subheader("📥 売上データのアップロード")
    st.caption("ファイルをアップロードして共通フォーマットへ自動整形・登録します。")

    # --- Standard Uploader (Primary/Stable) ---
    uploaded_file = st.file_uploader("ファイルを選択してください (CSV/TSV/Apple Musicレポート)", type=["csv", "tsv", "txt"])
    if uploaded_file:
        if st.button("🚀 登録を開始する", type="primary", use_container_width=True):
            with st.status("⌛ 整形・保存中...") as stat:
                try:
                    formatter = DataFormatter(mappings)
                    df = formatter.format_file(uploaded_file, uploaded_file.name)
                    if df is not None and not df.empty:
                        db_manager.save_unified_data(df, uploaded_file.name, overwrite=True)
                        stat.update(label=f"✅ {uploaded_file.name} を登録しました", state="complete")
                        clear_app_cache()
                        time.sleep(1); st.rerun()
                    else:
                        stat.update(label="❌ 解析失敗。フォーマットを確認してください。", state="error")
                except Exception as e:
                    st.error(f"処理中にエラーが発生しました: {e}")
    
    st.divider()

    # --- Large File Uploader (Advanced/Signed-URL flow) ---
    with st.expander("🐘 大容量ファイル(100MB〜1GB以上)のアップロード用 (設定が必要)", expanded=False):
        st.info("※ブラウザから直接GCSへ高速アップロードします。ローカル環境では権限エラーになる場合があります。")
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
                xhr.open('PUT', '${data_signed_url}');
                xhr.setRequestHeader('Content-Type', 'application/octet-stream');
                xhr.upload.onprogress=e=>{{
                    const p=Math.round(e.loaded/e.total*100);
                    bar.style.width=p+'%';
                }};
                xhr.onload=async ()=>{{
                    if(xhr.status===200) {{
                        status.innerText = '本体完了。ファイル名を記録中...';
                        const tagXhr = new XMLHttpRequest();
                        tagXhr.open('PUT', '${tag_signed_url}');
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
            st.warning(f"大容量アップローダーは現在利用できません (権限設定が必要です)")
            logging.error(f"Signed URL Error: {e}")

    if st.button("🚀 BigQueryへの登録を開始する", type="primary", use_container_width=True):
        with st.status("⌛ 処理中...") as stat:
            try:
                tag_io = db_manager.get_gcs_blob_io(temp_tag_path)
                if not tag_io:
                    st.warning("アップロードが完了していません。")
                else:
                    detected_fn = tag_io.read().decode('utf-8').strip()
                    blob_io = db_manager.get_gcs_blob_io(temp_data_path)
                    
                    formatter = DataFormatter(mappings)
                    df = formatter.format_file(blob_io, detected_fn)
                    
                    if df is not None and not df.empty:
                        db_manager.save_unified_data(df, detected_fn, overwrite=True)
                        db_manager.delete_gcs_file(temp_data_path)
                        db_manager.delete_gcs_file(temp_tag_path)
                        stat.update(label=f"✅ {detected_fn} を登録しました", state="complete")
                        clear_app_cache()
                        time.sleep(1); st.rerun()
                    else: stat.update(label="❌ 解析・整形失敗", state="error")
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
                    db_manager.delete_unified_data(h['filename'])
                    clear_app_cache(); st.rerun()

with tab_settings:
    st.subheader("⚙️ システム管理")
    st.info("APIキー接続や、ファイルアップロード時のマッピング（列名の変換辞書）管理を行います。")
    
    new_api_key = st.text_input("Gemini API Key", value=gemini_api_key, type="password", autocomplete="new-password")
    if st.button("💾 APIキーを保存"):
        st.session_state.gemini_api_key = new_api_key.strip()
        st.rerun()
        
    st.divider()
    st.subheader("🗺️ マッピング・ルール定義")
    st.markdown("アップロード時、指定された**【各社ヘッダー】**の内容を**【統合カラム】**にマッピングします。")
    st.caption("アップロードファイル内の元の列名が設定と完全に一致している必要があります。")
    
    edited_mappings = st.data_editor(
        mappings,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "unified_name": st.column_config.TextColumn("統合カラム", required=True),
            "orchard_col": st.column_config.TextColumn("ORCHARD ヘッダー"),
            "nextone_col": st.column_config.TextColumn("NexTone ヘッダー"),
            "itunes_col": st.column_config.TextColumn("Apple ヘッダー"),
            "is_date": st.column_config.CheckboxColumn("日付型", default=False),
            "is_numeric": st.column_config.CheckboxColumn("数値型", default=False)
        }
    )
    
    if st.button("💾 マッピング定義を保存", type="primary"):
        db_manager.save_unified_columns_batch(edited_mappings)
        clear_app_cache()
        st.success("マッピングを更新しました。")
        time.sleep(1); st.rerun()

    if st.button("🔄 設定を最新の状態に更新する (キャッシュクリア)"):
        clear_app_cache()
        st.success("最新の設定を読み込みました。")
        time.sleep(1); st.rerun()

    st.divider()
    st.warning("⚠️ 危険な操作")
    if st.button("💣 データベースリセット (売上データのみ削除)"):
        db_manager.reset_dataset()
        clear_app_cache()
        st.success("売上データをリセットしました（マッピング設定は維持されます）")
        time.sleep(1); st.rerun()
