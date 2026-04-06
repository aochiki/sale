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

tab_view, tab_flexible, tab_upload, tab_settings = st.tabs(["📋 売上データ閲覧", "📊 自由集計", "📥 RAWデータ追加", "⚙️ システム管理"])

# --- 共通データの取得 ---
raw_df = fetch_raw_data(project_id)
mappings = fetch_mappings(project_id)
unified_df = pd.DataFrame()
if not raw_df.empty and not mappings.empty:
    with st.status("🔄 データを動的に統合中...", expanded=False):
        unified_df = processor.unify_raw_records(raw_df, mappings)

# --- 1. 閲覧タブ (動的統合) ---
with tab_view:
    if raw_df.empty:
        st.info("データがありません。RAWデータをアップロードしてください。")
    elif unified_df.empty:
        st.warning("マッピング設定に基づいて統合されたデータがありません。設定を確認してください。")
    else:
        # フィルタリング
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

# --- 2. 自由集計タブ (ピボット) ---
with tab_flexible:
    if unified_df.empty:
        st.info("集計可能なデータがありません。")
    else:
        st.subheader("📊 ダイナミック・ピボットレポート")
        
        # 属性項目と数値項目の抽出
        attr_cols = [m['unified_name'] for _, m in mappings.iterrows() if not m['is_numeric'] and not m['is_date']]
        num_cols = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
        date_col = next((m['unified_name'] for _, m in mappings.iterrows() if m['is_date']), None)
        
        # 期間フィルター
        if date_col:
            months = sorted(unified_df[date_col].dropna().unique().tolist())
            c1, c2 = st.columns(2)
            start_m = c1.selectbox("🚩 開始月", months, index=0)
            end_m = c2.selectbox("🏁 終了月", months, index=len(months)-1)
            
            # フィルタ適用
            flex_df = unified_df[(unified_df[date_col] >= start_m) & (unified_df[date_col] <= end_m)].copy()
        else:
            flex_df = unified_df.copy()
            st.warning("日付項目が定義されていないため、期間絞り込みはスキップされました。")

        # 集計設定
        with st.expander("🛠️ 集計軸の設定", expanded=True):
            st.info("💡 **タテ軸・ヨコ軸**には「アーティスト」や「曲名」などの分類項目を選び、**表示する値**には「売上金額」や「数量」などの数字項目を選んでください。")
            cc1, cc2, cc3 = st.columns(3)
            
            # 軸の選択肢（数値以外を優先）
            axis_options = attr_cols + (['SOURCE'] if 'SOURCE' in unified_df.columns else [])
            row_axis = cc1.selectbox("タテ軸 (行)", axis_options, index=0 if axis_options else None)
            
            col_list = ["(なし)"] + axis_options
            col_axis = cc2.selectbox("ヨコ軸 (列)", col_list, index=0)
            
            # 数値項目のデフォルト選択
            val_cols = cc3.multiselect("表示する値 (集計対象)", num_cols, default=num_cols if num_cols else [])

        if not val_cols:
            st.warning("⚠️ **「表示する値」** を 1 つ以上選択してください（例：売上金額、数量）。")
        else:
            try:
                # ピボットテーブルの生成
                # もしユーザーが「数量」などを軸に選んでしまった場合（mappingsの設定ミス等）への考慮
                p_cols = col_axis if col_axis != "(なし)" else None
                
                with st.spinner("集計中..."):
                    pivot_res = flex_df.pivot_table(
                        index=row_axis,
                        columns=p_cols,
                        values=val_cols,
                        aggfunc='sum',
                        margins=True,
                        margins_name="合計"
                    )
                    
                    # 見栄えの調整: 数値をカンマ区切りに
                    st.write(f"### 📋 集計結果: {row_axis} " + (f"× {col_axis}" if p_cols else ""))
                    st.dataframe(pivot_res.style.format("{:,.0f}"), use_container_width=True)
                
            except Exception as e:
                st.error(f"集計エラー: {e}")
                st.info("選択した項目の組み合わせで集計できませんでした。軸を変更してみてください。")


# --- 3. アップロードタブ (GCS統合) ---
with tab_upload:
    st.subheader("📥 データファイルのアップロード")
    st.caption("ファイルサイズの制限なし — ドラッグ＆ドロップでクラウドに直接送信されます。")

    # すでにアップロード済みのファイル名リストを取得
    all_raw = raw_df
    existing_filenames = set(all_raw['filename'].unique()) if not all_raw.empty else set()

    # --- クエリパラメータからファイル名を自動取得（Phase 2） ---
    qp = st.query_params
    pending_file = qp.get("upload_file", "")

    if pending_file:
        # Phase 2: ファイル名が確定 → 署名URL生成 → 自動アップロード
        st.markdown(f"#### 📤 アップロード中: `{pending_file}`")
        if pending_file in existing_filenames:
            st.warning(f"⚠️ 「{pending_file}」は既に取り込み済みです。インポート時に上書きされます。")
        try:
            signed_url = db_manager.get_gcs_signed_url(pending_file)
            auto_upload_html = f"""
            <div id="drop-zone" style="
                font-family: 'Segoe UI', sans-serif;
                border: 2px dashed #007bff;
                border-radius: 12px;
                background: linear-gradient(135deg, #e8f0fe 0%, #d0e2ff 100%);
                padding: 28px 20px;
                text-align: center;
                cursor: pointer;
                transition: all 0.3s ease;
            ">
                <div id="upload-icon" style="font-size: 2.5rem; margin-bottom: 8px;">📂</div>
                <div id="upload-label" style="font-size: 1rem; font-weight: 600; color: #333; margin-bottom: 4px;">
                    「{pending_file}」をもう一度選択してください
                </div>
                <div id="upload-hint" style="font-size: 0.8rem; color: #888;">クリックしてファイルを選ぶと自動でアップロードが始まります</div>
                <div id="progress-wrap" style="width: 90%; margin: 16px auto 0; display: none;">
                    <div style="background: #e0e0e0; border-radius: 6px; overflow: hidden; height: 8px;">
                        <div id="prog-bar" style="width:0%; height:100%; background: linear-gradient(90deg,#007bff,#00b4d8); transition: width .15s;"></div>
                    </div>
                    <p id="prog-text" style="font-size: 0.82rem; color: #555; margin-top: 6px;">0%</p>
                </div>
                <input type="file" id="file-pick" style="display:none;" autocomplete="off">
            </div>
            <script>
            (function() {{
                const zone = document.getElementById('drop-zone');
                const pick = document.getElementById('file-pick');
                const icon = document.getElementById('upload-icon');
                const label = document.getElementById('upload-label');
                const hint = document.getElementById('upload-hint');
                const wrap = document.getElementById('progress-wrap');
                const bar  = document.getElementById('prog-bar');
                const txt  = document.getElementById('prog-text');

                zone.addEventListener('click', () => pick.click());
                zone.addEventListener('dragover', (e) => {{ e.preventDefault(); zone.style.borderColor='#007bff'; }});
                zone.addEventListener('dragleave', () => {{ zone.style.borderColor='#a0c4ff'; }});
                zone.addEventListener('drop', (e) => {{
                    e.preventDefault();
                    if (e.dataTransfer.files.length) {{ pick.files = e.dataTransfer.files; pick.dispatchEvent(new Event('change')); }}
                }});

                pick.onchange = () => {{
                    const file = pick.files[0];
                    if (!file) return;
                    icon.innerText = '⏳';
                    label.innerText = file.name + '  (' + (file.size/1024/1024).toFixed(1) + ' MB)';
                    hint.innerText = '送信中...';
                    wrap.style.display = 'block';
                    zone.style.cursor = 'default';
                    zone.onclick = null;
                    const xhr = new XMLHttpRequest();
                    xhr.open('PUT', '{signed_url}', true);
                    xhr.setRequestHeader('Content-Type', file.type || 'application/octet-stream');
                    xhr.upload.onprogress = (ev) => {{
                        if (ev.lengthComputable) {{
                            const pct = Math.round(ev.loaded / ev.total * 100);
                            bar.style.width = pct + '%';
                            txt.innerText = pct + '%  (' + (ev.loaded/1024/1024).toFixed(1) + ' / ' + (ev.total/1024/1024).toFixed(1) + ' MB)';
                        }}
                    }};
                    xhr.onload = () => {{
                        if (xhr.status === 200) {{
                            icon.innerText = '✅';
                            label.innerText = 'アップロード完了！';
                            hint.innerText = '画面を更新して「② 取り込む」へ進んでください。';
                            hint.style.color = '#28a745';
                            bar.style.background = 'linear-gradient(90deg,#28a745,#5cb85c)';
                        }} else {{
                            icon.innerText = '❌';
                            label.innerText = 'エラー (HTTP ' + xhr.status + ')';
                            hint.innerText = xhr.responseText || 'アップロードに失敗しました。';
                            hint.style.color = '#dc3545';
                        }}
                    }};
                    xhr.onerror = () => {{
                        icon.innerText = '❌';
                        label.innerText = 'ネットワークエラー';
                        hint.style.color = '#dc3545';
                    }};
                    xhr.send(file);
                }};
            }})();
            </script>
            """
            components.html(auto_upload_html, height=200)
        except Exception as e:
            st.error(f"アップロード準備エラー: {e}")
        
        if st.button("🔄 完了したら画面を更新"):
            st.query_params.clear()
            st.rerun()
    else:
        # Phase 1: ファイルを選択 → ファイル名を取得してリロード
        phase1_html = """
        <div id="drop-zone" style="
            font-family: 'Segoe UI', sans-serif;
            border: 2px dashed #a0c4ff;
            border-radius: 12px;
            background: linear-gradient(135deg, #f0f7ff 0%, #e8f0fe 100%);
            padding: 36px 20px;
            text-align: center;
            cursor: pointer;
            transition: all 0.3s ease;
        " onmouseover="this.style.borderColor='#007bff'; this.style.background='linear-gradient(135deg, #e8f0fe 0%, #d0e2ff 100%)';"
          onmouseout="this.style.borderColor='#a0c4ff'; this.style.background='linear-gradient(135deg, #f0f7ff 0%, #e8f0fe 100%)';">
            <div style="font-size: 2.8rem; margin-bottom: 10px;">📂</div>
            <div style="font-size: 1.05rem; font-weight: 600; color: #333; margin-bottom: 6px;">
                ここにファイルをドラッグ＆ドロップ
            </div>
            <div style="font-size: 0.85rem; color: #888;">またはクリックしてファイルを選択（サイズ制限なし）</div>
            <input type="file" id="file-pick" style="display:none;" autocomplete="off">
        </div>
        <script>
        (function() {
            const zone = document.getElementById('drop-zone');
            const pick = document.getElementById('file-pick');
            zone.addEventListener('click', () => pick.click());
            zone.addEventListener('dragover', (e) => { e.preventDefault(); zone.style.borderColor='#007bff'; });
            zone.addEventListener('dragleave', () => { zone.style.borderColor='#a0c4ff'; });
            zone.addEventListener('drop', (e) => {
                e.preventDefault();
                zone.style.borderColor='#a0c4ff';
                if (e.dataTransfer.files.length) { pick.files = e.dataTransfer.files; pick.dispatchEvent(new Event('change')); }
            });
            pick.onchange = () => {
                const file = pick.files[0];
                if (!file) return;
                // ファイル名をクエリパラメータにセットしてStreamlitをリロード
                const url = new URL(window.parent.location.href);
                url.searchParams.set('upload_file', file.name);
                window.parent.location.href = url.toString();
            };
        })();
        </script>
        """
        components.html(phase1_html, height=200)

    st.divider()

    # --- ステップ2: GCSからBigQueryへ取り込み ---
    st.markdown("#### ② データベースに取り込む")
    gcs_blobs = db_manager.list_gcs_files()
    if gcs_blobs:
        st.caption("アップロードされたファイルを解析し、BigQuery に保存します。完了後、ストレージからは自動で削除されます。")

        # 一括取り込みボタン
        if len(gcs_blobs) > 1:
            if st.button("🚀 すべてまとめて取り込む", type="primary"):
                rules = fetch_rules(project_id)
                ok_count = 0
                with st.status("一括処理中...") as batch_st:
                    for blob in gcs_blobs:
                        try:
                            blob_io = db_manager.get_gcs_blob_io(blob['name'])
                            df = processor.parse_raw_only(blob_io, rules=rules)
                            if df is not None:
                                s_type = processor.detect_source(blob['name'])
                                db_manager.save_raw_data(df, blob['name'], s_type, overwrite=True)
                                db_manager.delete_gcs_file(blob['name'])
                                ok_count += 1
                        except Exception as e:
                            st.error(f"エラー ({blob['name']}): {e}")
                    batch_st.update(label=f"✅ {ok_count} 件を取り込みました", state="complete")
                clear_app_cache()
                time.sleep(1)
                st.rerun()

        for blob in gcs_blobs:
            with st.container(border=True):
                c1, c2, c3 = st.columns([3, 1, 1])
                size_mb = blob['size'] / 1024 / 1024 if blob['size'] else 0
                c1.write(f"📦 **{blob['name']}**  ({size_mb:.1f} MB)")
                if c2.button("🚀 取り込む", key=f"imp_{blob['name']}"):
                    with st.status(f"{blob['name']} を処理中...") as imp_st:
                        try:
                            blob_io = db_manager.get_gcs_blob_io(blob['name'])
                            rules = fetch_rules(project_id)
                            df = processor.parse_raw_only(blob_io, rules=rules)
                            if df is not None:
                                s_type = processor.detect_source(blob['name'])
                                row_count = db_manager.save_raw_data(df, blob['name'], s_type, overwrite=True)
                                db_manager.delete_gcs_file(blob['name'])
                                imp_st.update(label=f"✅ {blob['name']} ({row_count:,}件)", state="complete")
                                st.toast(f"取り込み完了: {blob['name']}", icon="✅")
                                clear_app_cache()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("解析に失敗しました。")
                        except Exception as e:
                            st.error(f"エラー: {e}")
                if c3.button("🗑️ 削除", key=f"delg_{blob['name']}"):
                    db_manager.delete_gcs_file(blob['name'])
                    st.rerun()
    else:
        st.info("💡 ①でアップロードしたファイルがここに表示されます。")

    st.divider()

    # --- 取り込み済みデータ一覧 ---
    st.markdown("#### 📋 取り込み済みデータ")
    if not all_raw.empty:
        agg_dict = {'source_type': 'first', 'row_index': 'count'}
        has_created_at = 'created_at' in all_raw.columns
        if has_created_at:
            agg_dict['created_at'] = 'max'
        file_summary = all_raw.groupby('filename').agg(agg_dict).reset_index()
        if has_created_at:
            file_summary = file_summary.sort_values('created_at', ascending=False)
        else:
            file_summary = file_summary.sort_values('filename')

        for i, row in file_summary.iterrows():
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
                c1.write(f"📄 **{row['filename']}**")
                c2.write(f"🏷️ {row['source_type']}")
                c3.write(f"📊 {row['row_index']:,} 件")
                if c4.button("🗑️ 削除", key=f"del_{row['filename']}_{i}"):
                    with st.spinner(f"{row['filename']} を削除中..."):
                        if db_manager.delete_raw_data(row['filename']):
                            st.toast(f"削除しました: {row['filename']}", icon="🗑️")
                            clear_app_cache()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"削除に失敗しました: {row['filename']}")
    else:
        st.info("取り込み済みのデータはありません。")

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
