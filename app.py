import streamlit as st
import pandas as pd
from aggregator.processor import SalesAggregator
from aggregator.database_bq import DatabaseManager
import io
import datetime
import logging
import os

st.set_page_config(page_title="売上データ管理システム", layout="wide", initial_sidebar_state="expanded")

# --- 環境変数からの自動取得 (Cloud Run 用) ---
default_project_id = os.getenv('GOOGLE_CLOUD_PROJECT', st.session_state.get('project_id', ''))

# --- サイドバー (GCP 設定) ---
st.sidebar.title("☁️ Google Cloud 設定")
project_id = st.sidebar.text_input("Project ID (GCP)", value=default_project_id)
dataset_id = "sales_aggregator_dataset"

if project_id:
    st.session_state['project_id'] = project_id
    db_manager = DatabaseManager(project_id=project_id, dataset_id=dataset_id)
else:
    st.sidebar.warning("⚠️ Project ID を入力して認証してください。")
    st.stop()

@st.cache_data(ttl=600) # BigQuery はコストがかかるため少し長めにキャッシュ
def load_all_data():
    """キャッシュ付きで全データをデータベースから読み込み、正規化まで行う"""
    all_data = db_manager.get_all_data()
    if all_data.empty:
        return all_data

    # 日付形式の補正（正規化されていない過去データへの対応）
    if '_DATE_' in all_data.columns:
        from aggregator.processor import SalesAggregator
        agg = SalesAggregator()
        # まずベクトル化された正規化を適用し、その後に datetime 変換する
        all_data['_DATE_'] = pd.to_datetime(agg._vectorized_normalize_date(all_data['_DATE_']), errors='coerce')
    
    return all_data

# --- サイドバー (フィルター機能) ---
st.sidebar.title("🔍 フィルター設定")

def get_filtered_data():
    all_data = load_all_data()
    if all_data is None or all_data.empty:
        return pd.DataFrame(), []

    # --- シンプルな期間選択 (対象月リスト) ---
    all_data['MONTH_STR'] = all_data['_DATE_'].dt.strftime('%Y-%m').fillna("不明")
    month_list = sorted([m for m in all_data['MONTH_STR'].unique() if m != "不明"], reverse=True)
    
    selected_month = st.sidebar.selectbox("📅 対象月を選択", ["すべて"] + month_list)

    # --- フィルタリング適用 ---
    filtered_df = all_data.copy()
    if selected_month != "すべて":
        filtered_df = filtered_df[filtered_df['MONTH_STR'] == selected_month]
        
    # --- サイドバー追加フィルター ---
    sources = ["すべて"] + sorted(filtered_df['SOURCE'].unique().tolist())
    selected_source = st.sidebar.selectbox("プラットフォーム", sources)
    if selected_source != "すべて":
        filtered_df = filtered_df[filtered_df['SOURCE'] == selected_source]

    artists = ["すべて"] + sorted(filtered_df['_ARTIST_'].dropna().unique().tolist())
    selected_artist = st.sidebar.selectbox("アーティスト", artists)
    if selected_artist != "すべて":
        filtered_df = filtered_df[filtered_df['_ARTIST_'] == selected_artist]

    # --- 表示項目の選択 (カラム選択) ---
    st.sidebar.markdown("---")
    all_possible_cols = filtered_df.columns.tolist()
    selected_cols = st.sidebar.multiselect(
        "📊 表示項目の選択",
        options=all_possible_cols,
        default=all_possible_cols
    )
        
    return filtered_df, selected_cols

# --- メイン画面 ---
st.title("売上データ管理")

tab_data, tab_upload = st.tabs(["📋 売上データ", "📥 新規アップロード"])

filtered_df, selected_cols = get_filtered_data()

with tab_data:
    if filtered_df.empty:
        st.info("データがありません。まずはデータをアップロードしてください。")
    else:
        # KPI メトリクス
        total_revenue = filtered_df['_NET_REVENUE_'].sum()
        total_quantity = filtered_df['_QUANTITY_'].sum()
        unique_artists = filtered_df['_ARTIST_'].nunique()

        m1, m2, m3 = st.columns(3)
        m1.metric("総売上", f"¥{total_revenue:,.0f}")
        m2.metric("総数量", f"{total_quantity:,.0f}")
        m3.metric("アーティスト数", f"{unique_artists}")

        st.divider()
        
        st.write(f"表示件数: {len(filtered_df):,} 件")
        
        # サンプルの表示 (10行ランダム)
        with st.expander("🎲 データサンプル (ランダム10行)", expanded=True):
            sample_df = filtered_df.sample(min(10, len(filtered_df))) if not filtered_df.empty else filtered_df
            st.dataframe(sample_df[selected_cols] if selected_cols else sample_df, use_container_width=True)

        st.divider()
        
        # 全データテーブル表示
        st.write("全データ一覧:")
        display_df = filtered_df[selected_cols] if selected_cols else filtered_df
        st.dataframe(display_df, use_container_width=True)
        
        # CSV/Excelダウンロード
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer) as writer:
            display_df.to_excel(writer, index=False)
        st.download_button("📥 表示中のデータをExcelで保存", buffer.getvalue(), "sales_report.xlsx")

    st.divider()
    with st.expander("詳細管理"):
        if st.button("⚠️ 全データの削除"):
            db_manager.clear_all_data()
            st.cache_data.clear() # キャッシュをクリア
            st.rerun()

with tab_upload:
    st.subheader("新規データの追加")
    uploaded_files = st.file_uploader("レポートファイルをアップロード", accept_multiple_files=True)

    if uploaded_files:
        if st.button("解析・統合を開始", type="primary"):
            with st.spinner("解析中..."):
                aggregator = SalesAggregator()
                try:
                    merged_df = aggregator.process_files(uploaded_files)
                    if merged_df is not None and not merged_df.empty:
                        st.session_state['temp_df'] = merged_df
                        st.session_state['temp_files'] = [f.name for f in uploaded_files]
                    else:
                        st.warning("有効なデータが見つかりませんでした。")
                except Exception as e:
                    st.error(f"エラー: {e}")

    if 'temp_df' in st.session_state:
        df = st.session_state['temp_df']
        fnames = st.session_state['temp_files']
        st.success(f"{len(df):,} 行のデータを検出しました。")
        
        c1, c2 = st.columns(2)
        with c1:
            if st.button("💾 データベースに保存"):
                try:
                    db_manager.save_data(df, fnames, overwrite=True)
                    # キャッシュをクリア
                    st.cache_data.clear()
                    st.success("保存しました。")
                    del st.session_state['temp_df']
                    st.rerun()
                except Exception as e:
                    st.error(f"保存エラー: {e}")
        with c2:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer) as writer:
                df.to_excel(writer, index=False)
            st.download_button("📥 統合データをExcelで保存", buffer.getvalue(), "merged_report.xlsx")
        
        st.write("解析結果プレビュー:")
        st.dataframe(df.head(100), use_container_width=True)
