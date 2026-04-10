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
    page_title="å£²ä¸ãã¼ã¿çµ±åã·ã¹ãã  (AI & Auto-Upload)", 
    page_icon="ð",
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

st.title("ð å£²ä¸ãã¼ã¿ç®¡çã·ã¹ãã ")
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
    "ð å£²ä¸ãã¼ã¿é²è¦§", "ð èªç±éè¨", "ð¤ AIéè¨", "ð¥ RAWãã¼ã¿è¿½å ", "âï¸ ã·ã¹ãã ç®¡ç"
])

# --- å±éãã¼ã¿ã®åå¾ ---
if project_id:
    db_manager = get_db(project_id)
    raw_df = fetch_raw_data(project_id)
    mappings = fetch_mappings(project_id)
    rules = fetch_rules(project_id)

    if not raw_df.empty and not mappings.empty:
        with st.status("ð ãã¼ã¿ãåçã«çµ±åä¸­...", expanded=False):
            unified_df = processor.unify_raw_records(raw_df, mappings)

# --- 1. é²è¦§ã¿ã ---
with tab_view:
    if not project_id:
        st.info("ð¡ ãâï¸ ã·ã¹ãã ç®¡çãã¿ãï¼ä¸çªä¸ï¼ã§ GCP Project ID ãè¨­å®ãã¦ãã ããã")
        st.stop()
    
    if raw_df.empty:
        st.info("ãã¼ã¿ãããã¾ãããRAWãã¼ã¿ãã¢ããã­ã¼ããã¦ãã ããã")
    elif unified_df.empty:
        st.warning("ãããã³ã°è¨­å®ã«åºã¥ãã¦çµ±åããããã¼ã¿ãããã¾ãããè¨­å®ãç¢ºèªãã¦ãã ããã")
    else:
        # ç°¡æãã£ã«ã¿
        c1, c2 = st.columns(2)
        month_col = next((c for c in unified_df.columns if not mappings.empty and mappings[mappings['unified_name']==c]['is_date'].any()), None)
        month_list = ["ãã¹ã¦"] + sorted(unified_df[month_col].dropna().unique().tolist(), reverse=True) if month_col else ["ãã¹ã¦"]
        sel_m = c1.selectbox("ð å¯¾è±¡æ", month_list)
        sel_s = c2.selectbox("ð ã½ã¼ã¹", ["ãã¹ã¦"] + sorted(unified_df['SOURCE'].unique().tolist()))
        
        filtered = unified_df.copy()
        if sel_m != "ãã¹ã¦": filtered = filtered[filtered[month_col] == sel_m]
        if sel_s != "ãã¹ã¦": filtered = filtered[filtered['SOURCE'] == sel_s]
        
        st.dataframe(filtered, use_container_width=True, hide_index=True)
        st.download_button("ð¥ ãã¼ã¿ãCSVã¨ãã¦ãã¦ã³ã­ã¼ã", filtered.to_csv(index=False), f"unified_{datetime.datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

# --- 2. èªç±éè¨ã¿ã ---
with tab_flexible:
    if not project_id:
        st.info("ð¡ ãâï¸ ã·ã¹ãã ç®¡çãã¿ãï¼ä¸çªä¸ï¼ã§ GCP Project ID ãè¨­å®ãã¦ãã ããã")
        st.stop()
        
    if unified_df.empty:
        st.info("éè¨å¯è½ãªãã¼ã¿ãããã¾ããã")
    else:
        st.subheader("ð ãããããã¼ãã«éè¨")
        attr_cols = [m['unified_name'] for _, m in mappings.iterrows() if not m['is_numeric'] and not m['is_date']]
        num_cols = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
        
        c1, c2, c3 = st.columns(3)
        row_axis = c1.selectbox("ç¸¦è»¸ (è¡)", [None] + attr_cols + ['SOURCE'])
        col_axis = c2.selectbox("æ¨ªè»¸ (å)", [None] + attr_cols + ['SOURCE'])
        val_axis = c3.selectbox("éè¨å¤", num_cols if num_cols else [None])
        
        if val_axis:
            try:
                pivot_df = unified_df.pivot_table(
                    index=row_axis, columns=col_axis, values=val_axis,
                    aggfunc='sum', margins=True, margins_name="åè¨"
                )
                st.dataframe(pivot_df.style.format("{:,.0f}"), use_container_width=True)
            except:
                st.info("é¸æããé ç®ã®çµã¿åããã§éè¨ã§ãã¾ããã§ããã")

# --- 3. AIéè¨ã¿ã ---
with tab_ai:
    if not project_id:
        st.info("ð¡ ãâï¸ ã·ã¹ãã ç®¡çãã¿ãï¼ä¸çªä¸ï¼ã§ GCP Project ID ãè¨­å®ãã¦ãã ããã")
        st.stop()

    if unified_df.empty:
        st.info("éè¨å¯è½ãªãã¼ã¿ãããã¾ããã")
    else:
        st.subheader("ð¤ èªç¶è¨èªã«ããAIéè¨")
        st.caption("ããããã58ã®æ²ã                with st.spinner("AIãæå³ãè§£æä¸­..."):
                    # 1. ã«ã©ã æå ±ã®æ½åº
                    attr_cols_ai = [m['unified_name'] for _, m in mappings.iterrows() if not m['is_numeric'] and not m['is_date']]
                    num_cols_ai = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
                    all_cols = attr_cols_ai + (['SOURCE'] if 'SOURCE' in unified_df.columns else [])
                    
                    # 2. AIã¸ã®åãåãã
                    gemini_key = st.session_state.get('gemini_api_key')
                    if not gemini_key:
                        st.error("Gemini APIã­ã¼ãè¨­å®ããã¦ãã¾ããã")
                        st.stop()
                    
                    parsed = parse_natural_language_query(project_id, user_query, all_cols, num_cols_ai, api_key=gemini_key)
                
                # 3. è§£æçµæã®å¦ç (ã¹ããã¼ã®å¤)
                if not parsed:
                    st.error("AIããã®å¿ç­ãããã¾ããã§ããã")
                elif "error" in parsed:
                    st.error(f"AIè§£æã¨ã©ã¼: {parsed['error']}")
                else:
                    with st.expander("ð AIã®è§£æçµæãç¢ºèª"): st.json(parsed)
                    
                    try:
                        f_df = flex_df_ai.copy()
                        # 4. ãã£ã«ã¿é©ç¨
                        filters = parsed.get("filters", {})
                        if filters:
                            for col, val in filters.items():
                                if col in f_df.columns:
                                    f_df = f_df[f_df[col].astype(str).str.contains(str(val), na=False, case=False)]
                        
                        # 5. è»¸ã¨å¤ã®æ±ºå®
                        def clean(a):
                            if isinstance(a, list): return [str(i).strip() for i in a if i]
                            return str(a).strip() if a else None

                        r_axis = clean(parsed.get("row_axis"))
                        c_axis = clean(parsed.get("col_axis"))
                        v_axis = [str(v).strip() for v in parsed.get("value_axis", []) if v]
                        
                        if not v_axis and num_cols_ai: v_axis = [num_cols_ai[0]]
                        
                        # ã«ã©ã å­å¨ãã§ãã¯
                        def check(cols, df):
                            if not cols: return []
                            c_list = cols if isinstance(cols, list) else [cols]
                            return [x for x in c_list if x not in df.columns]

                        missing = check(r_axis, f_df) + check(c_axis, f_df) + check(v_axis, f_df)
                        
                        if missing:
                            st.warning(f"é ç®ãè¦ã¤ããã¾ãã: {', '.join(missing)}")
                            st.info(f"å©ç¨å¯è½ãªé ç®: {', '.join(f_df.columns)}")
                        elif not v_axis:
                            st.warning("éè¨å¯¾è±¡ã®æ°å¤é ç®ãæå®ããã¦ãã¾ããã")
                        else:
                            # 6. éè¨ã¨è¡¨ç¤º
                            if not r_axis and not c_axis:
                                st.write("### ð åè¨çµæ")
                                st.dataframe(f_df[v_axis].sum().to_frame(name='åè¨').style.format("{:,.0f}"))
                            else:
                                pivot_res = f_df.pivot_table(index=r_axis, columns=c_axis, values=v_axis, aggfunc='sum', margins=True, margins_name="åè¨")
                                st.dataframe(pivot_res.style.format("{:,.0f}"), use_container_width=True)
                                
                    except Exception as e:
                        st.error(f"éè¨å¦çä¸­ã«ã¨ã©ã¼ãçºçãã¾ãã: {e}")
                        st.exception(e)
                 if not cols: return []
                                    c_list = cols if isinstance(cols, list) else [cols]
                                    return [x for x in c_list if x not in df_cols]

                                missing = check_cols(r, f_df.columns) + check_cols(c, f_df.columns) + check_cols(vs, f_df.columns)
                                if missing:
                                    st.warning(f"ä»¥ä¸ã®é ç®ããã¼ã¿ã«è¦ã¤ããã¾ãã: {', '.join(missing)}")
                                    st.info(f"å©ç¨å¯è½ãªé ç®: {', '.join(f_df.columns.tolist())}")
                                elif not vs:
                                    st.warning("éè¨å¯¾è±¡ï¼æ°å¤ï¼ãè¦ã¤ããã¾ããã")
                                else:
                                    pivot_res = f_df.pivot_table(index=r, columns=c, values=vs, aggfunc='sum', margins=True, margins_name="åè¨")
                                    st.dataframe(pivot_res.style.format("{:,.0f}"), use_container_width=True)
                            except Exception as e: st.error(f"éè¨ã¨ã©ã¼: {e}")
                    else: st.error("AIè§£æã«å¤±æãã¾ããã(ã¬ã¹ãã³ã¹ãç©ºã§ã)")

# --- 4. RAWãã¼ã¿è¿½å  (V3æ¹å¼ å¾©åç) ---
with tab_upload:
    if not project_id:
        st.info("ð¡ ãâï¸ ã·ã¹ãã ç®¡çãã¿ãï¼ä¸çªä¸ï¼ã§ GCP Project ID ãè¨­å®ãã¦ãã ããã")
        st.stop()
        
    st.subheader("ð¥ å¤§å®¹éãã¼ã¿ã®ã¢ããã­ã¼ã")
    st.caption("1. ãã¡ã¤ã«ãæ åã«ãã­ãã â 2. éä¿¡å®äºå¾ãä¸ã®ãã¿ã³ãæ¼ãã¦ç»é²")

    # ã»ãã·ã§ã³ãã¨ã«åºå®ã®ãã¬ãã£ãã¯ã¹
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
            <div id="status" style="font-weight:600; color:#475569; font-family:sans-serif;">ããã«ãã¡ã¤ã«ããã­ãã</div>
            <div id="bar-wrap" style="display:none; margin:15px auto; width:80%; background:#e2e8f0; height:8px; border-radius:4px; overflow:hidden;">
                <div id="bar" style="width:0%; height:100%; background:#3b82f6; transition:width .2s;"></div>
            </div>
            <div id="hint" style="font-size:0.8rem; color:#94a3b8; margin-top:10px; font-family:sans-serif;">(èªåã§ãã¡ã¤ã«åãèªè­ãã¾ãã»1GBã¾ã§å¯¾å¿)</div>
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
            status.innerText = file.name + ' ãéä¿¡ä¸­...';
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
                    status.innerText = 'æ¬ä½å®äºããã¡ã¤ã«åãè¨é²ä¸­...';
                    const tagXhr = new XMLHttpRequest();
                    tagXhr.open('PUT', '{tag_signed_url}');
                    tagXhr.setRequestHeader('Content-Type', 'application/octet-stream');
                    tagXhr.onload = () => {{
                        if (tagXhr.status === 200) {{
                            status.innerText = 'â éä¿¡å®äºï¼ã' + file.name + 'ãã®ç»é²æºåãæ´ãã¾ãã';
                            wrap.style.display='none';
                        }} else {{ status.innerText = 'ã¨ã©ã¼: ' + tagXhr.status; }}
                    }};
                    tagXhr.send(file.name);
                }} else {{ status.innerText='éä¿¡ã¨ã©ã¼: ' + xhr.status; }}
            }};
            xhr.send(file);
        }}
        </script>
        """
        components.html(upload_html, height=200)
    except Exception as e:
        st.error(f"ç½²åä»ãURLã®åå¾ã«å¤±æãã¾ãã: {e}")

    # éè¤ãã§ãã¯ã®ç¢ºèª
    if 'dup_target' in st.session_state:
        target = st.session_state.dup_target
        st.warning(f"â ï¸ {target} ã¯æ¢ã«ç»é²ããã¦ãã¾ããä¸æ¸ããã¾ããï¼")
        c1, c2 = st.columns(2)
        if c1.button("ð¥ ä¸æ¸ããã¦ç»é²", type="primary", use_container_width=True):
            with st.status("ð ä¸æ¸ãç»é²ä¸­...") as force_stat:
                try:
                    blob_io = db_manager.get_gcs_blob_io(temp_data_path)
                    df = processor.parse_raw_only(blob_io, rules=rules)
                    if df is not None:
                        db_manager.save_raw_data(df, target, processor.detect_source(target), overwrite=True)
                        db_manager.delete_gcs_file(temp_data_path)
                        db_manager.delete_gcs_file(temp_tag_path)
                        del st.session_state.dup_target
                        force_stat.update(label=f"â {target} ãä¸æ¸ãç»é²ãã¾ãã", state="complete")
                        clear_app_cache()
                        time.sleep(1); st.rerun()
                except Exception as e: st.error(f"ã¨ã©ã¼: {e}")
        if c2.button("ð« ã­ã£ã³ã»ã«", use_container_width=True):
            del st.session_state.dup_target; st.rerun()
        st.stop()

    if st.button("ð BigQueryã¸ã®ç»é²ãéå§ãã", type="primary", use_container_width=True):
        with st.status("â æºåãç¢ºèªä¸­...") as stat:
            try:
                tag_io = db_manager.get_gcs_blob_io(temp_tag_path)
                if not tag_io:
                    st.warning("ã¢ããã­ã¼ããå®äºãã¦ãã¾ããã")
                    st.stop()
                detected_fn = tag_io.read().decode('utf-8').strip()
                
                # å±¥æ­´åå¾
                all_history = db_manager.get_file_history()
                if not all_history.empty and (detected_fn in all_history['filename'].values):
                    st.session_state.dup_target = detected_fn
                    st.rerun()

                stat.update(label=f"ð¦ {detected_fn} ãå¦çä¸­...")
                blob_io = db_manager.get_gcs_blob_io(temp_data_path)
                df = processor.parse_raw_only(blob_io, rules=rules)
                if df is not None:
                    db_manager.save_raw_data(df, detected_fn, processor.detect_source(detected_fn), overwrite=True)
                    db_manager.delete_gcs_file(temp_data_path)
                    db_manager.delete_gcs_file(temp_tag_path)
                    stat.update(label=f"â {detected_fn} ãç»é²ãã¾ãã", state="complete")
                    clear_app_cache()
                    time.sleep(1); st.rerun()
                else: stat.update(label="â è§£æå¤±æãå½¢å¼ãç¢ºèªãã¦ãã ããã", state="error")
            except Exception as e: st.error(f"ã¨ã©ã¼: {e}")

    st.divider()
    st.markdown("#### ð åãè¾¼ã¿æ¸ã¿å±¥æ­´ (ææ°10ä»¶)")
    history_df = db_manager.get_file_history()
    if not history_df.empty:
        for _, h in history_df.head(10).iterrows():
            with st.container(border=True):
                c1, c2, c3 = st.columns([4, 2, 1])
                c1.write(f"ð **{h['filename']}**")
                c2.caption(f"ð {h['row_count']:,} ä»¶ | ð {h['uploaded_at']}")
                if c3.button("ðï¸ åé¤", key=f"del_h_{h['filename']}"):
                    db_manager.delete_raw_data(h['filename'])
                    clear_app_cache(); st.rerun()

# --- 5. ç®¡çã¿ã (V3/V4 ãããã³ã°ç®¡ç) ---
with tab_settings:
    st.subheader("âï¸ ã·ã¹ãã ç®¡ç")
    
    # ãããã³ã°ç®¡ç
    st.markdown("#### ð çµ±åãããã³ã°å®ç¾©")
    orch_cols = db_manager.get_headers_by_pattern("Orchard%")
    next_cols = db_manager.get_headers_by_pattern("DivSiteAll%")
    itunes_cols = db_manager.get_headers_by_pattern("%_ZZ%")
    
    if 'edit_mapping' not in st.session_state: st.session_state.edit_mapping = None
    edit_data = st.session_state.edit_mapping

    with st.form("mapping_form"):
        u_name = st.text_input("çµ±åé ç®å", value=edit_data['unified_name'] if edit_data else "")
        c1, c2, c3 = st.columns(3)
        def get_idx(lst, val):
            try: return ([""] + lst).index(val)
            except: return 0
        
        o_col = c1.selectbox("Orchard å", [""] + (orch_cols or []), index=get_idx(orch_cols, edit_data['orchard_col']) if edit_data else 0)
        n_col = c2.selectbox("NexTone å", [""] + (next_cols or []), index=get_idx(next_cols, edit_data['nextone_col']) if edit_data else 0)
        i_col = c3.selectbox("iTunes å", [""] + (itunes_cols or []), index=get_idx(itunes_cols, edit_data['itunes_col']) if edit_data else 0)
        
        is_d = st.checkbox("æ¥ä»é ç®", value=edit_data['is_date'] if edit_data else False)
        is_n = st.checkbox("æ°å¤é ç®", value=edit_data['is_numeric'] if edit_data else False)
        
        if st.form_submit_button("ð¾ ãããã³ã°ãä¿å­"):
            if u_name:
                db_manager.save_unified_column(u_name, o_col, n_col, i_col, is_d, is_n)
                st.session_state.edit_mapping = None
                clear_app_cache(); st.rerun()

    cur_mappings = fetch_mappings(project_id)
    for i, m in cur_mappings.iterrows():
        with st.container(border=True):
            ct, ce, cd = st.columns([4, 1, 1])
            ct.write(f"ð **{m['unified_name']}** (O:{m['orchard_col']} / N:{m['nextone_col']} / I:{m['itunes_col']})")
            if ce.button("ð ç·¨é", key=f"ed_{i}"):
                st.session_state.edit_mapping = m.to_dict(); st.rerun()
            if cd.button("ðï¸ åé¤", key=f"dl_{i}"):
                db_manager.delete_unified_column(m['unified_name'])
                clear_app_cache(); st.rerun()

    st.divider()
    # ã«ã¼ã«ç®¡ç
    st.markdown("#### ð è§£æã«ã¼ã«ã®è¨­å®")
    with st.form("rule_add"):
        c1, c2, c3 = st.columns([3, 1, 1])
        pat = c1.text_input("ãã¡ã¤ã«åã­ã¼ã¯ã¼ã")
        hr = c2.number_input("ãããã¼éå§è¡", min_value=1, value=1)
        if c3.form_submit_button("â è¿½å "):
            if pat:
                db_manager.save_parsing_rule(pat, hr - 1)
                clear_app_cache(); st.rerun()
                
    for idx, row in rules.iterrows():
        with st.container(border=True):
            r1, r2, r3 = st.columns([3, 1, 1])
            r1.write(f"ãã¿ã¼ã³: `{row['file_pattern']}`")
            r2.write(f"ãããã¼: {row['header_row']+1}è¡ç®")
            if r3.button("ðï¸ åé¤", key=f"dr_{idx}"):
                db_manager.delete_parsing_rule(row['file_pattern'])
                clear_app_cache(); st.rerun()

    st.divider()
    
    # æ¥ç¶è¨­å®ãæä¸é¨ã«ç§»å
    st.markdown("#### ð¡ æ¥ç¶ã»APIè¨­å®")
    with st.container(border=True):
        new_project_id = st.text_input("GCP Project ID", value=project_id, help="ä¾: sales-aggregator-123")
        if new_project_id.startswith("http"):
            st.error("â ï¸ URLã§ã¯ãªãããã­ã¸ã§ã¯ãIDãå¥åãã¦ãã ããã")
        
        new_api_key = st.text_input("Gemini API Key", value=gemini_api_key, type="password", help="AIè§£æã«ä½¿ç¨ãã¾ãã")
        
        if st.button("ð¾ è¨­å®ãä¿å­ãã¦åæ "):
            st.session_state.project_id = new_project_id.strip()
            st.session_state.gemini_api_key = new_api_key.strip()
            st.success("è¨­å®ãä¿å­ãã¾ããã")
            time.sleep(1)
            st.rerun()

    st.divider()
    if st.button("ð£ ãã¼ã¿ãã¼ã¹ãå®å¨ã«ãªã»ãããã", type="primary"):
        db_manager.reset_dataset(); clear_app_cache(); st.rerun()
