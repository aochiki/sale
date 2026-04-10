import sys

target_file = r'c:\Users\aono\Desktop\antigravity\sales_aggregator\app.py'

with open(target_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_content = """                with st.spinner("AIが意図を解析中..."):
                    # 1. カラム情報の抽出
                    attr_cols_ai = [m['unified_name'] for _, m in mappings.iterrows() if not m['is_numeric'] and not m['is_date']]
                    num_cols_ai = [m['unified_name'] for _, m in mappings.iterrows() if m['is_numeric']]
                    all_cols = attr_cols_ai + (['SOURCE'] if 'SOURCE' in unified_df.columns else [])
                    
                    # 2. AIへの問い合わせ
                    gemini_key = st.session_state.get('gemini_api_key')
                    if not gemini_key:
                        st.error("Gemini APIキーが設定されていません。")
                        st.stop()
                    
                    parsed = parse_natural_language_query(project_id, user_query, all_cols, num_cols_ai, api_key=gemini_key)
                
                # 3. 解析結果の処理 (スピナーの外)
                if not parsed:
                    st.error("AIからの応答がありませんでした。")
                elif "error" in parsed:
                    st.error(f"AI解析エラー: {parsed['error']}")
                else:
                    with st.expander("🔍 AIの解析結果を確認"): st.json(parsed)
                    
                    try:
                        f_df = flex_df_ai.copy()
                        # 4. フィルタ適用
                        filters = parsed.get("filters", {})
                        if filters:
                            for col, val in filters.items():
                                if col in f_df.columns:
                                    f_df = f_df[f_df[col].astype(str).str.contains(str(val), na=False, case=False)]
                        
                        # 5. 軸と値の決定
                        def clean(a):
                            if isinstance(a, list): return [str(i).strip() for i in a if i]
                            return str(a).strip() if a else None

                        r_axis = clean(parsed.get("row_axis"))
                        c_axis = clean(parsed.get("col_axis"))
                        v_axis = [str(v).strip() for v in parsed.get("value_axis", []) if v]
                        
                        if not v_axis and num_cols_ai: v_axis = [num_cols_ai[0]]
                        
                        # カラム存在チェック
                        def check(cols, df):
                            if not cols: return []
                            c_list = cols if isinstance(cols, list) else [cols]
                            return [x for x in c_list if x not in df.columns]

                        missing = check(r_axis, f_df) + check(c_axis, f_df) + check(v_axis, f_df)
                        
                        if missing:
                            st.warning(f"項目が見つかりません: {', '.join(missing)}")
                            st.info(f"利用可能な項目: {', '.join(f_df.columns)}")
                        elif not v_axis:
                            st.warning("集計対象の数値項目が指定されていません。")
                        else:
                            # 6. 集計と表示
                            if not r_axis and not c_axis:
                                st.write("### 📋 合計結果")
                                st.dataframe(f_df[v_axis].sum().to_frame(name='合計').style.format("{:,.0f}"))
                            else:
                                pivot_res = f_df.pivot_table(index=r_axis, columns=c_axis, values=v_axis, aggfunc='sum', margins=True, margins_name="合計")
                                st.dataframe(pivot_res.style.format("{:,.0f}"), use_container_width=True)
                                
                    except Exception as e:
                        st.error(f"集計処理中にエラーが発生しました: {e}")
                        st.exception(e)
"""

# 174行目から227行目（1-indexed なので 173から227）を置換
# index は 0-indexed なので 173:227
start_idx = 173
end_idx = 227

# 行が実際に何行目かを確認しながら置換（安全のため）
# 最初の数文字でマッチング確認
if "with st.spinner(\"AIが意図を解析中...\"):" in lines[start_idx]:
    lines[start_idx:end_idx] = [new_content + "\n"]
    with open(target_file, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print("Success")
else:
    print(f"Mismatch at line {start_idx+1}: {lines[start_idx].strip()}")
    sys.exit(1)
