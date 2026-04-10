import json
import logging
from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

def parse_natural_language_query(project_id, user_text, unified_columns, num_cols, api_key=None):
    """
    ユーザーの自然言語入力を解析し、Pandas ピボットテーブル用のパラメータをJSONで返す。
    最新の google-genai SDK (APIキー方式) を使用。
    """
    import time
    if not api_key:
        return {"error": "APIキーが指定されていません。"}

    try:
        # Google AI SDK クライアントの初期化
        client = genai.Client(api_key=api_key)
        
        cols_text = ", ".join([str(c).strip() for c in unified_columns]) if unified_columns else "未定義"
        num_cols_text = ", ".join([str(c).strip() for c in num_cols]) if num_cols else "未定義"
        
        prompt = f"""
あなたは売上データ集計用のアシスタントです。ユーザーの要望から、ピボットテーブルを作成するための設定を抽出してください。

[利用可能な全項目名（属性）]
{cols_text}

[利用可能な数値項目名（集計対象）]
{num_cols_text}

[ユーザーの要望]
"{user_text}"

[出力形式]
以下のJSONスキーマに従い、JSON文字列のみを出力してください。
{{
  "filters": {{
    "属性項目名": "絞り込む文字列"
  }},
  "row_axis": "タテ軸（行）にする項目名。複数ある場合はリスト形式 ['A', 'B']。なければnull",
  "col_axis": "ヨコ軸（列）にする項目名、なければnull",
  "value_axis": ["集計対象の数値項目名のリスト"]
}}

[注意事項]
- カラム名は必ず [利用可能な項目名] にあるものから一字一句違わずに選んでください。
- フィルターの項目名は、データに含まれる属性項目名にマッピングしてください。
- アーティスト名や曲名などの固有名詞は、ユーザーが入力した通り（全角・半角を含め）に抽出してください。
- ユーザーが「すべて」の数値を求めた場合は、{num_cols_text} をすべて含めてください。
"""
        # モデル名を最新の gemini-3-flash-preview に変更
        model_name = "gemini-3-flash-preview"
        logger.info(f"Gemini API 呼び出し開始 (モデル: {model_name})")
        start_time = time.time()
        
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.0
                )
            )
            elapsed = time.time() - start_time
            logger.info(f"Gemini API レスポンス受領成功 (経過時間: {elapsed:.2f}秒)")
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Gemini API 直接エラー (経過時間: {elapsed:.2f}秒): {e}")
            
            # フォールバック: 2.5-flash も試す
            if "not found" in str(e).lower() or "500" in str(e):
                logger.warning("gemini-3-flash-preview が利用不可のため、gemini-2.5-flash で再試行します")
                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.0
                    )
                )
            else:
                raise e
        
        result_text = response.text.strip()
        if not result_text:
            return {"error": "AIからのレスポンスが空でした。"}
            
        result_json = json.loads(result_text)
        return result_json

    except json.JSONDecodeError as je:
        logger.error(f"JSONパースエラー: {je} | Text: {result_text}")
        return {"error": f"AIの回答形式が不正です: {je}"}
    except Exception as e:
        logger.error(f"Gemini API 総合エラー: {e}")
        return {"error": str(e)}
