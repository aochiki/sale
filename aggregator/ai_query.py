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
    if not api_key:
        return {"error": "APIキーが指定されていません。"}

    try:
        # Google AI SDK クライアントの初期化
        client = genai.Client(api_key=api_key)
        
        cols_text = ", ".join([str(c).strip() for c in unified_columns]) if unified_columns else "未定義"
        num_cols_text = ", ".join([str(c).strip() for c in num_cols]) if num_cols else "未定義"
        
        prompt = f"""
あなたは売上データ集計用のアシスタントです。ユーザーの要望から、ピボットテーブルを作成するための設定を抽出してください。

[利用可能なカラム名]
すべての項目: {cols_text}
集計値として利用可能な数値項目: {num_cols_text}

[ユーザーの要望]
"{user_text}"

[出力形式]
以下のJSONスキーマに従い、JSON文字列のみを出力してください。余計な文字列（```json など）は含めないでください。
{{
  "filters": {{
    "カラム名": "絞り込む文字列（例: アーティスト名など）"
  }},
  "row_axis": "タテ軸（行）にするカラム名、なければnull",
  "col_axis": "ヨコ軸（列）にするカラム名、なければnull",
  "value_axis": ["表示する値（数値項目）のリスト。要望になければデフォルトとして {num_cols_text}のうち最も適切なものを含める"]
}}
"""
        # 最新 SDK でのモデル呼び出し
        try:
            response = client.models.generate_content(
                model="gemini-flash-latest",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.0
                )
            )
        except Exception as e:
            if "not found" in str(e).lower():
                # 利用可能なモデルをリストアップして詳細を表示
                models = [m.name for m in client.models.list()]
                available = ", ".join(models)
                return {"error": f"モデルが見つかりません。利用可能なモデル: {available}"}
            raise e
        
        result_text = response.text.strip()
        result_json = json.loads(result_text)
        return result_json

    except Exception as e:
        logger.error(f"Gemini API (New SDK) 解析エラー: {e}")
        return {"error": str(e)}
