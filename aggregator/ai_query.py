import json
import logging
import vertexai
from vertexai.generative_models import GenerativeModel

logger = logging.getLogger(__name__)

def parse_natural_language_query(project_id, user_text, unified_columns, num_cols):
    """
    ユーザーの自然言語入力を解析し、Pandas ピボットテーブル用のパラメータをJSONで返す。
    
    Args:
        project_id (str): GCP Project ID
        user_text (str): ユーザーからの自然言語要求
        unified_columns (list): セレクト可能な全カラム名のリスト
        num_cols (list): 数値（集計対象）として定義されたカラム名のリスト
        
    Returns:
        dict: 抽出されたパラメータを持つ辞書
    """
    try:
        # Vertex AI の初期化
        vertexai.init(project=project_id, location="asia-northeast1")
        
        # 軽量・高速なFlashモデル
        model = GenerativeModel("gemini-1.5-flash")
        
        cols_text = ", ".join(unified_columns) if unified_columns else "未定義"
        num_cols_text = ", ".join(num_cols) if num_cols else "未定義"
        
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
        response = model.generate_content(
            prompt,
            generation_config={
                "response_mime_type": "application/json", 
                "temperature": 0.0
            }
        )
        
        result_text = response.text.strip()
        result_json = json.loads(result_text)
        return result_json

    except Exception as e:
        logger.error(f"Vertex AI 解析エラー: {e}")
        return None
