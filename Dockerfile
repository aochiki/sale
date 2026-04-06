FROM python:3.11-slim

# 作業ディレクトリの設定
WORKDIR /app

# OSパッケージのインストールを最小限に（ビルドエラー回避のため）
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# 依存ファイルのコピーとインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードのコピー
COPY . .

# Streamlit のポート設定を環境変数 $PORT に対応させる
# Cloud Run は起動時にポートを動的に割り当てます
EXPOSE 8080

# アプリの起動設定
# --server.port=$PORT を使用して、Cloud Run の仕様に合わせる
ENTRYPOINT ["sh", "-c", "streamlit run app.py --server.port=${PORT:-8080} --server.address=0.0.0.0 --server.enableCORS=false --server.enableXsrfProtection=false"]
