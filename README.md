# データ見える化ツール（Streamlit + DuckDB）

## 概要
- 複数の Excel / CSV / Parquet をアップロードし、**DuckDB** に格納してから自由に呼び出し
- 取り込みメタは `_catalog` に保存
- DB内テーブルのプレビュー/削除、任意 **SQL 実行 UI**
- 編集ビューで **「選択」「数量」だけ編集可** → CSV/Excel 出力 or 新規テーブル保存
- **スキーマ編集**（テーブル名/カラム名の変更を DB に反映）

## セットアップ
```bash
pip install -r requirements.txt
streamlit run app.py
```

## デプロイ（Streamlit Community Cloud）
- リポジトリを選択 → ブランチ → `app.py` をエントリに設定

## 注意
- DB はリポジトリ直下の `app_data.duckdb` に作成・永続化されます
- 大きくなる場合は `.gitignore` で除外してください
