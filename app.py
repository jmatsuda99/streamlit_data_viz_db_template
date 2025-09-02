import os
import re
import io
import json
import time
import math
import datetime as dt
from pathlib import Path

import pandas as pd
import streamlit as st
import duckdb

# ============================================================
# データ見える化ツール（多ファイル→DB格納→DBから自由に読み込み）
# - 環境: GitHub + Streamlit（Community Cloud対応）
# - DB: DuckDB（単一ファイル app_data.duckdb を作成）
# - 機能:
#   1) 複数のExcel/CSV/Parquetをアップロード
#   2) 各シートをテーブルとしてDBに格納（メタ情報は _catalog に記録）
#   3) DB内テーブルのプレビュー/削除
#   4) SQLで自由に読み込み
#   5) テーブルを編集ビューで開き「選択/数量」だけ編集→エクスポート or 新規テーブル保存
#   6) スキーマ編集（テーブル名/カラム名の変更→DB保存）
# ============================================================

st.set_page_config(page_title="データ見える化（DB格納版）", layout="wide")
DB_PATH = Path("app_data.duckdb")  # リポジトリ直下に作成
CATALOG_TABLE = "_catalog"
MAX_PREVIEW_ROWS = 1000

# -----------------------------
# DBユーティリティ
# -----------------------------
@st.cache_resource(show_spinner=False)
def get_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(DB_PATH))
    return conn


def init_db():
    con = get_conn()
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_TABLE} (
            table_name TEXT PRIMARY KEY,
            source_file TEXT,
            sheet_name TEXT,
            rows BIGINT,
            cols BIGINT,
            columns_json TEXT,
            uploaded_at TIMESTAMP
        );
        """
    )


def sanitize_name(name: str) -> str:
    base = name.strip().lower()
    base = re.sub(r"[^0-9a-zA-Z_]+", "_", base)
    base = re.sub(r"_{2,}", "_", base).strip("_")
    if not base:
        base = "tbl"
    if base[0].isdigit():
        base = f"t_{base}"
    return base


def ensure_unique_table_name(base: str) -> str:
    con = get_conn()
    existing = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    name = base
    i = 1
    while name in existing:
        name = f"{base}_{i}"
        i += 1
    return name


def list_tables_df() -> pd.DataFrame:
    con = get_conn()
    df = con.execute(
        f"SELECT * FROM {CATALOG_TABLE} ORDER BY uploaded_at DESC"
    ).fetchdf()
    return df


def drop_table(table_name: str):
    con = get_conn()
    con.execute(f"DROP TABLE IF EXISTS \"{table_name}\";")
    con.execute(f"DELETE FROM {CATALOG_TABLE} WHERE table_name = ?;", [table_name])


def register_df_as_table(df: pd.DataFrame, file_name: str, sheet_name: str) -> str:
    # 列名の改行や両端空白は除去
    df = df.copy()
    df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]
    # 追跡用メタカラムを付与
    df["_source_file"] = file_name
    df["_sheet_name"] = sheet_name
    df["_ingested_at"] = dt.datetime.now()

    base = sanitize_name(Path(file_name).stem + "_" + sheet_name)
    table_name = ensure_unique_table_name(base)

    con = get_conn()
    con.register("tmp_df", df)
    con.execute(f"CREATE TABLE \"{table_name}\" AS SELECT * FROM tmp_df;")
    con.unregister("tmp_df")

    meta = {
        "table_name": table_name,
        "source_file": file_name,
        "sheet_name": sheet_name,
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "columns_json": json.dumps(list(df.columns), ensure_ascii=False),
        "uploaded_at": dt.datetime.now(),
    }
    con.execute(
        f"""
        INSERT OR REPLACE INTO {CATALOG_TABLE}
        (table_name, source_file, sheet_name, rows, cols, columns_json, uploaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            meta["table_name"],
            meta["source_file"],
            meta["sheet_name"],
            meta["rows"],
            meta["cols"],
            meta["columns_json"],
            meta["uploaded_at"],
        ],
    )
    return table_name


def read_any(uploaded_file) -> dict:
    name = uploaded_file.name
    lower = name.lower()
    result = {}
    if lower.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
        result["(csv)"] = df
    elif lower.endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(uploaded_file)
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet)
            result[sheet] = df
    elif lower.endswith(".parquet"):
        df = pd.read_parquet(uploaded_file)
        result["(parquet)"] = df
    else:
        raise ValueError("対応拡張子: .csv / .xlsx / .xls / .parquet")
    return result


def ensure_selection_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "選択" not in df.columns:
        df["選択"] = False
    if "数量" not in df.columns:
        df["数量"] = 0
    return df


def apply_rounding(series: pd.Series, mode: str) -> pd.Series:
    if mode == "なし":
        return series
    def _f(x):
        try:
            x = float(x)
        except Exception:
            return x
        if mode == "切り上げ":
            return int(math.ceil(x))
        elif mode == "四捨五入":
            return int(round(x))
        elif mode == "切り捨て":
            return int(math.floor(x))
        return x
    return series.map(_f)


# -----------------------------
# 初期化
# -----------------------------
init_db()

st.title("データ見える化ツール — 多ファイルをDBに格納して活用")
st.caption("複数ファイルをアップロード→各シートをテーブル化→DBからプレビュー/SQL/編集/エクスポート/スキーマ編集")

# =============================
# 1) アップロード → DB格納
# =============================
st.sidebar.header("1) データ取り込み（複数可）")
uploads = st.sidebar.file_uploader(
    "Excel/CSV/Parquet を選択（複数可）",
    type=["xlsx", "xls", "csv", "parquet"],
    accept_multiple_files=True,
)

if uploads:
    with st.sidebar.expander("取り込みプレビュー", expanded=False):
        for uf in uploads:
            st.write(f"**{uf.name}**")

    if st.sidebar.button("DBに取り込む", type="primary"):
        with st.spinner("取り込み中…"):
            ingested = []
            for uf in uploads:
                try:
                    sheets = read_any(uf)
                    for sheet_name, df in sheets.items():
                        tbl = register_df_as_table(df, file_name=uf.name, sheet_name=sheet_name)
                        ingested.append((uf.name, sheet_name, tbl, len(df)))
                except Exception as e:
                    st.sidebar.error(f"{uf.name}: 取り込み失敗 → {e}")
            if ingested:
                st.sidebar.success(f"{len(ingested)} テーブルを作成しました。")
                import pandas as _pd
                with st.expander("作成テーブルの一覧", expanded=True):
                    st.dataframe(_pd.DataFrame(ingested, columns=["source_file", "sheet", "table", "rows"]))

st.sidebar.divider()

# =============================
# 2) カタログ表示 & 管理
# =============================
st.subheader("DB内テーブル（カタログ）")
cat_df = list_tables_df()
st.dataframe(cat_df, use_container_width=True, hide_index=True)

col_a, col_b, col_c = st.columns([2,1,1])
with col_a:
    target_table = st.selectbox("操作対象テーブル", [""] + cat_df["table_name"].tolist(), index=0)
with col_b:
    if st.button("プレビュー表示", disabled=(not target_table)):
        if target_table:
            con = get_conn()
            preview = con.execute(f'SELECT * FROM "{target_table}" LIMIT {MAX_PREVIEW_ROWS}').fetchdf()
            st.markdown(f"**{target_table}** プレビュー（最大 {MAX_PREVIEW_ROWS} 行）")
            st.dataframe(preview, use_container_width=True, hide_index=True)
with col_c:
    if st.button("テーブル削除", disabled=(not target_table)):
        if target_table:
            drop_table(target_table)
            st.success(f"削除しました: {target_table}")
            st.experimental_rerun()

st.divider()

# =============================
# 3) SQLで自由に読み込み
# =============================
st.subheader("SQL クエリ実行（自由に読み込み）")
def _default_sql():
    if len(cat_df) > 0:
        t0 = cat_df.iloc[0]["table_name"]
        return f'SELECT * FROM "{t0}" LIMIT 100;'
    return "-- ここにSQLを書いてください (例)\n-- SELECT * FROM \"table_name\" LIMIT 100;"

sql = st.text_area("SQL", value=_default_sql(), height=140, help='列名にスペース等がある場合は \"列名\" とダブルクォートで囲んでください')
run_q = st.button("クエリ実行", type="primary")

if run_q and sql.strip():
    try:
        con = get_conn()
        with st.spinner("実行中…"):
            qdf = con.execute(sql).fetchdf()
        st.success(f"{len(qdf)} 行取得")
        st.dataframe(qdf, use_container_width=True, hide_index=True)
        # ダウンロード
        csv_bytes = qdf.to_csv(index=False).encode("utf-8-sig")
        st.download_button("結果をCSVダウンロード", data=csv_bytes, file_name="query_result.csv", mime="text/csv")
    except Exception as e:
        st.error(f"SQLエラー: {e}")

st.divider()

# =============================
# 4) 編集ビュー（選択/数量のみ編集）→ エクスポート or 新規テーブル保存
# =============================
st.subheader("編集ビュー（数量と選択だけ編集可）")
edit_table = st.selectbox("編集するテーブルを選択", [""] + cat_df["table_name"].tolist(), index=0)

if edit_table:
    con = get_conn()
    df_edit = con.execute(f'SELECT * FROM "{edit_table}"').fetchdf()
    df_edit = ensure_selection_cols(df_edit)

    # 表示列の選択
    with st.expander("列の表示/非表示", expanded=False):
        all_cols = list(df_edit.columns)
        show_cols = st.multiselect("表示する列", all_cols, default=all_cols)
    vdf = df_edit[show_cols] if show_cols else df_edit

    # 数量/選択 以外は編集不可
    disabled_cols = [c for c in vdf.columns if c not in ["数量", "選択"]]

    st.markdown("**テーブルを直接編集**（数量・選択のみ）")
    edited = st.data_editor(
        vdf,
        key=f"editor_{edit_table}",
        use_container_width=True,
        hide_index=True,
        column_config={
            "選択": st.column_config.CheckboxColumn("選択"),
            "数量": st.column_config.NumberColumn("数量", min_value=0, step=1),
        },
        disabled=disabled_cols,
    )

    # 端数処理
    st.write("")
    col1, col2, col3 = st.columns(3)
    with col1:
        rounding = st.selectbox("数量の端数処理", ["なし", "切り上げ", "四捨五入", "切り捨て"], index=0)
    with col2:
        apply_round_btn = st.button("端数処理を適用")
    with col3:
        pass

    merged = df_edit.copy()
    common_cols = [c for c in edited.columns if c in merged.columns]
    merged[common_cols] = edited[common_cols]

    if apply_round_btn:
        merged["数量"] = apply_rounding(merged["数量"], rounding)
        # 表示も更新
        if "数量" in edited.columns:
            edited.loc[:, "数量"] = merged["数量"]

    # 選択行のみ
    sel = merged[merged["選択"] == True].copy()

    # 金額(概算)
    if "単価" in sel.columns and "数量" in sel.columns:
        with pd.option_context('mode.chained_assignment', None):
            sel["金額(概算)"] = pd.to_numeric(sel["単価"], errors='coerce').fillna(0) * pd.to_numeric(sel["数量"], errors='coerce').fillna(0)

    # 集計・エクスポート
    st.markdown("---")
    colx, coly, colz = st.columns(3)
    with colx:
        st.write(f"選択行: **{len(sel)}** 件")
    with coly:
        if "金額(概算)" in sel.columns:
            st.write(f"合計金額(概算): **{int(sel['金額(概算)'].sum()):,}**")
    with colz:
        pass

    csv_bytes = sel.to_csv(index=False).encode("utf-8-sig")
    st.download_button("CSVダウンロード", data=csv_bytes, file_name=f"{edit_table}_selection.csv", mime="text/csv")

    # Excel
    def to_excel_bytes(df: pd.DataFrame) -> bytes:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False)
        buf.seek(0)
        return buf.read()

    xlsx_bytes = to_excel_bytes(sel)
    st.download_button(
        "Excelダウンロード",
        data=xlsx_bytes,
        file_name=f"{edit_table}_selection.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # 新規テーブルとして保存
    st.markdown("#### 編集結果を新規テーブルとしてDBに保存")
    new_name_default = ensure_unique_table_name(edit_table + "_edited")
    new_table_name = st.text_input("保存テーブル名", value=new_name_default)
    if st.button("DBに保存"):
        try:
            con.register("_tmp_save", merged)
            con.execute(f'CREATE TABLE "{new_table_name}" AS SELECT * FROM _tmp_save;')
            con.unregister("_tmp_save")
            # カタログにも登録
            meta = {
                "table_name": new_table_name,
                "source_file": f"__from:{edit_table}",
                "sheet_name": "(edited)",
                "rows": int(len(merged)),
                "cols": int(len(merged.columns)),
                "columns_json": json.dumps(list(merged.columns), ensure_ascii=False),
                "uploaded_at": dt.datetime.now(),
            }
            con.execute(
                f"INSERT OR REPLACE INTO {CATALOG_TABLE} VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    meta["table_name"],
                    meta["source_file"],
                    meta["sheet_name"],
                    meta["rows"],
                    meta["cols"],
                    meta["columns_json"],
                    meta["uploaded_at"],
                ],
            )
            st.success(f"保存しました: {new_table_name}")
        except Exception as e:
            st.error(f"保存に失敗: {e}")

st.sidebar.divider()

# =============================
# 5) スキーマ編集（テーブル名/カラム名の変更→DB保存）
# =============================
st.subheader("スキーマ編集（テーブル名/カラム名の変更→DB保存）")
schema_table = st.selectbox("対象テーブル", [""] + cat_df["table_name"].tolist(), index=0, key="schema_tbl")

if schema_table:
    # --- テーブル名の変更 ---
    st.markdown("**テーブル名の変更**")
    new_tbl_input = st.text_input("新しいテーブル名", value=schema_table, key="new_tbl_name")
    if st.button("テーブル名を変更", disabled=(not new_tbl_input or new_tbl_input == schema_table)):
        try:
            safe = sanitize_name(new_tbl_input)
            if safe != new_tbl_input:
                st.warning(f"テーブル名を安全化して '{safe}' を適用します")
            # 既存と重複があればユニーク化
            uniq = ensure_unique_table_name(safe)
            if uniq != safe:
                st.info(f"既存と重複のため '{uniq}' に変更します")
            con = get_conn()
            con.execute(f'ALTER TABLE "{schema_table}" RENAME TO "{uniq}";')
            con.execute(f'UPDATE {CATALOG_TABLE} SET table_name=? WHERE table_name=?;', [uniq, schema_table])
            st.success(f"テーブル名を '{schema_table}' → '{uniq}' に変更しました")
            st.experimental_rerun()
        except Exception as e:
            st.error(f"変更に失敗: {e}")

    # --- カラム名の変更 ---
    st.markdown("**カラム名の変更**")
    con = get_conn()
    cols_df = con.execute(f'PRAGMA table_info("{schema_table}")').fetchdf()

    with st.form(f"col_rename_{schema_table}"):
        new_names = {}
        for _, row in cols_df.iterrows():
            col = row["name"]
            new_val = st.text_input(f'列: {col}', value=col, key=f'col_{schema_table}_{col}')
            new_names[col] = new_val
        submitted = st.form_submit_button("カラム名を一括変更")
        if submitted:
            try:
                # 安全化と重複チェック
                sanitized = {}
                for old, new in new_names.items():
                    safe = sanitize_name(new)
                    if safe != new:
                        st.warning(f"列 '{old}' は安全化して '{safe}' を適用します")
                    sanitized[old] = safe
                if len(set(sanitized.values())) != len(sanitized.values()):
                    st.error("重複する新カラム名があります。修正してください。")
                else:
                    # 順次リネーム
                    for old in cols_df["name"].tolist():
                        new = sanitized.get(old, old)
                        if old != new:
                            con.execute(f'ALTER TABLE "{schema_table}" RENAME COLUMN "{old}" TO "{new}";')
                    # カタログのcolumns_json更新
                    cols_after = con.execute(f'PRAGMA table_info("{schema_table}")').fetchdf()["name"].tolist()
                    con.execute(f'UPDATE {CATALOG_TABLE} SET columns_json=? WHERE table_name=?;', [json.dumps(cols_after, ensure_ascii=False), schema_table])
                    st.success("カラム名を更新しました")
                    st.experimental_rerun()
            except Exception as e:
                st.error(f"変更に失敗: {e}")

st.sidebar.divider()

# =============================
# 6) メンテナンス
# =============================
st.sidebar.header("メンテナンス")
if st.sidebar.button("カタログ再読み込み"):
    st.experimental_rerun()

with st.sidebar.expander("DBファイルの場所"):
    st.code(str(DB_PATH.resolve()))

st.caption("DuckDBは単一ファイルDBです。リポジトリにコミットする場合はサイズに注意してください。必要に応じて .gitignore に追加してください。")
