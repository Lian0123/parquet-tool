"""
Parquet Viewer — a minimal Flask app that lists and displays Parquet
files from a mounted directory.  Used via docker-compose for developer
verification.
"""

import os
from flask import Flask, render_template, request, jsonify
import pyarrow.parquet as pq

app = Flask(__name__)

PARQUET_DIR = os.environ.get("PARQUET_DIR", "/data")
MAX_PREVIEW_ROWS = 5000


def _resolve_parquet_path(filename: str) -> str | None:
    if not filename:
        return None

    root = os.path.realpath(PARQUET_DIR)
    filepath = os.path.realpath(os.path.join(root, filename))
    if os.path.commonpath([root, filepath]) != root:
        return None
    return filepath


def _parse_limit(raw_value: str | None, default: int) -> int:
    try:
        limit = int(raw_value or str(default))
    except (TypeError, ValueError):
        raise ValueError("limit must be an integer")

    if limit < 1:
        raise ValueError("limit must be greater than 0")

    return min(limit, MAX_PREVIEW_ROWS)


def _list_parquet_files() -> list[str]:
    files: list[str] = []
    if not os.path.isdir(PARQUET_DIR):
        return files
    for root, _, names in os.walk(PARQUET_DIR):
        for n in sorted(names):
            if n.endswith(".parquet"):
                files.append(os.path.relpath(os.path.join(root, n), PARQUET_DIR))
    return files


@app.route("/")
def index():
    files = _list_parquet_files()
    return render_template("index.html", files=files, parquet_dir=PARQUET_DIR)


@app.route("/view")
def view_file():
    filename = request.args.get("file", "")
    if not filename:
        return "Missing file parameter", 400

    filepath = _resolve_parquet_path(filename)
    if filepath is None:
        return "Invalid path", 403

    if not os.path.isfile(filepath):
        return f"File not found: {filename}", 404

    try:
        limit = _parse_limit(request.args.get("limit"), 100)
    except ValueError as exc:
        return str(exc), 400

    pf = pq.ParquetFile(filepath)
    meta = pf.metadata

    info = {
        "file": filename,
        "num_rows": meta.num_rows,
        "num_row_groups": meta.num_row_groups,
        "num_columns": meta.num_columns,
        "created_by": meta.created_by,
        "format_version": str(meta.format_version),
        "schema": [],
    }
    schema = pf.schema_arrow
    for i in range(len(schema)):
        field = schema.field(i)
        info["schema"].append({"name": field.name, "type": str(field.type)})

    table = pf.read().slice(0, limit)
    df = table.to_pandas()
    table_html = df.to_html(
        classes="table table-striped table-bordered table-sm",
        index=False,
        na_rep="NULL",
    )

    return render_template(
        "index.html",
        files=_list_parquet_files(),
        parquet_dir=PARQUET_DIR,
        selected=filename,
        info=info,
        table_html=table_html,
        limit=limit,
    )


@app.route("/api/files")
def api_files():
    return jsonify(_list_parquet_files())


@app.route("/api/read")
def api_read():
    filename = request.args.get("file", "")
    if not filename:
        return jsonify({"error": "Missing file parameter"}), 400

    filepath = _resolve_parquet_path(filename)
    if filepath is None:
        return jsonify({"error": "Invalid path"}), 403

    if not os.path.isfile(filepath):
        return jsonify({"error": f"File not found: {filename}"}), 404

    try:
        limit = _parse_limit(request.args.get("limit"), 1000)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    pf = pq.ParquetFile(filepath)
    df = pf.read().slice(0, limit).to_pandas()
    return jsonify(df.to_dict(orient="records"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
