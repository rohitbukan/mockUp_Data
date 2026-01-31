from flask import Flask, render_template, request, jsonify, send_file
from faker import Faker
import io
import os
import csv
import random
import re

app = Flask(__name__)
fake = Faker()

# -------------------------------
# Helpers
# -------------------------------

def parse_columns(ddl_text):
    """Extract column names and datatypes from DDL"""
    ddl_text = re.sub(r'--.*', '', ddl_text)
    ddl_text = re.sub(r'/\*.*?\*/', '', ddl_text, flags=re.DOTALL)

    m = re.search(r'CREATE\s+TABLE.*?\((.*)\)', ddl_text, re.IGNORECASE | re.DOTALL)
    if not m:
        return []

    cols_text = m.group(1)
    cols_lines = re.split(r',\s*(?![^(]*\))', cols_text)
    columns = []

    for line in cols_lines:
        line = line.strip()
        if not line:
            continue
        col_match = re.match(r'(\w+)\s+([\w() ]+)', line)
        if col_match:
            columns.append((col_match.group(1), col_match.group(2).upper().strip()))
    return columns

def extract_table_name(ddl):
    """Extract table name from DDL"""
    match = re.search(r"CREATE\s+TABLE\s+`?([\w]+(?:\.[\w]+)?)`?", ddl, re.I)
    return match.group(1) if match else "my_table"

def gen_value(dtype, col_name=None, row_index=0, row_data=None):
    """
    Generates dummy data based on datatype and column name.
    Supports Oracle/Hive datatypes
    """
    col = (col_name or "").lower()
    dtype = (dtype or "").upper()

    # ID columns
    if col == "id" or col.endswith("_id"):
        return row_index + 1

    # NAME
    if "name" in col:
        full_name = fake.name()
        if row_data is not None:
            row_data["__generated_name__"] = full_name
        return full_name

    # EMAIL
    if "email" in col:
        if row_data is not None and "__generated_name__" in row_data:
            name = row_data["__generated_name__"].lower()
            name = re.sub(r"[^a-z ]", "", name)
            parts = name.strip().split()
            email_name = f"{parts[0]}.{parts[-1]}" if len(parts) > 1 else parts[0]
        else:
            name = fake.name().lower()
            name = re.sub(r"[^a-z ]", "", name)
            parts = name.strip().split()
            email_name = f"{parts[0]}.{parts[-1]}" if len(parts) > 1 else parts[0]
        domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "company.com"])
        return f"{email_name}@{domain}"

    # PHONE
    if "phone" in col or "mobile" in col:
        return fake.msisdn()[:10]

    # ADDRESS
    if "address" in col:
        return fake.address().replace("\n", ", ")

    # COMPANY
    if "company" in col:
        return fake.company()

    # BOOLEAN
    if "BOOL" in col or "BOOLEAN" in dtype:
        return random.choice([True, False])

    # INTEGER TYPES
    if any(x in dtype for x in ["TINYINT","SMALLINT","INT","INTEGER","BIGINT"]) or dtype.startswith("NUMBER") and "(" not in dtype:
        return fake.random_int(min=1, max=10000)

    # DECIMAL / FLOAT TYPES
    if any(x in dtype for x in ["FLOAT","DOUBLE","DECIMAL","BINARY_FLOAT","BINARY_DOUBLE"]) or (dtype.startswith("NUMBER") and "(" in dtype):
        match = re.search(r"\((\d+),\s*(\d+)\)", dtype)
        if match:
            scale = int(match.group(2))
            left_digits = int(match.group(1)) - scale
        else:
            left_digits = 5
            scale = 2
        return round(fake.pyfloat(left_digits=left_digits, right_digits=scale, positive=True), scale)

    # STRING / CHAR TYPES
    if any(x in dtype for x in ["STRING","CHAR","VARCHAR","VARCHAR2","NCHAR","NVARCHAR2","LONG"]):
        # Try to extract length if present
        length_match = re.search(r'\((\d+)\)', dtype)
        length = int(length_match.group(1)) if length_match else 10
        # Generate random text of that length
        text = fake.text(max_nb_chars=length*2).replace("\n", " ")  # text may be longer
        return text[:length]

    # DATE / TIMESTAMP
    if "DATE" == dtype:
        return fake.date_time().strftime("%Y-%m-%d")
    if "TIMESTAMP" == dtype:
        return fake.date_time().strftime("%Y-%m-%d %H:%M:%S")


    if "TIMESTAMP WITH TIME ZONE" in dtype:
        return fake.date_time().isoformat() + "+00:00"

    # fallback
    return fake.word()

# -------------------------------
# Routes
# -------------------------------

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload-sql", methods=["POST"])
def upload_sql():
    try:
        if "file" not in request.files:
            return jsonify({"success": False, "message": "No file uploaded"}), 400
        file = request.files["file"]
        ddl_text = file.read().decode("utf-8")
        return jsonify({"success": True, "ddl": ddl_text})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/generate", methods=["POST"])
def generate():
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"success": False, "message": "No data provided"}), 400

        ddl = payload.get("ddl", "")
        rows = int(payload.get("rows", 0))
        cols = parse_columns(ddl)

        if not cols:
            return jsonify({"success": False, "message": "No valid columns found in DDL"}), 400

        # Allow adding extra columns
        extra_cols = payload.get("extra_columns", [])  # List of tuples [(colname, dtype)]
        if extra_cols:
            cols.extend(extra_cols)

        columns = [c[0] for c in cols]
        data = []
        for i in range(rows):
            row = {}
            for name, dtype in cols:
                row[name] = gen_value(dtype, name, i, row)
            data.append(row)

        return jsonify({"success": True, "columns": columns, "data": data})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/csv", methods=["POST"])
def download_csv():
    try:
        payload = request.get_json()
        columns = payload["columns"]
        data = payload["data"]

        clean_data = []
        for row in data:
            row_copy = {k: v for k, v in row.items() if k in columns}
            clean_data.append(row_copy)

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(clean_data)

        return send_file(
            io.BytesIO(output.getvalue().encode("utf-8-sig")),
            as_attachment=True,
            download_name="dummy_data.csv",
            mimetype="text/csv"
        )
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/insert-sql", methods=["POST"])
def generate_insert_sql():
    try:
        payload = request.json
        ddl = payload.get("ddl", "")
        columns = payload.get("columns", [])
        data = payload.get("data", [])

        if not ddl or not columns or not data:
            return jsonify({"success": False, "message": "Missing DDL, columns, or data"}), 400

        table_name = extract_table_name(ddl)
        sql_lines = []

        for row in data:
            values = []
            for col in columns:
                val = row.get(col)
                if val is None or val == "":
                    values.append("NULL")
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                else:
                    escaped = str(val).replace("'", "''")
                    values.append(f"'{escaped}'")
            sql_lines.append(
                f"INSERT INTO {table_name} ({', '.join(columns)}) "
                f"VALUES ({', '.join(values)});"
            )

        sql_content = "\n".join(sql_lines)

        return send_file(
            io.BytesIO(sql_content.encode("utf-8")),
            as_attachment=True,
            download_name=f"{table_name}_insert.sql",
            mimetype="text/sql"
        )
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

# -------------------------------
# Run
# -------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
