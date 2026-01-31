import re
import random
from datetime import datetime, timedelta
from faker import Faker
import csv
from pathlib import Path

fake = Faker()

# ---------- Value generators ----------

def gen_value(col_name: str, sql_type: str):
    """Generate realistic dummy values based on column name and datatype."""
    name = col_name.lower()
    t = sql_type.upper()

    # Semantic mapping based on column name
    if "name" in name:
        return fake.name()
    if "first" in name and "name" in name:
        return fake.first_name()
    if "last" in name and "name" in name:
        return fake.last_name()
    if "email" in name:
        return fake.email()
    if "phone" in name or "mobile" in name:
        return fake.phone_number()
    if "address" in name:
        return fake.address()
    if "city" in name:
        return fake.city()
    if "country" in name:
        return fake.country()
    if "company" in name or "org" in name:
        return fake.company()
    if "job" in name or "role" in name:
        return fake.job()
    if "salary" in name or "amount" in name or "price" in name:
        return round(random.uniform(30000, 200000), 2)
    if "date" in name and "time" not in name:
        return fake.date()
    if "time" in name or "timestamp" in name:
        start = datetime.now() - timedelta(days=5*365)
        return fake.date_time_between(start_date=start, end_date="now").isoformat(sep=" ")

    # Fallback to datatype-based generation
    if "CHAR" in t or "TEXT" in t or "CLOB" in t or "VARCHAR" in t:
        return fake.word()
    if "INT" in t or "NUMBER" in t or "DECIMAL" in t:
        return random.randint(1, 1000)
    if "DATE" in t and "TIME" not in t:
        return fake.date()
    if "TIMESTAMP" in t or "DATETIME" in t or "TIME" in t:
        start = datetime.now() - timedelta(days=5*365)
        return fake.date_time_between(start_date=start, end_date="now").isoformat(sep=" ")
    if "BOOL" in t:
        return random.choice([0, 1])
    if "FLOAT" in t or "DOUBLE" in t or "REAL" in t:
        return round(random.uniform(0, 1000), 2)

    return fake.word()

# ---------- DDL parsing ----------

create_table_re = re.compile(
    r"CREATE\s+TABLE\s+([`\"']?)(?P<name>[A-Za-z0-9_]+)\1\s*\((?P<body>.*?)\);",
    re.IGNORECASE | re.DOTALL
)

def parse_columns(body: str):
    """Extract column definitions from CREATE TABLE body."""
    cols = []
    parts = re.split(r",(?![^(]*\))", body)  # split on commas not inside ()
    for part in parts:
        line = part.strip()
        if not line:
            continue
        # skip constraints
        if re.match(r"(?i)(PRIMARY|FOREIGN)\s+KEY|UNIQUE|CHECK|CONSTRAINT", line):
            continue
        tokens = line.split()
        if len(tokens) < 2:
            continue
        col_name = tokens[0].strip("`\"'")
        col_type = " ".join(tokens[1:])
        cols.append((col_name, col_type))
    return cols

def parse_sql_ddl(ddl: str):
    """Parse full DDL string and return tables with columns."""
    tables = []
    for match in create_table_re.finditer(ddl):
        table_name = match.group("name")
        body = match.group("body")
        columns = parse_columns(body)
        tables.append((table_name, columns))
    return tables

# ---------- Data generation ----------

def generate_data_for_table(table_name, columns, rows=100, out_dir="output"):
    """Generate dummy CSV data for a single table."""
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    csv_file = out_path / f"{table_name}.csv"

    with csv_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        col_names = [c[0] for c in columns]
        writer.writerow(col_names)

        for _ in range(rows):
            row = [gen_value(col_name, col_type) for col_name, col_type in columns]
            writer.writerow(row)

    print(f"Generated {rows} rows for table {table_name} -> {csv_file}")

def generate_from_ddl_string(ddl_string: str, rows_per_table=100, out_dir="output"):
    """Generate dummy data from a DDL string."""
    tables = parse_sql_ddl(ddl_string)
    if not tables:
        print("No CREATE TABLE statements found.")
        return
    for table_name, columns in tables:
        generate_data_for_table(table_name, columns, rows=rows_per_table, out_dir=out_dir)

def generate_from_file(file_path: str, rows_per_table=100, out_dir="output"):
    """Generate dummy data from a DDL file."""
    ddl_string = Path(file_path).read_text(encoding="utf-8")
    generate_from_ddl_string(ddl_string, rows_per_table=rows_per_table, out_dir=out_dir)

# ---------- CLI entry point ----------

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python ddl_to_csv.py <ddl_file> [rows] [out_dir]")
    else:
        ddl_file = sys.argv[1]
        rows = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        out_dir = sys.argv[3] if len(sys.argv) > 3 else "output"
        generate_from_file(ddl_file, rows_per_table=rows, out_dir=out_dir)