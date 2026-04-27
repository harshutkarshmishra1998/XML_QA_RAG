import re
import json
from pathlib import Path
from sqlglot import parse_one, exp


# -----------------------------
# 🔥 EXTRACT DYNAMIC SQL
# -----------------------------
def extract_dynamic_sql(stmt: str):
    if "EXECUTE IMMEDIATE" not in stmt.upper():
        return stmt

    parts = re.findall(r"'([^']*)'", stmt, re.DOTALL)
    if parts:
        return " ".join(parts)

    return stmt


# -----------------------------
# NORMALIZATION
# -----------------------------
def normalize_sql(text: str) -> str:
    text = re.sub(r'--.*', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def split_procedures(text: str):
    pattern = r'CREATE OR REPLACE.*?PROCEDURE.*?END\s+\w+\s*;'
    matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
    return matches if matches else [text]


# -----------------------------
# 🔥 SAFE STATEMENT SPLITTER
# -----------------------------
def extract_statements(proc_text: str):
    statements = []
    current = ""
    paren = 0
    in_string = False

    for ch in proc_text:
        current += ch

        if ch == "'":
            in_string = not in_string

        elif not in_string:
            if ch == "(":
                paren += 1
            elif ch == ")":
                paren -= 1

            elif ch == ";" and paren == 0:
                statements.append(current.strip())
                current = ""

    if current.strip():
        statements.append(current.strip())

    return statements


# -----------------------------
# 🔥 SQL PARSER (FIXED)
# -----------------------------
def parse_sql(stmt):
    try:
        clean_stmt = extract_dynamic_sql(stmt)

        # DEBUG (remove later if needed)
        print("\n====================")
        print("CLEAN SQL:")
        print(clean_stmt[:500])

        return parse_one(clean_stmt, read="oracle")

    except Exception as e:
        print("PARSE FAILED:", e)
        return None


# -----------------------------
# AST HELPERS
# -----------------------------
def extract_tables(ast):
    tables = set()
    for table in ast.find_all(exp.Table):
        tables.add(table.sql())   # ✅ keeps schema
    return list(tables)


# def extract_joins(ast):
#     joins = []
#     for join in ast.find_all(exp.Join):
#         if join.on:
#             joins.append(join.on.sql())
#     return joins

def extract_joins(ast):
    joins = []

    for join in ast.find_all(exp.Join):
        on_clause = join.args.get("on")

        if on_clause and hasattr(on_clause, "sql"):
            joins.append(on_clause.sql())

    return joins


def extract_filters(ast):
    where = ast.find(exp.Where)
    return where.sql() if where else None


def extract_column_lineage(ast):
    lineage = []

    select = ast.find(exp.Select)
    if not select:
        return lineage

    for projection in select.expressions:
        target = projection.alias_or_name

        sources = []
        for col in projection.find_all(exp.Column):
            if col.table:
                sources.append(f"{col.table}.{col.name}")
            else:
                sources.append(col.name)

        lineage.append({
            "target": target,
            "sources": list(set(sources))
        })

    return lineage


# -----------------------------
# PARSERS
# -----------------------------
def parse_insert(stmt: str):
    ast = parse_sql(stmt)

    result = {
        "type": "INSERT",
        "target_table": None,
        "source_tables": [],
        "joins": [],
        "filters": None,
        "column_lineage": []
    }

    target = re.search(r'INSERT INTO\s+([\w\."]+)', stmt, re.IGNORECASE)
    result["target_table"] = target.group(1).replace('"', '') if target else None

    if ast:
        result["source_tables"] = extract_tables(ast)
        result["joins"] = extract_joins(ast)
        result["filters"] = extract_filters(ast)
        result["column_lineage"] = extract_column_lineage(ast)

    return result


def parse_truncate(stmt: str):
    match = re.search(r'TRUNCATE TABLE\s+([\w\."]+)', stmt, re.IGNORECASE)
    return {
        "type": "TRUNCATE",
        "truncate_table": match.group(1).replace('"', '') if match else None
    }


def parse_procedure(proc_text: str):
    data = {
        "procedure": None,
        "operations": []
    }

    name = re.search(r'PROCEDURE\s+"?([\w]+)"?', proc_text, re.IGNORECASE)
    if name:
        data["procedure"] = name.group(1)

    statements = extract_statements(proc_text)

    for stmt in statements:
        stmt_upper = stmt.upper()

        if "INSERT INTO" in stmt_upper:
            data["operations"].append(parse_insert(stmt))

        elif "TRUNCATE TABLE" in stmt_upper:
            data["operations"].append(parse_truncate(stmt))

    return data


# -----------------------------
# CHUNK BUILDERS
# -----------------------------
def chunk_overview(data):
    ops = [op["type"] for op in data["operations"]]
    return f"Procedure {data['procedure']} performs operations: {', '.join(ops)}."


def chunk_execution_flow(data):
    steps = []
    for i, op in enumerate(data["operations"], 1):
        if op["type"] == "TRUNCATE":
            steps.append(f"{i}. Truncate {op['truncate_table']}")
        elif op["type"] == "INSERT":
            steps.append(f"{i}. Load {op['target_table']}")
    return "Execution Flow: " + " → ".join(steps)


def chunk_tables(data):
    tables = set()
    for op in data["operations"]:
        if op.get("target_table"):
            tables.add(op["target_table"])
        if op.get("truncate_table"):
            tables.add(op["truncate_table"])
        tables.update(op.get("source_tables", []))
    return [f"Table involved: {t}" for t in tables]


def chunk_joins(data):
    return [f"Join condition: {j}" for op in data["operations"] for j in op.get("joins", [])]


def chunk_filters(data):
    return [f"Filter applied: {op['filters']}" for op in data["operations"] if op.get("filters")]


def chunk_lineage(data):
    return [f"{src} → {op.get('target_table')}" for op in data["operations"] for src in op.get("source_tables", [])]


def chunk_column_lineage(data):
    chunks = []
    for op in data["operations"]:
        tgt = op.get("target_table")
        for col in op.get("column_lineage", []):
            for src in col["sources"]:
                chunks.append(f"{tgt}.{col['target']} ← {src}")
    return chunks


# -----------------------------
# DOCUMENT BUILDER
# -----------------------------
def build_documents(parsed_proc):
    docs = []

    def add(content, type_):
        docs.append({
            "content": content,
            "metadata": {
                "type": type_,
                "procedure": parsed_proc["procedure"]
            }
        })

    add(chunk_overview(parsed_proc), "overview")
    add(chunk_execution_flow(parsed_proc), "flow")

    for c in chunk_tables(parsed_proc):
        add(c, "table")

    for c in chunk_lineage(parsed_proc):
        add(c, "lineage")

    for c in chunk_column_lineage(parsed_proc):
        add(c, "column_lineage")

    for c in chunk_joins(parsed_proc):
        add(c, "join")

    for c in chunk_filters(parsed_proc):
        add(c, "filter")

    return docs


# -----------------------------
# MAIN
# -----------------------------
def process_file(input_path: str):
    with open(input_path, "r", encoding="utf-8") as f:
        content = f.read()

    normalized = normalize_sql(content)
    procedures = split_procedures(normalized)

    all_docs = []

    for proc in procedures:
        parsed = parse_procedure(proc)
        docs = build_documents(parsed)
        all_docs.extend(docs)

    return all_docs


def main():
    input_path = "files/sample_4.pls"
    output_path = "files/result_4_new.json"

    docs = process_file(input_path)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2)

    print(f"Processed {len(docs)} chunks and saved to {output_path}")


if __name__ == "__main__":
    main()