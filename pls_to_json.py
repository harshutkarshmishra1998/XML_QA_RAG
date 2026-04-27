import re
import json
from pathlib import Path
from sqlglot import parse_one, exp


# -----------------------------
# CLEAN HELPERS
# -----------------------------
def clean_table_name(name):
    name = name.replace('"', '').strip().lower()
    if "." not in name:
        return f"vnadsprd.{name}"
    return name


def is_valid_source(src):
    if not src:
        return False

    src = src.upper()

    if src in ["NAUTILUS", "AVAILABLE", "MSERI"]:
        return False

    if len(src) <= 2:
        return False

    return True


# -----------------------------
# DYNAMIC SQL
# -----------------------------
def extract_dynamic_sql(stmt):
    if "EXECUTE IMMEDIATE" not in stmt.upper():
        return stmt
    parts = re.findall(r"'([^']*)'", stmt, re.DOTALL)
    return " ".join(parts) if parts else stmt


# -----------------------------
# NORMALIZE INPUT
# -----------------------------
def normalize_sql(text):
    text = re.sub(r'--.*', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def split_procedures(text):
    pattern = r'CREATE OR REPLACE.*?PROCEDURE.*?END\s+\w+\s*;'
    matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
    return matches if matches else [text]


def extract_statements(proc_text):
    statements, current = [], ""
    paren, in_string = 0, False

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
# SQL PARSE
# -----------------------------
def parse_sql(stmt):
    try:
        return parse_one(extract_dynamic_sql(stmt), read="oracle")
    except:
        return None


# -----------------------------
# AST HELPERS (FINAL)
# -----------------------------
def extract_tables(ast):
    return list({
        clean_table_name(t.this.sql())
        for t in ast.find_all(exp.Table)
    })


def extract_ctes(ast):
    cte_map = {}
    for cte in ast.find_all(exp.CTE):
        name = cte.alias
        tables = [
            clean_table_name(t.this.sql())
            for t in cte.this.find_all(exp.Table)
        ]
        if name:
            cte_map[name] = tables
    return cte_map


def extract_alias_map(ast, cte_map):
    alias_map = {}

    # direct table aliases
    for t in ast.find_all(exp.Table):
        if t.alias:
            alias_map[t.alias] = [clean_table_name(t.this.sql())]

    # subqueries (multi-table)
    for sub in ast.find_all(exp.Subquery):
        if sub.alias:
            tables = [
                clean_table_name(t.this.sql())
                for t in sub.this.find_all(exp.Table)
            ]
            if tables:
                alias_map[sub.alias] = tables

    # include CTEs
    alias_map.update(cte_map)

    return alias_map


def resolve_alias(alias, alias_map):
    if alias in alias_map:
        return alias_map[alias]
    return [clean_table_name(alias)]


# -----------------------------
# SQL REWRITE (NEW)
# -----------------------------
def rewrite_expression(expr_sql, alias_map):
    """
    Replace aliases in SQL string with actual table names
    """
    for alias, tables in alias_map.items():
        for t in tables:
            expr_sql = re.sub(rf"\b{alias}\.", f"{t}.", expr_sql)
    return expr_sql


def extract_joins(ast, alias_map):
    joins = []

    for j in ast.find_all(exp.Join):
        on = j.args.get("on")
        if on and hasattr(on, "sql"):
            sql = rewrite_expression(on.sql(), alias_map)
            joins.append(sql)

    return list(set(joins))


def extract_filters(ast, alias_map):
    where = ast.find(exp.Where)
    if where:
        return rewrite_expression(where.sql(), alias_map)
    return None


# -----------------------------
# TRANSFORMATION DETECTION
# -----------------------------
def detect_transformation(expr_sql):
    expr_sql = expr_sql.upper()

    return any(fn in expr_sql for fn in [
        "CAST(", "NVL(", "CASE ", "COALESCE(", "||", "+", "-"
    ])


# -----------------------------
# COLUMN LINEAGE (FINAL)
# -----------------------------
def extract_column_lineage(ast, alias_map, known_tables):
    lineage = []
    select = ast.find(exp.Select)

    if not select:
        return lineage

    for proj in select.expressions:
        target = proj.alias_or_name
        expr_sql = proj.sql()
        sources = set()

        for col in proj.find_all(exp.Column):
            table = col.table
            column = col.name

            if table:
                resolved_tables = resolve_alias(table, alias_map)

                for t in resolved_tables:
                    if t not in known_tables:
                        continue

                    full = f"{t}.{column}"

                    if is_valid_source(full):
                        sources.add(full)

        if sources:
            lineage.append({
                "target": target,
                "sources": list(sources),
                "transformation": detect_transformation(expr_sql)
            })

    return lineage


# -----------------------------
# PARSERS
# -----------------------------
def parse_insert(stmt):
    ast = parse_sql(stmt)

    result = {
        "type": "INSERT",
        "target_table": None,
        "source_tables": [],
        "joins": [],
        "filters": None,
        "column_lineage": []
    }

    tgt = re.search(r'INSERT INTO\s+([\w\."]+)', stmt, re.IGNORECASE)
    if tgt:
        result["target_table"] = clean_table_name(tgt.group(1))

    if ast:
        cte_map = extract_ctes(ast)
        alias_map = extract_alias_map(ast, cte_map)

        tables = extract_tables(ast)
        result["source_tables"] = tables

        result["joins"] = extract_joins(ast, alias_map)
        result["filters"] = extract_filters(ast, alias_map)
        result["column_lineage"] = extract_column_lineage(ast, alias_map, tables)

    return result


def parse_truncate(stmt):
    match = re.search(r'TRUNCATE TABLE\s+([\w\."]+)', stmt, re.IGNORECASE)
    return {
        "type": "TRUNCATE",
        "truncate_table": clean_table_name(match.group(1)) if match else None
    }


def parse_procedure(proc_text):
    data = {"procedure": None, "operations": []}

    name = re.search(r'PROCEDURE\s+"?([\w]+)"?', proc_text, re.IGNORECASE)
    if name:
        data["procedure"] = name.group(1)

    for stmt in extract_statements(proc_text):
        clean = extract_dynamic_sql(stmt)
        stmt_upper = clean.upper()

        if "INSERT INTO" in stmt_upper:
            data["operations"].append(parse_insert(stmt))

        elif "TRUNCATE TABLE" in stmt_upper:
            data["operations"].append(parse_truncate(stmt))

    return data


# -----------------------------
# CHUNKS
# -----------------------------
def chunk_tables(data):
    tables = set()

    for op in data["operations"]:
        tables.update(op.get("source_tables", []))
        if op.get("target_table"):
            tables.add(op["target_table"])

    return [f"Table involved: {t}" for t in sorted(tables)]


def chunk_lineage(data):
    chunks = set()

    for op in data["operations"]:
        tgt = op.get("target_table")

        for src in op.get("source_tables", []):
            if src != tgt:
                chunks.add(f"{src} → {tgt}")

    return list(chunks)


def chunk_column_lineage(data):
    chunks = set()

    for op in data["operations"]:
        tgt = op.get("target_table")

        for col in op.get("column_lineage", []):
            for src in col["sources"]:
                suffix = " (transformed)" if col["transformation"] else ""
                chunks.add(f"{tgt}.{col['target']} ← {src}{suffix}")

    return list(chunks)


def chunk_joins(data):
    return list(set(
        f"Join condition: {j}"
        for op in data["operations"]
        for j in op.get("joins", [])
    ))


def chunk_filters(data):
    return list(set(
        f"Filter applied: {op['filters']}"
        for op in data["operations"]
        if op.get("filters")
    ))


def chunk_overview(data):
    ops = [op["type"] for op in data["operations"]]
    return f"Procedure {data['procedure']} performs operations: {', '.join(ops)}."


def chunk_flow(data):
    steps = []

    for i, op in enumerate(data["operations"], 1):
        if op["type"] == "TRUNCATE":
            steps.append(f"{i}. Truncate {op['truncate_table']}")
        elif op["type"] == "INSERT":
            steps.append(f"{i}. Load {op['target_table']}")

    return "Execution Flow: " + " → ".join(steps)


# -----------------------------
# BUILD DOCS
# -----------------------------
def build_documents(parsed):
    docs = []

    def add(c, t):
        docs.append({
            "content": c,
            "metadata": {"type": t, "procedure": parsed["procedure"]}
        })

    add(chunk_overview(parsed), "overview")
    add(chunk_flow(parsed), "flow")

    for c in chunk_tables(parsed): add(c, "table")
    for c in chunk_lineage(parsed): add(c, "lineage")
    for c in chunk_column_lineage(parsed): add(c, "column_lineage")
    for c in chunk_joins(parsed): add(c, "join")
    for c in chunk_filters(parsed): add(c, "filter")

    return docs


# -----------------------------
# MAIN
# -----------------------------
def process_file(input_path):
    with open(input_path, "r", encoding="utf-8") as f:
        content = f.read()

    docs = []

    for proc in split_procedures(normalize_sql(content)):
        parsed = parse_procedure(proc)
        docs.extend(build_documents(parsed))

    return docs


def main():
    input_path = "files/sample_4.pls"
    output_path = "files/result_final_elite.json"

    docs = process_file(input_path)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2)

    print(f"Processed {len(docs)} chunks and saved to {output_path}")


if __name__ == "__main__":
    main()