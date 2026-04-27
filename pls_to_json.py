import re
import json
import argparse
from pathlib import Path


def normalize_sql(text: str) -> str:
    text = re.sub(r'--.*', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def split_procedures(text: str):
    pattern = r'CREATE OR REPLACE.*?PROCEDURE.*?END\s+\w+\s*;'
    matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
    return matches if matches else [text]


def extract_statements(proc_text: str):
    pattern = r'(TRUNCATE TABLE.*?;|INSERT INTO.*?;|UPDATE.*?;|DELETE FROM.*?;)'
    return re.findall(pattern, proc_text, re.IGNORECASE | re.DOTALL)


def parse_insert(stmt: str):
    result = {"type": "INSERT"}

    target = re.search(r'INSERT INTO\s+([\w\."]+)', stmt, re.IGNORECASE)
    result["target_table"] = target.group(1).replace('"', '') if target else None

    cols = re.search(r'\((.*?)\)\s*SELECT', stmt, re.DOTALL | re.IGNORECASE)
    if cols:
        result["columns"] = [c.strip() for c in cols.group(1).split(",")]
    else:
        result["columns"] = []

    sources = re.findall(r'(FROM|JOIN)\s+([\w\."]+)', stmt, re.IGNORECASE)
    result["source_tables"] = list(set([s[1].replace('"', '') for s in sources]))

    where_match = re.search(r'WHERE (.*?)(GROUP BY|ORDER BY|;)', stmt, re.IGNORECASE | re.DOTALL)
    result["filters"] = where_match.group(1).strip() if where_match else None

    return result


def parse_truncate(stmt: str):
    match = re.search(r'TRUNCATE TABLE\s+([\w\."]+)', stmt, re.IGNORECASE)
    return {
        "type": "TRUNCATE",
        "truncate_table": match.group(1).replace('"', '') if match else None
    }


def parse_update(stmt: str):
    match = re.search(r'UPDATE\s+([\w\."]+)', stmt, re.IGNORECASE)
    return {
        "type": "UPDATE",
        "target_table": match.group(1).replace('"', '') if match else None
    }


def parse_delete(stmt: str):
    match = re.search(r'DELETE FROM\s+([\w\."]+)', stmt, re.IGNORECASE)
    return {
        "type": "DELETE",
        "target_table": match.group(1).replace('"', '') if match else None
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

        elif "UPDATE" in stmt_upper:
            data["operations"].append(parse_update(stmt))

        elif "DELETE FROM" in stmt_upper:
            data["operations"].append(parse_delete(stmt))

    return data


def chunk_overview(data):
    ops = [op["type"] for op in data["operations"]]
    return f"Procedure {data['procedure']} performs operations: {', '.join(ops)}."


def chunk_operations(data):
    chunks = []

    for op in data["operations"]:
        if op["type"] == "INSERT":
            chunks.append(
                f"Inserts into {op['target_table']} from sources {', '.join(op.get('source_tables', []))}."
            )

        elif op["type"] == "TRUNCATE":
            chunks.append(
                f"Truncates table {op['truncate_table']} before data load."
            )

        elif op["type"] == "UPDATE":
            chunks.append(
                f"Updates records in table {op['target_table']}."
            )

        elif op["type"] == "DELETE":
            chunks.append(
                f"Deletes records from table {op['target_table']}."
            )

    return chunks


def chunk_tables(data):
    tables = set()

    for op in data["operations"]:
        if "target_table" in op and op["target_table"]:
            tables.add(op["target_table"])

        if "truncate_table" in op and op["truncate_table"]:
            tables.add(op["truncate_table"])

        if "source_tables" in op:
            tables.update(op["source_tables"])

    return [f"Table involved: {t}" for t in tables]


def chunk_columns(data):
    chunks = []

    for op in data["operations"]:
        if op["type"] == "INSERT" and op.get("columns"):
            chunks.append(
                f"Columns inserted into {op['target_table']}: {', '.join(op['columns'])}"
            )

    return chunks


def build_documents(parsed_proc):
    docs = []

    docs.append({
        "content": chunk_overview(parsed_proc),
        "metadata": {
            "type": "overview",
            "procedure": parsed_proc["procedure"]
        }
    })

    for chunk in chunk_operations(parsed_proc):
        docs.append({
            "content": chunk,
            "metadata": {
                "type": "operation",
                "procedure": parsed_proc["procedure"]
            }
        })

    for chunk in chunk_tables(parsed_proc):
        docs.append({
            "content": chunk,
            "metadata": {
                "type": "table",
                "procedure": parsed_proc["procedure"]
            }
        })

    for chunk in chunk_columns(parsed_proc):
        docs.append({
            "content": chunk,
            "metadata": {
                "type": "column",
                "procedure": parsed_proc["procedure"]
            }
        })

    return docs


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
    output_path = "files/result_4.json"

    docs = process_file(input_path)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2)

    print(f"Processed {len(docs)} chunks and saved to {output_path}")


if __name__ == "__main__":
    main()