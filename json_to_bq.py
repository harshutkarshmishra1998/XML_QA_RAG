import json
import re
from pathlib import Path
from groq import Groq
from sqlglot import parse_one, exp
import api_keys

client = Groq()

# -----------------------------
# LLM FALLBACK
# -----------------------------
def convert_sql_llm(sql):
    prompt = f"""
Convert Oracle SQL to BigQuery SQL.

Rules:
- Preserve JOIN structure EXACTLY
- Do NOT change driving table
- Convert Oracle (+) joins to LEFT/RIGHT JOIN
- Replace NVL with IFNULL
- Output ONLY SQL
- DO NOT convert joins into comma-separated tables
- ALWAYS use explicit JOIN ... ON syntax
- NEVER use FROM A, B, C style joins

SQL:
{sql}
"""
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content.strip()

def has_comma_join(sql):
    return bool(re.search(r"FROM\s+[^()]+,", sql, re.IGNORECASE))

# -----------------------------
# CLEAN SQL OUTPUT
# -----------------------------
def clean_llm_sql(sql):
    sql = sql.strip()

    sql = re.sub(r"^```sql", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"```$", "", sql)

    sql = re.sub(r"(?<!')\bNAUTILUS\b(?!')", "'NAUTILUS'", sql)
    sql = re.sub(r"\bN\s*/\s*A\b", "'N/A'", sql)
    sql = re.sub(r"'{2,}", "'", sql)
    sql = re.sub(r"\bCURRENT_TIMESTAMP\b(?!\()", "CURRENT_TIMESTAMP()", sql)

    return sql.strip()


# -----------------------------
# HELPERS
# -----------------------------
def clean_sql(text):
    text = re.sub(r'--.*', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def split_statements(text):
    statements, current = [], ""
    paren, in_string = 0, False

    for ch in text:
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


def extract_dynamic_sql(stmt):
    if "EXECUTE IMMEDIATE" not in stmt.upper():
        return stmt
    parts = re.findall(r"'([^']*)'", stmt, re.DOTALL)
    return " ".join(parts) if parts else stmt


def normalize_table(t):
    return t.lower().replace('"', '').strip()


def extract_target_table(stmt):
    match = re.search(r'INSERT INTO\s+([\w\."]+)', stmt, re.IGNORECASE)
    return normalize_table(match.group(1)) if match else None


def extract_truncate_table(stmt):
    match = re.search(r'TRUNCATE TABLE\s+([\w\."]+)', stmt, re.IGNORECASE)
    return normalize_table(match.group(1)) if match else None


def extract_select_sql(stmt):
    try:
        ast = parse_one(stmt, read="oracle")
        select = ast.find(exp.Select)
        return select.sql() if select else None
    except:
        return None


# -----------------------------
# COMPLEXITY DETECTOR
# -----------------------------
def is_complex_query(sql):
    sql_upper = sql.upper()

    return any([
        "(+)" in sql,
        "JOIN" in sql and "WHERE" in sql,   # mixed join styles
        "SELECT" in sql and "FROM (" in sql, # subquery
        "GROUP BY" in sql,
    ])


# -----------------------------
# LOAD ORDER FROM JSON
# -----------------------------
def extract_execution_order(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    flow = None
    for item in data:
        if item["metadata"]["type"] == "flow":
            flow = item["content"]
            break

    if not flow:
        return []

    steps = flow.replace("Execution Flow:", "").split("→")

    ordered = []
    for step in steps:
        step = re.sub(r'^\d+\.\s*', '', step.strip().lower())

        if "truncate" in step:
            table = step.split("truncate")[-1].strip()
            ordered.append(("TRUNCATE", normalize_table(table)))

        elif "load" in step:
            table = step.split("load")[-1].strip()
            ordered.append(("INSERT", normalize_table(table)))

    return ordered


# -----------------------------
# MAIN CONVERTER
# -----------------------------
def convert_using_json_order(pls_path, json_path, output_path):
    with open(pls_path, "r", encoding="utf-8") as f:
        content = clean_sql(f.read())

    statements = split_statements(content)

    sql_ops = []

    for stmt in statements:
        stmt = extract_dynamic_sql(stmt)
        stmt_upper = stmt.upper()

        if "TRUNCATE TABLE" in stmt_upper:
            table = extract_truncate_table(stmt)
            sql_ops.append(("TRUNCATE", table, stmt))

        elif "INSERT INTO" in stmt_upper:
            table = extract_target_table(stmt)
            sql_ops.append(("INSERT", table, stmt))

    ordered_ops = extract_execution_order(json_path)

    final_sql = []
    step = 1

    for op_type, table in ordered_ops:
        found = False

        for sql_type, sql_table, stmt in sql_ops:
            if op_type == sql_type and normalize_table(table) == normalize_table(sql_table):

                if op_type == "TRUNCATE":
                    final_sql.append(
                        f"-- Step {step}\nDELETE FROM `{sql_table}` WHERE TRUE;"
                    )

                elif op_type == "INSERT":
                    select_sql = extract_select_sql(stmt)

                    if not select_sql:
                        print(f"WARNING: No SELECT found for {table}")
                        continue

                    # 🔥 KEY FIX: use LLM for complex queries
                    if is_complex_query(select_sql):
                        print(f"Using LLM for complex query: {table}")
                        bq_select = clean_llm_sql(convert_sql_llm(select_sql))

                        # 🚨 safety check
                        if has_comma_join(bq_select):
                            print("⚠️ LLM produced comma joins, retrying with stricter prompt")

                            bq_select = clean_llm_sql(convert_sql_llm(
                                "STRICT MODE:\nConvert using ONLY explicit JOIN syntax.\n\n" + select_sql
                            ))
                    else:
                        try:
                            bq_select = parse_one(select_sql, read="oracle").sql(dialect="bigquery")
                        except:
                            bq_select = clean_llm_sql(convert_sql_llm(select_sql))

                    final_sql.append(f"""-- Step {step}
INSERT INTO `{sql_table}`
{bq_select};
""")

                step += 1
                found = True
                break

        if not found:
            print(f"WARNING: No match found for {op_type} {table}")

    script = "BEGIN\n\n" + "\n\n".join(final_sql) + "\n\nEND;"

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(script)

    print(f"BigQuery script saved to: {output_path}")


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    convert_using_json_order(
        pls_path="files/sample_4.pls",
        json_path="files/result_4.json",
        output_path="files/bq_script.sql"
    )