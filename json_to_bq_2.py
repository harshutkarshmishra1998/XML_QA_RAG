import re
import time
import hashlib
from typing import List
from dataclasses import dataclass
from groq import Groq
import api_keys

PROJECT_ID = "your_project"
DATASET = "your_dataset"

client = Groq()

FORBIDDEN = ["LOOP", "WHILE", "DECLARE", "BEGIN", "END;"]

@dataclass
class Block:
    type: str
    content: str


def load_pls(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()


def normalize_sql(sql: str) -> str:
    sql = re.sub(r"--.*", "", sql)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql


# ----------- CRITICAL FIX: DO NOT SPLIT SQL NAIVELY -----------
def extract_main_sql(sql: str) -> List[Block]:
    blocks = []

    # Extract EXECUTE IMMEDIATE SQL
    dynamic_sql = re.findall(r"EXECUTE IMMEDIATE '(.*?)'", sql, re.DOTALL)
    for stmt in dynamic_sql:
        blocks.append(Block("dynamic", stmt))

    # Extract TRUNCATE manually
    if "truncate table" in sql.lower():
        match = re.search(r"truncate table\s+(\w+\.\w+)", sql, re.IGNORECASE)
        if match:
            blocks.append(Block("truncate", f"TRUNCATE TABLE {match.group(1)}"))

    # Extract INSERT SELECT (full block)
    inserts = re.findall(r"INSERT INTO .*?SELECT .*?;", sql, re.DOTALL | re.IGNORECASE)
    for stmt in inserts:
        blocks.append(Block("insert_select", stmt))

    return blocks


# ----------- FIX: ORACLE JOIN (+) -----------
def convert_outer_join(sql: str) -> str:
    pattern = r"(\w+\.\w+)\s*=\s*(\w+\.\w+)\(\+\)"
    matches = re.findall(pattern, sql)

    for left, right in matches:
        table = right.split(".")[0]
        sql = re.sub(
            rf"{left}\s*=\s*{right}\(\+\)",
            f"{left} = {right}",
            sql
        )
        sql += f"\nLEFT JOIN {table} ON {left} = {right}"

    return sql


# ----------- FIX: FUNCTION MAPPING -----------
def apply_deterministic_rules(sql: str) -> str:
    rules = [
        (r"NVL\((.*?)\,(.*?)\)", r"IFNULL(\1,\2)"),
        (r"SYSTIMESTAMP", "CURRENT_TIMESTAMP()"),
        (r"SYSDATE", "CURRENT_TIMESTAMP()"),
        (r"\|\|", "CONCAT"),
        (r"\(\+\)", ""),
    ]

    for pattern, repl in rules:
        sql = re.sub(pattern, repl, sql, flags=re.IGNORECASE)

    return sql


# ----------- FIX: FULL TABLE QUALIFICATION -----------
def qualify_tables(sql: str) -> str:
    def repl(match):
        table = match.group(1)
        return f"`{PROJECT_ID}.{DATASET}.{table}`"

    sql = re.sub(r"\bFROM\s+(\w+)", repl, sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bJOIN\s+(\w+)", repl, sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bINTO\s+(\w+)", repl, sql, flags=re.IGNORECASE)

    return sql


# ----------- LLM WITH SAFETY -----------
cache = {}

def hash_block(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def validate_sql(sql: str):
    if any(k in sql.upper() for k in FORBIDDEN):
        raise ValueError("Procedural SQL detected")

    if not any(k in sql.upper() for k in ["SELECT", "INSERT", "MERGE", "DELETE"]):
        raise ValueError("Invalid SQL output")


def llm_transform(sql: str) -> str:
    key = hash_block(sql)
    if key in cache:
        return cache[key]

    prompt = f"""
Convert Oracle SQL to BigQuery SQL.

STRICT:
- No procedural constructs
- Fix joins
- Fix syntax
- Output ONLY SQL

INPUT:
{sql}
"""

    for _ in range(3):
        try:
            res = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                temperature=0,
                messages=[
                    {"role": "system", "content": "SQL converter"},
                    {"role": "user", "content": prompt}
                ]
            )

            output = res.choices[0].message.content.strip() #type: ignore

            validate_sql(output)

            cache[key] = output
            return output

        except:
            time.sleep(1)

    return f"-- FAILED\n{sql}"


# ----------- MAIN PIPELINE -----------
def convert_pls_to_bq(file_path: str) -> List[str]:
    raw = load_pls(file_path)
    sql = normalize_sql(raw)

    blocks = extract_main_sql(sql)

    results = []

    for block in blocks:
        s = block.content

        s = apply_deterministic_rules(s)
        s = convert_outer_join(s)
        s = qualify_tables(s)

        # LLM only for complex SQL
        if "SELECT" in s.upper():
            s = llm_transform(s)

        results.append(s)

    return results


def save_output(sql_list: List[str], output_path: str):
    with open(output_path, "w", encoding="utf-8") as f:
        for stmt in sql_list:
            f.write(stmt + "\n\n")

values = [0, 1, 2, 3, 4, 5]

if __name__ == "__main__":
    for i in values:
        input_file = f"files/pls_sample_{i}.pls"
        output_file = f"files/bq_script_{i}.sql"

        result = convert_pls_to_bq(input_file)
        save_output(result, output_file)

        print(f"pls_sample_{i}: Done")
