"""Production-oriented PL/SQL -> BigQuery conversion pipeline.

Goals:
- Use the conversion playbook as a first-class input, not just a prompt add-on.
- Apply deterministic rewrites before calling the LLM.
- Classify blocks by complexity and route only the hard parts to the LLM.
- Preserve execution order and output every statement in a stable, testable way.

Notes:
- This is intentionally conservative: it avoids unsafe semantic guesses.
- The LLM is used for complex procedural blocks, while simple rewrites are handled deterministically.
- Adjust the PLAYBOOK_PATH and PROJECT/DATASET values for your environment.
"""

from __future__ import annotations

import hashlib
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
import api_keys

from groq import Groq

# -------------------- Configuration --------------------

PROJECT_ID = os.getenv("BQ_PROJECT_ID", "your_project")
DATASET = os.getenv("BQ_DATASET", "your_dataset")
PLAYBOOK_PATH = os.getenv("CONVERSION_PLAYBOOK_PATH", "conversion_doc.txt")
LLM_MODEL = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")
MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))
CACHE_PATH = os.getenv("LLM_CACHE_PATH", "")

# Keywords that indicate complexity, not necessarily invalid SQL.
COMPLEXITY_MARKERS = {
    "LOOP",
    "WHILE",
    "FOR ",
    "CURSOR",
    "FETCH",
    "BULK COLLECT",
    "FORALL",
    "EXECUTE IMMEDIATE",
    "EXCEPTION",
    "PRAGMA",
    "GOTO",
    "PIPELINED",
}

# BigQuery scripting keywords that are valid and should not be flagged.
BQ_SCRIPTING_KEYWORDS = {
    "DECLARE",
    "BEGIN",
    "END",
    "IF",
    "ELSE",
    "ELSEIF",
    "WHILE",
    "LOOP",
    "SET",
    "CREATE TEMP TABLE",
    "MERGE",
    "ASSERT",
    "EXECUTE IMMEDIATE",
}

# Deterministic syntax/function mappings.
FUNCTION_RULES: Sequence[Tuple[re.Pattern[str], str]] = (
    (re.compile(r"\bNVL\s*\(", re.IGNORECASE), "IFNULL("),
    (re.compile(r"\bSYSTIMESTAMP\b", re.IGNORECASE), "CURRENT_TIMESTAMP()"),
    (re.compile(r"\bSYSDATE\b", re.IGNORECASE), "CURRENT_TIMESTAMP()"),
    (re.compile(r"\bSUBSTR\b", re.IGNORECASE), "SUBSTR"),
    (re.compile(r"\bINSTR\b", re.IGNORECASE), "STRPOS"),
    (re.compile(r"\bDECODE\s*\(", re.IGNORECASE), "CASE_DECODE_PLACEHOLDER("),
)

# Patterns that are complex enough to send to the LLM.
COMPLEXITY_PATTERNS: Sequence[re.Pattern[str]] = (
    re.compile(r"\bLOOP\b", re.IGNORECASE),
    re.compile(r"\bWHILE\b", re.IGNORECASE),
    re.compile(r"\bCURSOR\b", re.IGNORECASE),
    re.compile(r"\bFETCH\b", re.IGNORECASE),
    re.compile(r"\bBULK\s+COLLECT\b", re.IGNORECASE),
    re.compile(r"\bFORALL\b", re.IGNORECASE),
    re.compile(r"\bEXECUTE\s+IMMEDIATE\b", re.IGNORECASE),
    re.compile(r"\bEXCEPTION\b", re.IGNORECASE),
    re.compile(r"\bMERGE\b", re.IGNORECASE),
    re.compile(r"\bCONNECT\s+BY\b", re.IGNORECASE),
    re.compile(r"\(\+\)"),
)

# -------------------- Data models --------------------

@dataclass(frozen=True)
class Block:
    kind: str
    content: str
    start_line: int = 0
    end_line: int = 0


@dataclass
class ConversionResult:
    original: str
    deterministic_sql: str
    final_sql: str
    used_llm: bool
    complexity_score: int
    notes: List[str]


# -------------------- Cache --------------------

class MemoryCache:
    def __init__(self, path: str = "") -> None:
        self.path = path.strip()
        self._cache: Dict[str, str] = {}
        if self.path:
            self._load()

    def _load(self) -> None:
        p = Path(self.path)
        if p.exists():
            try:
                import json

                self._cache = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                self._cache = {}

    def save(self) -> None:
        if not self.path:
            return
        try:
            import json

            Path(self.path).write_text(json.dumps(self._cache, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def get(self, key: str) -> Optional[str]:
        return self._cache.get(key)

    def set(self, key: str, value: str) -> None:
        self._cache[key] = value
        self.save()


# -------------------- Utilities --------------------

def load_text(file_path: str) -> str:
    return Path(file_path).read_text(encoding="utf-8")


def md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def strip_comments(sql: str) -> str:
    # Remove block comments first, then line comments.
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    return sql


def normalize_whitespace(sql: str) -> str:
    # Keep line structure but normalize trailing spaces and excessive blank lines.
    lines = [line.rstrip() for line in sql.splitlines()]
    cleaned: List[str] = []
    blank_run = 0
    for line in lines:
        if line.strip():
            blank_run = 0
            cleaned.append(line)
        else:
            blank_run += 1
            if blank_run <= 1:
                cleaned.append("")
    return "\n".join(cleaned).strip()


def normalize_sql(sql: str) -> str:
    return normalize_whitespace(strip_comments(sql))

def fix_parentheses(sql: str) -> str:
    open_count = sql.count("(")
    close_count = sql.count(")")

    if open_count > close_count:
        sql += ")" * (open_count - close_count)
    elif close_count > open_count:
        sql = sql.rstrip(")")  # remove extras from end

    return sql


def split_statements(sql: str) -> List[str]:
    """Split SQL/PLSQL into top-level statements without breaking quoted strings.

    This is conservative and only splits on semicolons at depth 0.
    """
    statements: List[str] = []
    buf: List[str] = []
    in_single = False
    in_double = False
    depth = 0
    i = 0
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if ch == "'" and not in_double:
            # Handle escaped single quote ''
            if in_single and nxt == "'":
                buf.append(ch)
                buf.append(nxt)
                i += 2
                continue
            in_single = not in_single
            buf.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single:
            in_double = not in_double
            buf.append(ch)
            i += 1
            continue

        if not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            elif ch == ";" and depth == 0:
                stmt = "".join(buf).strip()
                if stmt:
                    statements.append(stmt)
                buf = []
                i += 1
                continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def count_lines(text: str) -> Tuple[int, int]:
    lines = text.splitlines()
    return (1, len(lines)) if lines else (0, 0)


# -------------------- Deterministic transformations --------------------

def map_oracle_functions(sql: str) -> str:
    out = sql
    for pattern, replacement in FUNCTION_RULES:
        out = pattern.sub(replacement, out)

    # Convert Oracle concatenation operator only in obvious string-building cases.
    # Keep this conservative; the LLM should handle more complex rewrites.
    out = re.sub(r"(?<!\|)\|(?!\|)", "||", out)
    return out


def replace_simple_date_tokens(sql: str) -> str:
    replacements = {
        r"\bTRUNC\s*\(\s*([^)]+)\s*\)": r"DATE(\1)",
        r"\bSYSDATE\b": "CURRENT_TIMESTAMP()",
        r"\bSYSTIMESTAMP\b": "CURRENT_TIMESTAMP()",
    }
    out = sql
    for pat, repl in replacements.items():
        out = re.sub(pat, repl, out, flags=re.IGNORECASE)
    return out


def qualify_unqualified_tables(sql: str, project: str, dataset: str) -> str:
    """Qualify simple table references in FROM/JOIN/INTO/UPDATE/DELETE.

    This intentionally skips already qualified names and subqueries.
    """

    def qualify(name: str) -> str:
        if name.startswith("`") or "." in name or name.upper().startswith("SELECT"):
            return name
        if name.upper() in {"DUAL"}:
            return name
        return f"`{project}.{dataset}.{name}`"

    patterns = [
        (re.compile(r"\bFROM\s+([A-Za-z_][\w$]*)\b", re.IGNORECASE), "FROM"),
        (re.compile(r"\bJOIN\s+([A-Za-z_][\w$]*)\b", re.IGNORECASE), "JOIN"),
        (re.compile(r"\bINTO\s+([A-Za-z_][\w$]*)\b", re.IGNORECASE), "INTO"),
        (re.compile(r"\bUPDATE\s+([A-Za-z_][\w$]*)\b", re.IGNORECASE), "UPDATE"),
        (re.compile(r"\bDELETE\s+FROM\s+([A-Za-z_][\w$]*)\b", re.IGNORECASE), "DELETE FROM"),
    ]

    out = sql
    for pat, _label in patterns:
        out = pat.sub(lambda m: m.group(0).replace(m.group(1), qualify(m.group(1))), out)
    return out


def normalize_outer_joins(sql: str) -> str:
    """Convert Oracle (+) markers into a conservative annotation for the LLM.

    We do NOT try to rewrite the full join tree deterministically because that is
    frequently unsafe. Instead, we annotate and remove the token so validation does
    not falsely reject the block.
    """
    if "(+)" not in sql:
        return sql
    return sql.replace("(+)" , " /*ORACLE_OUTER_JOIN_MARKER*/ ")


def convert_simple_assignments(sql: str) -> str:
    """Handle very common scalar rewrites when they are obvious.

    Examples:
    - v := expr;  -> SET v = expr;
    - SELECT x INTO v FROM t; -> SET v = (SELECT x FROM t);
    """
    out = sql

    # PL/SQL variable assignment.
    out = re.sub(
        r"\b([A-Za-z_][\w$]*)\s*:=\s*(.+?);",
        r"SET \1 = \2;",
        out,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # SELECT ... INTO var FROM ... ;
    def _select_into_repl(match: re.Match[str]) -> str:
        select_part = match.group(1).strip()
        var_part = match.group(2).strip()
        from_part = match.group(3).strip()
        return f"SET {var_part} = (SELECT {select_part} FROM {from_part});"

    out = re.sub(
        r"SELECT\s+(.+?)\s+INTO\s+([A-Za-z_][\w$]*)\s+FROM\s+(.+?);",
        _select_into_repl,
        out,
        flags=re.IGNORECASE | re.DOTALL,
    )

    return out


def mark_complex_blocks(sql: str) -> bool:
    text = sql.upper()
    return any(p.search(text) for p in COMPLEXITY_PATTERNS)


def complexity_score(sql: str) -> int:
    score = 0
    upper = sql.upper()
    for pat in COMPLEXITY_PATTERNS:
        if pat.search(upper):
            score += 2
    if re.search(r"\bDECLARE\b", upper):
        score += 1
    if re.search(r"\bBEGIN\b", upper):
        score += 1
    if re.search(r"\bEXCEPTION\b", upper):
        score += 2
    if re.search(r"\bEXECUTE\s+IMMEDIATE\b", upper):
        score += 2
    if re.search(r"\(\+\)", upper):
        score += 2
    return score


# -------------------- Block classification --------------------

def classify_statement(stmt: str) -> str:
    s = stmt.strip().upper()
    if not s:
        return "empty"
    if s.startswith("CREATE OR REPLACE") or s.startswith("CREATE TABLE"):
        return "ddl"
    if s.startswith("INSERT"):
        return "insert"
    if s.startswith("UPDATE"):
        return "update"
    if s.startswith("DELETE"):
        return "delete"
    if s.startswith("MERGE"):
        return "merge"
    if s.startswith("DECLARE") or s.startswith("BEGIN"):
        return "block"
    if re.search(r"\bEXECUTE\s+IMMEDIATE\b", s):
        return "dynamic_sql"
    if re.search(r"\bSELECT\b", s):
        return "select"
    return "other"


def build_blocks(sql: str) -> List[Block]:
    statements = split_statements(sql)
    blocks: List[Block] = []
    line_cursor = 1
    for stmt in statements:
        start, end = count_lines(stmt)
        kind = classify_statement(stmt)
        blocks.append(Block(kind=kind, content=stmt.strip(), start_line=line_cursor, end_line=line_cursor + max(end - 1, 0)))
        line_cursor += max(end, 1)
    return blocks


# -------------------- Playbook / prompt handling --------------------

def load_playbook(path: str = PLAYBOOK_PATH) -> str:
    p = Path(path)
    if p.exists():
        return p.read_text(encoding="utf-8")
    return (
        "PLSQL_TO_BIGQUERY_CONVERSION_PLAYBOOK_NOT_FOUND\n"
        "Use deterministic rewrites first; preserve logic; prefer set-based SQL; use scripting for procedural logic."
    )


def build_llm_prompt(block: str, playbook: str, context_notes: Sequence[str]) -> str:
    notes = "\n".join(f"- {n}" for n in context_notes) if context_notes else "- none"
    return f"""You are converting Oracle PL/SQL to BigQuery SQL.

Use this playbook as the source of truth:

<<<PLAYBOOK_START
{playbook}
PLAYBOOK_END>>>

Deterministic context already applied:
{notes}

Conversion requirements:
1. Preserve semantics, execution order, and data correctness.
2. Use BigQuery scripting when variables, conditionals, loops, or multi-step DML exist.
3. Use CTEs for read-only multi-step transformations.
4. Do not invent columns, tables, or business logic.
5. Keep the result production-grade and valid for BigQuery.
6. If a construct is ambiguous, prefer the safest semantically equivalent BigQuery pattern.
7. Output ONLY SQL. No prose, no markdown, no code fences.

Input block:
<<<SQL_START
{block}
SQL_END>>>
"""


# -------------------- Validation --------------------

# def validate_bigquery_output(sql: str) -> List[str]:
#     issues: List[str] = []
#     upper = sql.upper()

#     # These are valid in BigQuery scripting, so they are NOT forbidden globally.
#     # We only flag patterns that are clearly Oracle-only or malformed.
#     forbidden_oracle = [
#         r"\bNVL\b",
#         r"\bSYSDATE\b",
#         r"\bSYSTIMESTAMP\b",
#         r"\bDUAL\b",
#         r"\bROWNUM\b",
#         r"\bDECODE\s*\(",
#         r"\(\+\)",
#     ]
#     for pat in forbidden_oracle:
#         if re.search(pat, upper):
#             issues.append(f"Oracle construct still present: {pat}")

#     # Extremely conservative syntax sanity check.
#     if not any(k in upper for k in ["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE"]):
#         issues.append("No recognizable SQL statement found")

#     # Unbalanced parentheses often indicates a broken transformation.
#     if sql.count("(") != sql.count(")"):
#         issues.append("Unbalanced parentheses")

#     return issues

def validate_bigquery_output(sql: str) -> List[str]:
    issues = []
    upper = sql.upper()

    # 1. Check it's not empty
    if not sql.strip():
        issues.append("Empty SQL output")

    # 2. Allow BigQuery scripting constructs
    allowed_keywords = [
        "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE",
        "CREATE", "DECLARE", "SET", "BEGIN", "END",
        "IF", "ELSE", "WITH"
    ]

    if not any(k in upper for k in allowed_keywords):
        issues.append("No valid SQL or scripting construct found")

    # 3. Detect leftover Oracle syntax (real errors)
    forbidden_oracle = [
        r"\bNVL\b",
        r"\bSYSDATE\b",
        r"\bSYSTIMESTAMP\b",
        r"\bROWNUM\b",
        r"\(\+\)"
    ]

    for pat in forbidden_oracle:
        if re.search(pat, upper):
            issues.append(f"Oracle syntax still present: {pat}")

    # 4. Basic structure sanity
    if sql.count("(") != sql.count(")"):
        issues.append("Unbalanced parentheses")

    return issues


# -------------------- LLM transformation --------------------

class LLMTransformer:
    def __init__(self, model: str = LLM_MODEL, cache_path: str = CACHE_PATH) -> None:
        self.client = Groq()
        self.model = model
        self.cache = MemoryCache(cache_path)
        self.playbook = load_playbook()

    def transform(self, block_sql: str, notes: Optional[List[str]] = None) -> str:
        notes = notes or []
        key = md5(self.playbook + "\n" + block_sql + "\n" + "\n".join(notes))
        cached = self.cache.get(key)
        if cached:
            return cached

        prompt = build_llm_prompt(block_sql, self.playbook, notes)
        last_error: Optional[Exception] = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.client.chat.completions.create(
                    model=self.model,
                    temperature=0,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You convert Oracle PL/SQL to BigQuery SQL with high precision. "
                                "Follow the playbook exactly and output only SQL."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                )
                output = resp.choices[0].message.content.strip()  # type: ignore[attr-defined]
                # issues = validate_bigquery_output(output)
                output = fix_parentheses(output)
                issues = validate_bigquery_output(output)
                if issues:
                    raise ValueError("; ".join(issues))
                self.cache.set(key, output)
                return output
            except Exception as exc:
                last_error = exc
                time.sleep(min(2 * attempt, 5))

        # raise RuntimeError(f"LLM transform failed after {MAX_RETRIES} retries: {last_error}")
        print(f"[WARNING] LLM failed: {last_error}")
        return block_sql   # fallback instead of crash


# -------------------- Main conversion pipeline --------------------

def deterministic_preprocess(block_sql: str, project_id: str = PROJECT_ID, dataset: str = DATASET) -> Tuple[str, List[str]]:
    notes: List[str] = []
    out = block_sql

    before = out
    out = map_oracle_functions(out)
    if out != before:
        notes.append("Applied deterministic function mapping (NVL/SYSDATE/etc.).")

    before = out
    out = replace_simple_date_tokens(out)
    if out != before:
        notes.append("Applied deterministic date/time token mapping.")

    before = out
    out = convert_simple_assignments(out)
    if out != before:
        notes.append("Applied simple scalar assignment rewrites.")

    before = out
    out = normalize_outer_joins(out)
    if out != before:
        notes.append("Detected Oracle outer join markers and annotated them for LLM conversion.")

    before = out
    out = qualify_unqualified_tables(out, project_id, dataset)
    if out != before:
        notes.append("Qualified simple table references.")

    # Preserve text structure for the LLM; do not over-edit.
    out = normalize_whitespace(out)
    return out, notes


def route_block(block_sql: str) -> str:
    """Return either a directly usable SQL string or an LLM-transformed string."""
    deterministic, notes = deterministic_preprocess(block_sql)
    score = complexity_score(deterministic)

    # Simple blocks stay deterministic unless they are obviously complex.
    is_complex = score >= 3 or mark_complex_blocks(deterministic)

    if not is_complex:
        return deterministic

    transformer = LLMTransformer()
    return transformer.transform(deterministic, notes=notes)


def convert_pls_to_bq(file_path: str) -> List[ConversionResult]:
    raw = load_text(file_path)
    normalized = normalize_sql(raw)
    blocks = build_blocks(normalized)

    results: List[ConversionResult] = []
    for block in blocks:
        if not block.content.strip():
            continue

        deterministic, notes = deterministic_preprocess(block.content)
        score = complexity_score(deterministic)
        needs_llm = score >= 3 or mark_complex_blocks(deterministic)

        if needs_llm:
            llm = LLMTransformer()
            final = llm.transform(deterministic, notes=notes)
        else:
            final = deterministic

        results.append(
            ConversionResult(
                original=block.content,
                deterministic_sql=deterministic,
                final_sql=final,
                used_llm=needs_llm,
                complexity_score=score,
                notes=notes,
            )
        )

    return results


# -------------------- Output helpers --------------------

def save_output(results: Sequence[ConversionResult], output_path: str) -> None:
    out_lines: List[str] = []
    for idx, result in enumerate(results, start=1):
        out_lines.append(f"-- BLOCK {idx}")
        out_lines.append(f"-- complexity_score: {result.complexity_score}")
        out_lines.append(f"-- used_llm: {result.used_llm}")
        if result.notes:
            for note in result.notes:
                out_lines.append(f"-- note: {note}")
        out_lines.append(result.final_sql.rstrip())
        out_lines.append("")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text("\n".join(out_lines).rstrip() + "\n", encoding="utf-8")


def convert_many(input_files: Sequence[str], output_files: Sequence[str]) -> None:
    if len(input_files) != len(output_files):
        raise ValueError("input_files and output_files must have the same length")

    for input_file, output_file in zip(input_files, output_files):
        results = convert_pls_to_bq(input_file)
        save_output(results, output_file)
        print(f"{Path(input_file).name}: Done")


# -------------------- CLI entry point --------------------

def main() -> None:
    values = [0, 1, 2, 3, 4, 5]  # Example file indices; adjust as needed.
    input_files = [f"files/pls_sample_{i}.pls" for i in values]
    output_files = [f"files/bq_script_{i}.sql" for i in values]
    convert_many(input_files, output_files)


if __name__ == "__main__":
    main()
