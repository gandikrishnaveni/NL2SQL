import os
import sqlite3
import re
import json
import pandas as pd
from dotenv import load_dotenv
import requests

load_dotenv()

# ── Direct HTTP to Ollama — skips ALL LangChain overhead (~200-400ms saved) ──
OLLAMA_URL  = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = "qwen2.5-coder:1.5b"

# ── Schema cache: {db_path: schema_string} ───────────────────────────────────
_SCHEMA_CACHE: dict[str, str] = {}


def _ollama(prompt: str) -> str:
    """
    Bare HTTP call to Ollama /api/generate.
    Bypasses LangChain, ChatPromptTemplate, and all their overhead.
    stream=False so we get one JSON response back immediately.
    """
    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0,
            "top_p": 0.1,
            "num_predict": 256,   # SQL rarely exceeds 256 tokens; cap for speed
            "repeat_penalty": 1.0,
        }
    }
    r = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=60)
    r.raise_for_status()
    return r.json().get("response", "").strip()


class NLPEngine:
    def __init__(self, db_path: str):
        self.db_path = db_path

    # ── Schema ────────────────────────────────────────────────────────────────

    def get_database_schema(self) -> str:
        """Cached schema extraction — hits disk only once per db_path."""
        if self.db_path in _SCHEMA_CACHE:
            return _SCHEMA_CACHE[self.db_path]
        try:
            if not os.path.exists(self.db_path):
                return ""
            conn = sqlite3.connect(self.db_path)
            cur  = conn.execute("SELECT sql FROM sqlite_master WHERE type='table';")
            schemas = [row[0] for row in cur.fetchall() if row[0]]
            conn.close()
            result = "\n\n".join(schemas)
            _SCHEMA_CACHE[self.db_path] = result
            return result
        except Exception as e:
            return f"Error reading schema: {e}"

    def _invalidate_schema_cache(self):
        """Call after DDL operations so schema is re-read next time."""
        _SCHEMA_CACHE.pop(self.db_path, None)

    # ── SINGLE combined Ollama call (replaces two sequential calls) ───────────

    def clarify_and_generate(self, user_query: str) -> dict:
        """
        ONE Ollama round-trip instead of two.
        Returns:
            {"ambiguous": False, "sql": "SELECT ..."}
            {"ambiguous": True,  "message": "AMBIGUOUS: ..."}
        """
        schema = self.get_database_schema()

        prompt = f"""<|im_start|>system
You are an expert SQLite Translator and SQL Architect.

SCHEMA:
{schema}

RULES:
1. AMBIGUITY GUARD: You MUST refuse to generate SQL and return ambiguous: true if the query falls into these categories:
   - Contains subjective adjectives without a defined metric (e.g., "worst", "best", "popular", "top", "bad", "hard workers", "slow").
   - Requests an UPDATE, DELETE, or "fire" but fails to specify exactly WHICH record(s) to modify or what the exact new values are.
   If ambiguous, respond ONLY with this JSON:
   {{"ambiguous": true, "message": "AMBIGUOUS: <ask the user for the exact metric, threshold, or IDs they mean>"}}

2. If you have any doubts regarding query or not sure with what intent the message is explain it and ask for confirmation.
3. You MUST use the EXACT table names and column names provided in the SCHEMA above. Do not invent table names.
4. Otherwise generate the correct SQLite SQL using these intent rules:
   - "Show/List/Who" → SELECT
   - "Total/Sum" → SUM()
   - "How many" → COUNT()
   - "top/best/high N" → ORDER BY [col] DESC LIMIT N
   - NEVER use CREATE DATABASE or USE. Use CREATE TABLE IF NOT EXISTS.
   Respond ONLY with this JSON:
   {{"ambiguous": false, "sql": "<single valid SQL statement>"}}

EXAMPLES:
User: "Who are the worst students?"
Assistant: {{"ambiguous": true, "message": "AMBIGUOUS: What defines 'worst'? Please specify if you mean lowest GPA, most absences, or fewest credits.", "sql": ""}}

User: "Update the grades for the failing students."
Assistant: {{"ambiguous": true, "message": "AMBIGUOUS: Please specify the exact new grade and the exact threshold for 'failing' (e.g., GPA < 2.0).", "sql": ""}}

User: "Show me the most popular courses."
Assistant: {{"ambiguous": true, "message": "AMBIGUOUS: Does 'popular' mean the highest enrollment count, or the highest student rating?", "sql": ""}}

User: "List the top managers in the company."
Assistant: {{"ambiguous": true, "message": "AMBIGUOUS: Please clarify if 'top' means managing the most employees, highest salaries, or longest tenure.", "sql": ""}}

User: "Fire the bad employees."
Assistant: {{"ambiguous": true, "message": "AMBIGUOUS: Please specify which employees to fire by exact ID or name. 'Bad' is not a valid metric.", "sql": ""}}

User: "Increase the salary for the hard workers."
Assistant: {{"ambiguous": true, "message": "AMBIGUOUS: Please specify the salary increase amount and define the exact metric for 'hard workers'.", "sql": ""}}

User: "Show me the best cars for city driving."
Assistant: {{"ambiguous": true, "message": "AMBIGUOUS: What makes a car 'best' for city driving? Please specify (e.g., highest City MPG or lowest price).", "sql": ""}}

User: "Delete the slow cars from the database."
Assistant: {{"ambiguous": true, "message": "AMBIGUOUS: What defines 'slow'? Is it a top speed under 100mph, or 0-60 acceleration time?", "sql": ""}}

User: "What are the worst car makers?"
Assistant: {{"ambiguous": true, "message": "AMBIGUOUS: Please clarify whether you mean the lowest average MPG, lowest production volume, or fewest models.", "sql": ""}}

User: "Delete student name ravi with id 123"
Assistant: {{"ambiguous": false, "message": "", "sql": "DELETE FROM my_students WHERE id = 123 AND name = 'ravi'"}}

Output raw JSON only. No markdown. No explanation.
<|im_end|>
<|im_start|>user
{user_query}
<|im_end|>
<|im_start|>assistant
"""
        raw = _ollama(prompt)

        # Strip any accidental markdown fences
        clean = re.sub(r"```[a-z]*\n?", "", raw).strip().strip("`").strip()

        try:
            result = json.loads(clean)
            return {
                "ambiguous": bool(result.get("ambiguous", False)),
                "message":   result.get("message", ""),
                "sql":       re.sub(r'```sql|```', '', result.get("sql", "")).strip(),
            }
        except json.JSONDecodeError:
            # Model didn't follow JSON format — treat whole output as SQL
            sql = re.sub(r'```sql|```', '', clean).strip()
            return {"ambiguous": False, "sql": sql, "message": ""}

    # ── Legacy methods kept for backward compatibility ────────────────────────

    def get_clarification(self, user_query: str) -> str:
        """Legacy: returns raw string with AMBIGUOUS or CLEAR."""
        result = self.clarify_and_generate(user_query)
        if result["ambiguous"]:
            return result["message"] if result["message"] else "AMBIGUOUS"
        return "CLEAR"

    def generate_sql(self, user_query: str) -> str:
        """Legacy: returns SQL string directly."""
        result = self.clarify_and_generate(user_query)
        return result.get("sql", "")

    # ── Execution ─────────────────────────────────────────────────────────────

    def execute_query(self, sql_query: str, user_command: str = None):
        """
        Fast execution:
        - SELECT → raw sqlite3 + dict rows (no pandas read_sql overhead)
        - DML    → direct cursor.executescript
        - pandas DataFrame still returned for SELECT so callers that expect it still work.
        """
        try:
            # Handle "create database / new database" commands
            if user_command and any(w in user_command.lower() for w in ["create database", "new database"]):
                match = re.search(r'(?:database|named)\s+(\w+)', user_command.lower())
                if match:
                    new_db_name = match.group(1)
                    self.db_path = f"data/{new_db_name}.sqlite"
                    os.makedirs('data', exist_ok=True)
                    sqlite3.connect(self.db_path).close()
                    self._invalidate_schema_cache()

            # Strip incompatible MySQL commands
            clean_stmts = [
                s.strip() for s in sql_query.split(';')
                if s.strip() and not any(
                    bad in s.upper() for bad in ["CREATE DATABASE", "USE "]
                )
            ]
            if not clean_stmts:
                return "⚠️ No valid SQL statements found."

            conn = sqlite3.connect(self.db_path, timeout=10)

            if clean_stmts[0].upper().startswith("SELECT"):
                # ── Fast path: raw sqlite3, convert to DataFrame at the end ──
                conn.row_factory = sqlite3.Row
                cur  = conn.execute(clean_stmts[0])
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
                conn.close()
                if not rows:
                    return "⚠️ No results found."
                return pd.DataFrame([dict(r) for r in rows], columns=cols)
            else:
                cursor = conn.cursor()
                for sql in clean_stmts:
                    cursor.execute(sql)
                conn.commit()
                conn.close()
                # Invalidate schema cache on DDL
                if any(k in clean_stmts[0].upper() for k in ["CREATE", "DROP", "ALTER"]):
                    self._invalidate_schema_cache()
                return f"✅ Success! Executed against {self.db_path}"

        except Exception as e:
            return f"Execution Error: {str(e)}"


# ── CLI for quick testing ─────────────────────────────────────────────────────
if __name__ == "__main__":
    import time
    os.makedirs('data', exist_ok=True)
    engine = NLPEngine("data/college_2.sqlite")
    print("--- ⚖️ LegalBrain AI: Optimized Engine ---")
    while True:
        query = input("\n[Enter Command]: ")
        if query.lower() == 'exit':
            break
        t0 = time.perf_counter()
        result = engine.clarify_and_generate(query)
        if result["ambiguous"]:
            print(f"🤔 {result['message']}")
            query = input("Please specify: ")
            result = engine.clarify_and_generate(query)

        sql = result["sql"]
        print(f"🚀 SQL: {sql}  [{time.perf_counter()-t0:.2f}s]")

        t1 = time.perf_counter()
        output = engine.execute_query(sql, user_command=query)
        print(f"\n--- RESULTS ({time.perf_counter()-t1:.3f}s) ---")
        print(output)