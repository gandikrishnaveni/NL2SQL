import sqlite3
from database.db_config import DB_PATH  # ✅ IMPORTANT

def create_audit_table():
    conn = sqlite3.connect(DB_PATH)  # ✅ SAME DB PATH EVERYWHERE
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        user_role TEXT NOT NULL,
        action_type TEXT NOT NULL,
        dataset_name TEXT,
        natural_language_query TEXT,
        generated_sql TEXT,
        affected_rows INTEGER,
        outcome_status TEXT,
        executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()
