import sqlite3
from database.db_config import DB_PATH

def log_action(
    user_id,
    user_role,
    action_type,
    dataset_name,
    natural_language_query,
    generated_sql,
    outcome_status,
    affected_rows
):
    conn = sqlite3.connect(DB_PATH)  # ✅ ALWAYS use DB_PATH
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO audit_log (
            user_id,
            user_role,
            action_type,
            dataset_name,
            natural_language_query,
            generated_sql,
            outcome_status,
            affected_rows
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id,
        user_role,
        action_type,
        dataset_name,
        natural_language_query,
        generated_sql,
        outcome_status,
        affected_rows
    ))

    conn.commit()
    conn.close()
