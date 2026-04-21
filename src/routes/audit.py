from flask import Blueprint, jsonify, session
from functools import wraps
import sqlite3

audit_bp = Blueprint('audit', __name__)


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get('logged_in'):
            return jsonify(error='Unauthorized'), 401
        return f(*args, **kwargs)
    return wrapper


@audit_bp.route('/api/audit-logs')
@login_required
def get_logs():
    if session['user']['role'] == 'Employee':
        return jsonify(error='Forbidden'), 403
    try:
        from database.db_config import DB_PATH
        from database.audit_schema import create_audit_table
        create_audit_table()
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM audit_log ORDER BY executed_at DESC LIMIT 500"
        ).fetchall()
        conn.close()
        return jsonify(logs=[dict(r) for r in rows])
    except Exception as e:
        return jsonify(logs=[], error=str(e))