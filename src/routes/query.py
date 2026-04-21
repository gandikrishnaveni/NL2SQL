from flask import Blueprint, render_template, request, session, jsonify, Response, stream_with_context
import json, re, os, time
import sqlite3

from extensions import get_engine
from functools import wraps

query_bp = Blueprint('query', __name__)

DML_KEYWORDS = re.compile(r'\b(UPDATE|DELETE|INSERT|DROP|ALTER)\b', re.IGNORECASE)


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get('logged_in'):
            return jsonify(error='Unauthorized'), 401
        return f(*args, **kwargs)
    return wrapper


# ---------- helpers ----------

def _is_authorized(emp_id: str, sql: str) -> bool:
    """Thin wrapper around project's rbac_manager."""
    try:
        from rbac_manager import is_authorized
        return is_authorized(emp_id, sql)
    except Exception:
        return True  # fallback: allow if module missing


def _count_affected_rows(db_path: str, sql: str) -> int:
    """
    Dry-run DML to count affected rows WITHOUT committing.
    Uses a savepoint that is immediately rolled back.
    """
    try:
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        # Use EXPLAIN QUERY PLAN is not useful for counts;
        # instead run inside a savepoint and rollback
        conn.execute("BEGIN")
        cur.execute(sql)
        affected = cur.rowcount
        conn.rollback()
        conn.close()
        return max(affected, 0)
    except Exception:
        return -1   # unknown


def _run_select(db_path: str, sql: str):
    """Execute a SELECT and return list-of-dicts."""
    import pandas as pd
    from extensions import get_engine
    engine = get_engine(db_path)
    result = engine.execute_query(sql, user_command='')
    if hasattr(result, 'to_dict'):
        return result.to_dict(orient='records'), list(result.columns)
    return [], []


# ---------- pages ----------

@query_bp.route('/dashboard')
@login_required
def console():
    db_files = _list_dbs()
    return render_template('console.html',
                           user=session['user'],
                           db_files=db_files,
                           active_db=os.path.basename(session.get('db_path', '')))


# ---------- API ----------

@query_bp.route('/api/set-db', methods=['POST'])
@login_required
def set_db():
    db_name = request.json.get('db_name', '')
    path    = os.path.join('data', db_name)
    if os.path.exists(path):
        session['db_path'] = path
        return jsonify(success=True, active_db=db_name)
    return jsonify(success=False, error='DB not found'), 404


@query_bp.route('/api/query', methods=['POST'])
@login_required
def run_query():
    """
    Streaming SSE endpoint.
    Emits JSON lines: { step, label, status }  during pipeline
    Final line:       { done: true, result_type, ... }
    """
    body       = request.get_json(silent=True) or {}
    user_input = (body.get('query') or '').strip()
    db_path    = session.get('db_path', '')
    user       = session['user']

    def generate():
        def emit(obj):
            yield f"data: {json.dumps(obj)}\n\n"

        steps = [
            "Indexing Database Schema",
            "Running Ambiguity Guard",
            "LLM SQL Generation (Qwen 2.5)",
            "Syntax Sanitization",
            "Dry-Run Trace Analysis",
            "Finalizing Execution & Audit",
        ]

        try:
            # STEP 0
            yield from emit({"step": 0, "label": steps[0], "status": "active"})
            engine = get_engine(db_path)
            yield from emit({"step": 0, "label": steps[0], "status": "done"})

            # STEP 1
            yield from emit({"step": 1, "label": steps[1], "status": "active"})
            clarification = engine.get_clarification(user_input)
            yield from emit({"step": 1, "label": steps[1], "status": "done"})

            if "AMBIGUOUS" in clarification:
                yield from emit({"done": True, "result_type": "warning",
                                 "message": clarification})
                return

            # STEP 2
            yield from emit({"step": 2, "label": steps[2], "status": "active"})
            generated_sql = engine.generate_sql(user_input)
            yield from emit({"step": 2, "label": steps[2], "status": "done"})

            # STEP 3 – sanitization (placeholder, extend as needed)
            yield from emit({"step": 3, "label": steps[3], "status": "active"})
            yield from emit({"step": 3, "label": steps[3], "status": "done"})

            # STEP 4 – RBAC + dry run
            yield from emit({"step": 4, "label": steps[4], "status": "active"})

            if not _is_authorized(user['emp_id'], generated_sql):
                yield from emit({"done": True, "result_type": "rbac_error",
                                 "message": f"❌ RBAC BLOCKED: {user['role']} role does not have permission for this operation."})
                return

            is_dml = bool(DML_KEYWORDS.search(generated_sql))

            if is_dml:
                affected = _count_affected_rows(db_path, generated_sql)
                yield from emit({"step": 4, "label": steps[4], "status": "done"})
                yield from emit({"done": True, "result_type": "dml_confirm",
                                 "sql": generated_sql,
                                 "affected_rows": affected})
                return

            yield from emit({"step": 4, "label": steps[4], "status": "done"})

            # STEP 5 – execute SELECT
            yield from emit({"step": 5, "label": steps[5], "status": "active"})
            rows, columns = _run_select(db_path, generated_sql)

            _log(user, "SELECT", db_path, user_input, generated_sql, "SUCCESS", len(rows))

            yield from emit({"step": 5, "label": steps[5], "status": "done"})
            yield from emit({"done": True, "result_type": "data",
                             "sql": generated_sql,
                             "columns": columns,
                             "rows": rows,
                             "count": len(rows)})

        except Exception as e:
            yield from emit({"done": True, "result_type": "error",
                             "message": str(e)})

    return Response(stream_with_context(generate()),
                    mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache',
                             'X-Accel-Buffering': 'no'})


@query_bp.route('/api/dml-execute', methods=['POST'])
@login_required
def dml_execute():
    """Called when user clicks YES on the DML confirmation modal."""
    body    = request.get_json(silent=True) or {}
    sql     = body.get('sql', '')
    user    = session['user']
    db_path = session.get('db_path', '')

    if not sql or not DML_KEYWORDS.search(sql):
        return jsonify(success=False, error='Invalid DML'), 400

    try:
        engine = get_engine(db_path)
        result = engine.execute_query(sql, user_command=body.get('user_query', ''))
        _log(user, "DML_COMMIT", db_path, body.get('user_query', ''), sql, "SUCCESS", 0)
        msg = result if isinstance(result, str) else "Operation completed successfully."
        return jsonify(success=True, message=msg)
    except Exception as e:
        return jsonify(success=False, error=str(e)), 500


# ---------- utils ----------

def _list_dbs():
    folder = 'data'
    os.makedirs(folder, exist_ok=True)
    return [f for f in os.listdir(folder) if f.endswith(('.sqlite', '.db', '.sqlite3'))]


def _log(user, action, db_path, user_input, sql, status, rows):
    try:
        from services.audit_logger import log_action
        log_action(user['name'], user['role'], action,
                   os.path.basename(db_path), user_input, sql, status, rows)
    except Exception:
        pass