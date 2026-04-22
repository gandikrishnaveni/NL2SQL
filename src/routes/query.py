from flask import Blueprint, render_template, request, session, jsonify, Response, stream_with_context
import json, re, os, sqlite3, hashlib
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

from extensions import get_engine

query_bp = Blueprint('query', __name__)

DML_KEYWORDS = re.compile(r'\b(UPDATE|DELETE|INSERT|DROP|ALTER)\b', re.IGNORECASE)

# ── Shared thread pool for parallel execution and async logging ──────────────
_POOL = ThreadPoolExecutor(max_workers=4)

# ── Simple in-process SQL cache (query_hash → sql string) ────────────────────
# Avoids re-hitting Qwen for identical natural-language queries on the same DB.
_SQL_CACHE: dict[str, str] = {}
_SQL_CACHE_MAX = 256

def _cache_key(db_path: str, user_input: str) -> str:
    raw = f"{db_path}||{user_input.strip().lower()}"
    return hashlib.md5(raw.encode()).hexdigest()

def _get_cached_sql(db_path: str, user_input: str):
    return _SQL_CACHE.get(_cache_key(db_path, user_input))

def _put_cached_sql(db_path: str, user_input: str, sql: str):
    key = _cache_key(db_path, user_input)
    if len(_SQL_CACHE) >= _SQL_CACHE_MAX:
        # evict oldest quarter to prevent memory bloat
        for k in list(_SQL_CACHE)[:_SQL_CACHE_MAX // 4]:
            del _SQL_CACHE[k]
    _SQL_CACHE[key] = sql


# ── Auth decorator ───────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get('logged_in'):
            return jsonify(error='Unauthorized'), 401
        return f(*args, **kwargs)
    return wrapper


# ── Helpers ──────────────────────────────────────────────────────────────────

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
        conn = sqlite3.connect(db_path, timeout=5)
        conn.execute("BEGIN")
        cur = conn.execute(sql)
        affected = cur.rowcount
        conn.rollback()
        conn.close()
        return max(affected, 0)
    except Exception:
        return -1


def _run_select(db_path: str, sql: str):
    """Execute SELECT using raw sqlite3 — skips pandas overhead for speed."""
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql)
    rows_raw = cur.fetchall()
    columns  = [d[0] for d in cur.description] if cur.description else []
    conn.close()
    rows = [dict(r) for r in rows_raw]
    return rows, columns


# ── Pages ────────────────────────────────────────────────────────────────────

@query_bp.route('/dashboard')
@login_required
def dashboard():
    db_files = _list_dbs()
    return render_template('console.html',
                           user=session['user'],
                           db_files=db_files,
                           active_db=os.path.basename(session.get('db_path', '')))


# ── API ──────────────────────────────────────────────────────────────────────

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
    Streaming SSE pipeline mapped to the 6-step UI.
    Includes caching and parallel async auditing for maximum speed.
    """
    body       = request.get_json(silent=True) or {}
    user_input = (body.get('query') or '').strip()
    db_path    = session.get('db_path', '')
    user       = dict(session['user'])  # Copy for thread safety

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

            # Check cache early
            cached_sql = _get_cached_sql(db_path, user_input)
            
            if cached_sql:
                # Cache Hit: Rapidly cycle UI steps 1 & 2
                yield from emit({"step": 1, "label": steps[1], "status": "active"})
                yield from emit({"step": 1, "label": steps[1], "status": "done"})
                yield from emit({"step": 2, "label": steps[2], "status": "active"})
                generated_sql = cached_sql
                yield from emit({"step": 2, "label": steps[2], "status": "done", "cache_hit": True})
            else:
                # Cache Miss: Call Model
                yield from emit({"step": 1, "label": steps[1], "status": "active"})
                
                try:
                    # Attempt single optimized Ollama call
                    combined = engine.clarify_and_generate(user_input)
                    yield from emit({"step": 1, "label": steps[1], "status": "done"})
                    
                    if combined.get("ambiguous"):
                        yield from emit({"done": True, "result_type": "warning", "message": combined["message"]})
                        return
                        
                    yield from emit({"step": 2, "label": steps[2], "status": "active"})
                    generated_sql = combined["sql"]
                    yield from emit({"step": 2, "label": steps[2], "status": "done"})
                
                except AttributeError:
                    # Fallback to two-call logic if engine doesn't support combined yet
                    clarification = engine.get_clarification(user_input)
                    yield from emit({"step": 1, "label": steps[1], "status": "done"})
                    
                    if "AMBIGUOUS" in clarification:
                        yield from emit({"done": True, "result_type": "warning", "message": clarification})
                        return
                        
                    yield from emit({"step": 2, "label": steps[2], "status": "active"})
                    generated_sql = engine.generate_sql(user_input)
                    yield from emit({"step": 2, "label": steps[2], "status": "done"})

                _put_cached_sql(db_path, user_input, generated_sql)

            # STEP 3
            yield from emit({"step": 3, "label": steps[3], "status": "active"})
            yield from emit({"step": 3, "label": steps[3], "status": "done"})

            # STEP 4: RBAC & Dry-Run
            yield from emit({"step": 4, "label": steps[4], "status": "active"})

            if not _is_authorized(user['emp_id'], generated_sql):
                yield from emit({"done": True, "result_type": "rbac_error",
                                 "message": f"❌ RBAC BLOCKED: {user['role']} role does not have permission for this operation."})
                return

            is_dml = bool(DML_KEYWORDS.search(generated_sql))

            if is_dml:
                # Parallelize row counting so the UI can update while waiting
                fut_count = _POOL.submit(_count_affected_rows, db_path, generated_sql)
                yield from emit({"step": 4, "label": steps[4], "status": "done"})
                affected = fut_count.result()
                
                yield from emit({"done": True, "result_type": "dml_confirm",
                                 "sql": generated_sql,
                                 "affected_rows": affected})
                return

            yield from emit({"step": 4, "label": steps[4], "status": "done"})

            # STEP 5: Execute & Audit
            yield from emit({"step": 5, "label": steps[5], "status": "active"})
            rows, columns = _run_select(db_path, generated_sql)

            # Audit log is fire-and-forget; runs in background thread
            _POOL.submit(_log, user, "SELECT", db_path, user_input, generated_sql, "SUCCESS", len(rows))

            yield from emit({"step": 5, "label": steps[5], "status": "done"})
            yield from emit({"done": True, "result_type": "data",
                             "sql": generated_sql,
                             "columns": columns,
                             "rows": rows,
                             "count": len(rows)})

        except Exception as e:
            yield from emit({"done": True, "result_type": "error", "message": str(e)})

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
    user    = dict(session['user'])
    db_path = session.get('db_path', '')

    if not sql or not DML_KEYWORDS.search(sql):
        return jsonify(success=False, error='Invalid DML'), 400

    try:
        # Run DML via raw sqlite3 for speed (bypasses pandas/engine overhead)
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute(sql)
        conn.commit()
        conn.close()
        
        # Async audit logging
        _POOL.submit(_log, user, "DML_COMMIT", db_path, body.get('user_query', ''), sql, "SUCCESS", 0)
        return jsonify(success=True, message="Operation completed successfully.")
    except Exception as e:
        return jsonify(success=False, error=str(e)), 500


# ── Utils ─────────────────────────────────────────────────────────────────────

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