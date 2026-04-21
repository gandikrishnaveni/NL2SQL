from flask import Blueprint, request, session, jsonify, current_app
from functools import wraps
import os

database_bp = Blueprint('database', __name__)


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get('logged_in'):
            return jsonify(error='Unauthorized'), 401
        return f(*args, **kwargs)
    return wrapper


@database_bp.route('/api/upload-db', methods=['POST'])
@login_required
def upload_db():
    file = request.files.get('file')
    if not file or not file.filename:
        return jsonify(success=False, error='No file'), 400

    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ('.sqlite', '.db', '.sqlite3'):
        return jsonify(success=False, error='Invalid file type'), 400

    upload_folder = current_app.config['UPLOAD_FOLDER']
    os.makedirs(upload_folder, exist_ok=True)
    target = os.path.join(upload_folder, file.filename)
    file.save(target)
    session['db_path'] = target
    return jsonify(success=True, db_name=file.filename)


@database_bp.route('/api/list-dbs')
@login_required
def list_dbs():
    folder = current_app.config['UPLOAD_FOLDER']
    os.makedirs(folder, exist_ok=True)
    files = [f for f in os.listdir(folder) if f.endswith(('.sqlite', '.db', '.sqlite3'))]
    return jsonify(files=files, active=os.path.basename(session.get('db_path', '')))