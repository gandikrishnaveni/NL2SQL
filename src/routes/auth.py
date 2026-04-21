from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify

auth_bp = Blueprint('auth', __name__)

EMPLOYEES = {
    "E001": {"name": "Aarav Sharma",  "role": "Admin"},
    "E002": {"name": "Meera Nair",    "role": "Manager"},
    "E003": {"name": "Rahul Verma",   "role": "Employee"},
}


@auth_bp.route('/', methods=['GET'])
def index():
    if session.get('logged_in'):
        # Updated to point to the new dashboard route
        return redirect(url_for('query.dashboard'))
    return render_template('login.html')


@auth_bp.route('/login', methods=['POST'])
def login():
    data    = request.get_json(silent=True) or {}
    emp_id  = (data.get('emp_id') or '').strip().upper()

    if emp_id in EMPLOYEES:
        session.clear()
        session['logged_in'] = True
        session['user'] = {**EMPLOYEES[emp_id], 'emp_id': emp_id}
        session['db_path'] = 'data/college_2.sqlite'
        return jsonify(success=True, user=session['user'])

    return jsonify(success=False, error='Invalid Employee ID'), 401


@auth_bp.route('/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify(success=True)