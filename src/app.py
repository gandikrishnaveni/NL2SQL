from flask import Flask
from extensions import db_session
from routes.auth import auth_bp
from routes.query import query_bp
from routes.audit import audit_bp
from routes.database import database_bp
import os

def create_app():
    app = Flask(__name__)
    app.secret_key = os.urandom(24)
    app.config['UPLOAD_FOLDER'] = 'data'
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB

    os.makedirs('data', exist_ok=True)

    app.register_blueprint(auth_bp)
    app.register_blueprint(query_bp)
    app.register_blueprint(audit_bp)
    app.register_blueprint(database_bp)

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, threaded=True, port=5000)