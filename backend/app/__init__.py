"""
Flask Application Factory
Creates and configures the AuroraDB application
"""
import os

from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, request, url_for
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
from .utils.paths import ensure_runtime_storage, get_sqlalchemy_database_uri, get_upload_folder

load_dotenv()

db = SQLAlchemy()
login_manager = LoginManager()


def create_app():
    """Create and configure Flask application."""
    ensure_runtime_storage()

    app = Flask(
        __name__,
        template_folder='../../frontend/templates',
        static_folder='../../frontend/static'
    )

    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-change-in-production')
    app.config['SQLALCHEMY_DATABASE_URI'] = get_sqlalchemy_database_uri()
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['UPLOAD_FOLDER'] = os.getenv('UPLOAD_FOLDER', get_upload_folder())
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Please log in to access this page.'
    login_manager.login_message_category = 'info'

    @login_manager.unauthorized_handler
    def unauthorized():
        """Return JSON for API requests instead of redirecting to HTML pages."""
        if request.path.startswith('/api/'):
            return jsonify({
                'success': False,
                'message': 'Your session expired. Please sign in again.'
            }), 401
        return redirect(url_for('auth.login'))

    from .models.user import QueryHistory, UploadedFile, User

    @login_manager.user_loader
    def load_user(user_id):
        """Load user by ID for Flask-Login."""
        return User.query.get(int(user_id))

    with app.app_context():
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        db.create_all()
        print("[OK] Database tables created!")

    from .routes import auth, main, query, visualizations

    app.register_blueprint(auth.bp)
    app.register_blueprint(main.bp)
    app.register_blueprint(query.bp)
    app.register_blueprint(visualizations.bp)

    @app.route('/')
    def index():
        return redirect(url_for('auth.login'))

    print("[OK] AuroraDB initialized successfully!")
    return app
