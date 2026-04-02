"""
Flask Application Factory
Creates and configures the AuroraDB application
"""
import os

from dotenv import load_dotenv
from flask import Flask, redirect, url_for
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy

load_dotenv()

db = SQLAlchemy()
login_manager = LoginManager()


def create_app():
    """Create and configure Flask application."""
    app = Flask(
        __name__,
        template_folder='../../frontend/templates',
        static_folder='../../frontend/static'
    )

    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-change-in-production')
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///auroradb.db')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['UPLOAD_FOLDER'] = os.getenv('UPLOAD_FOLDER', 'uploads')
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Please log in to access this page.'
    login_manager.login_message_category = 'info'

    from .models.user import QueryHistory, UploadedFile, User

    @login_manager.user_loader
    def load_user(user_id):
        """Load user by ID for Flask-Login."""
        return User.query.get(int(user_id))

    with app.app_context():
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
