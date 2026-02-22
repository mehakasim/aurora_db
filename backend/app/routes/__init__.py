"""
Routes package initialization
"""
from flask import Flask

def register_routes(app):
    """Register all route blueprints"""
    from .auth import bp as auth_bp
    from .main import bp as main_bp
    from .query import bp as query_bp 
    
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(main_bp)
    app.register_blueprint(query_bp)  