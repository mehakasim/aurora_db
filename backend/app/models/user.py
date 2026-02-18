"""
Database Models for AuroraDB
Defines User, UploadedFile, and QueryHistory tables
"""
from datetime import datetime
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from .. import db


class User(UserMixin, db.Model):
    """User account model"""
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    uploaded_files = db.relationship('UploadedFile', backref='user', lazy=True, cascade='all, delete-orphan')
    query_history = db.relationship('QueryHistory', backref='user', lazy=True, cascade='all, delete-orphan')
    
    def set_password(self, password):
        """Hash and store password"""
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        """Verify password"""
        return check_password_hash(self.password_hash, password)
    
    def get_file_count(self):
        """Get number of uploaded files"""
        return len(self.uploaded_files)
    
    def get_query_count(self):
        """Get number of queries made"""
        return len(self.query_history)
    
    def __repr__(self):
        return f'<User {self.email}>'


class UploadedFile(db.Model):
    """Uploaded Excel/CSV file metadata"""
    __tablename__ = 'uploaded_files'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    original_filename = db.Column(db.String(255), nullable=False)
    file_path = db.Column(db.String(500), nullable=False)
    table_name = db.Column(db.String(100), nullable=False, index=True)
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)
    file_size = db.Column(db.Integer)
    row_count = db.Column(db.Integer)
    column_count = db.Column(db.Integer)
    
    def __repr__(self):
        return f'<File {self.original_filename}>'


class QueryHistory(db.Model):
    """User query history"""
    __tablename__ = 'query_history'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    file_id = db.Column(db.Integer, db.ForeignKey('uploaded_files.id'))
    question = db.Column(db.Text, nullable=False)
    sql_query = db.Column(db.Text)
    result_count = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    execution_time = db.Column(db.Float)  # in seconds
    
    def __repr__(self):
        return f'<Query {self.id}: {self.question[:30]}...>'
