"""
Runtime path helpers for local and serverless deployments.
"""
import os
import shutil


def get_project_root():
    """Return the repository root."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))


def get_runtime_root():
    """Return a writable runtime directory."""
    if os.getenv('VERCEL') == '1':
        return '/tmp'
    return os.getcwd()


def get_upload_folder():
    """Return a writable uploads folder path."""
    return os.path.join(get_runtime_root(), 'uploads')


def get_sqlite_db_path():
    """Return the SQLite database file path."""
    return os.path.join(get_runtime_root(), 'auroradb.db')


def get_sqlalchemy_database_uri():
    """Return a SQLite URI unless a non-SQLite DATABASE_URL is configured."""
    configured = os.getenv('DATABASE_URL')
    if configured and not configured.startswith('sqlite:///'):
        return configured
    return f"sqlite:///{get_sqlite_db_path()}"


def ensure_runtime_storage():
    """Prepare writable runtime folders and seed the SQLite database when needed."""
    runtime_root = get_runtime_root()
    os.makedirs(runtime_root, exist_ok=True)
    os.makedirs(get_upload_folder(), exist_ok=True)

    runtime_db = get_sqlite_db_path()
    source_db = os.path.join(get_project_root(), 'auroradb.db')
    if runtime_db != source_db and not os.path.exists(runtime_db) and os.path.exists(source_db):
        shutil.copy2(source_db, runtime_db)
