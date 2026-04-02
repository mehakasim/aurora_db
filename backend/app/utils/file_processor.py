"""
File Processor - Handles Excel/CSV uploads and converts to SQLite
"""
import os
import sqlite3
from datetime import datetime

import pandas as pd
from werkzeug.utils import secure_filename

from ..models.user import UploadedFile, db

UPLOAD_FOLDER = 'uploads'


def process_uploaded_file(file, user_id):
    """
    Process uploaded Excel/CSV file:
    1. Save file to disk
    2. Read with pandas
    3. Convert to SQLite table
    4. Save record in database
    """
    original_filename = secure_filename(file.filename)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{user_id}_{timestamp}_{original_filename}"

    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    file_path = os.path.join(UPLOAD_FOLDER, filename)

    file.save(file_path)
    file_size = os.path.getsize(file_path)

    try:
        if original_filename.lower().endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        unnamed_count = sum(1 for col in df.columns if str(col).startswith('Unnamed'))
        if unnamed_count > len(df.columns) * 0.5:
            if original_filename.lower().endswith('.csv'):
                df = pd.read_csv(file_path, header=None)
            else:
                df = pd.read_excel(file_path, header=None)
            df.columns = [f'Column_{index + 1}' for index in range(len(df.columns))]

        df.columns = [clean_column_name(col) for col in df.columns]

        rows, columns = df.shape
        table_name = f"user_{user_id}_data_{timestamp}"

        convert_to_sqlite(df, table_name)

        file_record = UploadedFile(
            user_id=user_id,
            filename=filename,
            original_filename=original_filename,
            file_path=file_path,
            table_name=table_name,
            file_size=file_size,
            row_count=rows,
            column_count=columns
        )

        db.session.add(file_record)
        db.session.commit()

        return {
            'id': file_record.id,
            'filename': filename,
            'original_filename': original_filename,
            'table_name': table_name,
            'rows': rows,
            'columns': columns,
            'file_size': file_size
        }
    except Exception as exc:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise Exception(f"Error processing file: {str(exc)}")


def clean_column_name(col):
    """Clean column name to be SQL-safe."""
    import re

    col = str(col)

    if col.startswith('Unnamed:'):
        try:
            num = col.split(':')[1].strip()
            return f'Column_{int(num) + 1}'
        except Exception:
            pass

    col = re.sub(r'[^a-zA-Z0-9_]', '_', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_')

    if col and col[0].isdigit():
        col = 'col_' + col

    if not col:
        col = 'column'

    return col


def convert_to_sqlite(df, table_name):
    """Convert pandas DataFrame to SQLite table."""
    conn = sqlite3.connect('auroradb.db')

    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"[OK] Created table: {table_name}")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
    finally:
        conn.close()
