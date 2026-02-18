"""
File Processor - Handles Excel/CSV uploads and converts to SQLite
FIXED: Properly handles column headers
"""
import pandas as pd
import os
from werkzeug.utils import secure_filename
from datetime import datetime
from ..models.user import db, UploadedFile
import sqlite3

UPLOAD_FOLDER = 'uploads'

def process_uploaded_file(file, user_id):
    """
    Process uploaded Excel/CSV file:
    1. Save file to disk
    2. Read with pandas
    3. Convert to SQLite table
    4. Save record in database
    """
    # Secure the filename
    original_filename = secure_filename(file.filename)
    
    # Create unique filename using timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{user_id}_{timestamp}_{original_filename}"
    
    # Full path to save file
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    
    # Save file to disk
    file.save(file_path)
    file_size = os.path.getsize(file_path)
    
    try:
        # Read file with pandas
        if original_filename.lower().endswith('.csv'):
            # Try to read CSV, handle if first row is not headers
            df = pd.read_csv(file_path)
        else:  # Excel file
            # Read Excel file
            df = pd.read_excel(file_path)
        
        # Check if columns are unnamed (no header row)
        unnamed_count = sum(1 for col in df.columns if str(col).startswith('Unnamed'))
        
        # If all or most columns are unnamed, assume no header row
        if unnamed_count > len(df.columns) * 0.5:
            # Re-read without header
            if original_filename.lower().endswith('.csv'):
                df = pd.read_csv(file_path, header=None)
            else:
                df = pd.read_excel(file_path, header=None)
            
            # Create column names: Column_1, Column_2, etc.
            df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
        
        # Clean column names (remove special characters, spaces)
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Get dimensions
        rows, columns = df.shape
        
        # Create table name (SQL-safe)
        table_name = f"user_{user_id}_data_{timestamp}"
        
        # Convert to SQLite
        convert_to_sqlite(df, table_name)
        
        # Save record in database
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
        
    except Exception as e:
        # If anything goes wrong, delete the uploaded file
        if os.path.exists(file_path):
            os.remove(file_path)
        raise Exception(f"Error processing file: {str(e)}")


def clean_column_name(col):
    """Clean column name to be SQL-safe"""
    import re
    
    # Convert to string
    col = str(col)
    
    # If it's "Unnamed: X", replace with Column_X
    if col.startswith('Unnamed:'):
        try:
            num = col.split(':')[1].strip()
            return f'Column_{int(num) + 1}'
        except:
            pass
    
    # Remove special characters, keep only alphanumeric and underscore
    col = re.sub(r'[^a-zA-Z0-9_]', '_', col)
    
    # Remove consecutive underscores
    col = re.sub(r'_+', '_', col)
    
    # Remove leading/trailing underscores
    col = col.strip('_')
    
    # If column starts with number, prefix with 'col_'
    if col and col[0].isdigit():
        col = 'col_' + col
    
    # If empty after cleaning, use generic name
    if not col:
        col = 'column'
    
    return col


def convert_to_sqlite(df, table_name):
    """Convert pandas DataFrame to SQLite table"""
    conn = sqlite3.connect('auroradb.db')
    
    try:
        # Write DataFrame to SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        print(f"✅ Created table: {table_name}")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        
    finally:
        conn.close()