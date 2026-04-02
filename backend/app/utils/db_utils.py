"""
Database Utilities - Functions for working with SQLite
"""
import sqlite3

import pandas as pd


def get_table_preview(table_name, limit=100, offset=0):
    """Get preview of data from table."""
    conn = sqlite3.connect('auroradb.db')

    try:
        query = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"
        df = pd.read_sql_query(query, conn)

        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]

        return {
            'columns': list(df.columns),
            'rows': df.values.tolist(),
            'total_rows': total_rows,
            'displayed_rows': len(df)
        }
    finally:
        conn.close()


def get_table_schema(table_name):
    """Get column names and types from SQLite table."""
    conn = sqlite3.connect('auroradb.db')
    cursor = conn.cursor()

    try:
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        return {
            'columns': [col[1] for col in columns],
            'types': [col[2] for col in columns]
        }
    finally:
        conn.close()


def execute_query(sql_query, table_name=None):
    """Execute SQL query and return results."""
    conn = sqlite3.connect('auroradb.db')

    try:
        df = pd.read_sql_query(sql_query, conn)
        return {
            'columns': list(df.columns),
            'rows': df.values.tolist(),
            'row_count': len(df)
        }
    except Exception as exc:
        raise Exception(f"Query error: {str(exc)}")
    finally:
        conn.close()


def drop_table(table_name):
    """Delete a table from database."""
    conn = sqlite3.connect('auroradb.db')
    cursor = conn.cursor()

    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        print(f"[OK] Dropped table: {table_name}")
    finally:
        conn.close()
