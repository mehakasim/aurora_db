"""
Query Routes - AI-Powered Spreadsheet Queries
"""
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from ..models.user import db, UploadedFile, QueryHistory
from ..utils.ai_processor import process_natural_language_query
from ..utils.db_utils import execute_query, get_table_schema
import time
import re

bp = Blueprint('query', __name__)


# ---------------------------------------------------------------------------
# Column selection helpers
# ---------------------------------------------------------------------------

def try_parse_numeric(val):
    """
    Try to parse a value as float, stripping currency/percent symbols.
    Returns float if parseable, else None.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = (val.strip()
                   .replace(',', '')
                   .replace('%', '')
                   .replace('₹', '')
                   .replace('$', '')
                   .replace('€', '')
                   .replace('£', '')
                   .strip())
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def extract_col_from_sql(sql: str, candidates: list) -> tuple:
    """Find the most relevant value column from WHERE/ORDER BY clause."""
    if not sql:
        return None, None
    for keyword in ('ORDER BY', 'WHERE', 'GROUP BY'):
        idx = sql.upper().find(keyword)
        if idx == -1:
            continue
        clause = sql[idx + len(keyword):]
        for col_name, col_idx in candidates:
            if re.search(rf'\b{re.escape(col_name)}\b', clause, re.IGNORECASE):
                return col_name, col_idx
    return None, None


def pick_label_column(text_cols: list, id_cols: list) -> tuple:
    """Priority: name-like text col > id/code col > first text col."""
    name_keywords = ['name', 'title', 'label', 'student', 'employee',
                     'university', 'company', 'product', 'item']
    for col, idx in text_cols:
        if any(kw in col.lower() for kw in name_keywords):
            return col, idx
    if id_cols:
        return id_cols[0]
    if text_cols:
        return text_cols[0]
    return None, None


def pick_best_value_column(candidates: list, question: str) -> tuple:
    """Score numeric column candidates against question words."""
    if not candidates:
        return None, None
    q_words = set(question.lower().replace('_', ' ').split())
    best, best_score = candidates[0], 0
    for col_name, col_idx in candidates:
        col_words = set(col_name.lower().replace('_', ' ').split())
        score = len(q_words & col_words)
        for qw in q_words:
            if len(qw) >= 3 and qw in col_name.lower():
                score += 2
        if score > best_score:
            best_score = score
            best = (col_name, col_idx)
    return best


# ---------------------------------------------------------------------------
# Chart builder
# ---------------------------------------------------------------------------

def generate_chart_from_data(columns, rows, question, sql_query=None):
    if not rows or not columns:
        return None

    numeric_cols, text_cols, id_cols = [], [], []

    for i, col in enumerate(columns):
        col_lower = col.lower()
        # Sample a few rows for more reliable type detection
        samples = [rows[j][i] for j in range(min(5, len(rows))) if rows[j][i] is not None]
        numeric_samples = [try_parse_numeric(v) for v in samples]
        is_numeric = len(numeric_samples) > 0 and all(v is not None for v in numeric_samples)

        if col_lower == 'id' or col_lower.endswith('_id') or col_lower.endswith('_code'):
            id_cols.append((col, i))
        elif is_numeric:
            numeric_cols.append((col, i))
        else:
            text_cols.append((col, i))

    # --- Value column (y-axis): ORDER BY hint → question scoring → first numeric ---
    value_col_name, value_col_idx = extract_col_from_sql(sql_query, numeric_cols)
    if value_col_name is None:
        value_col_name, value_col_idx = pick_best_value_column(numeric_cols, question)

    # --- Label column (x-axis): name > id > first text ---
    label_col_name, label_col_idx = pick_label_column(text_cols, id_cols)

    # --- Build labels & values ---
    if not numeric_cols:
        # All text — frequency count chart
        if not label_col_name:
            return None
        from collections import Counter
        counts = Counter(str(r[label_col_idx]) for r in rows)
        top = counts.most_common(20)
        labels = [t[0] for t in top]
        values = [t[1] for t in top]
        value_col_name = f"Count of {label_col_name}"

    elif label_col_name is None:
        labels = [f"Row {i + 1}" for i in range(min(20, len(rows)))]
        values = [try_parse_numeric(r[value_col_idx]) or 0 for r in rows[:20]]

    else:
        labels = [str(r[label_col_idx])[:30] for r in rows[:20]]
        # Use try_parse_numeric so "94%" → 94.0
        values = [try_parse_numeric(r[value_col_idx]) or 0 for r in rows[:20]]

    colors = generate_chart_colors(len(values))
    chart_title = value_col_name
    if label_col_name and label_col_name != value_col_name:
        chart_title += f" by {label_col_name}"

    return {
        'type': 'bar',
        'title': chart_title,
        'data': {
            'labels': labels,
            'datasets': [{
                'label': value_col_name,
                'data': values,
                'backgroundColor': colors,
                'borderRadius': 8,
            }]
        },
        'options': {
            'responsive': True,
            'maintainAspectRatio': False,
            'plugins': {
                'legend': {'display': False},
                'title': {
                    'display': True,
                    'text': chart_title,
                    'font': {'size': 14, 'weight': 'bold'},
                    'color': '#1f2937'
                }
            },
            'scales': {
                'y': {
                    'beginAtZero': True,
                    'grid': {'color': 'rgba(148, 163, 184, 0.1)'},
                    'ticks': {'color': '#6b7280', 'font': {'size': 11}}
                },
                'x': {
                    'grid': {'display': False},
                    'ticks': {
                        'color': '#6b7280',
                        'font': {'size': 10},
                        'maxRotation': 45,
                        'minRotation': 0
                    }
                }
            }
        }
    }


def generate_chart_colors(count):
    teal_gradient = [
        '#0D9488', '#14B8A6', '#2DD4BF', '#5EEAD4',
        '#99F6E4', '#CCFBF1', '#0F766E', '#115E59'
    ]
    return [teal_gradient[i % len(teal_gradient)] for i in range(count)]


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@bp.route('/api/query', methods=['POST'])
@login_required
def handle_query():
    try:
        data = request.get_json()
        file_id = data.get('file_id')
        question = data.get('question')
        response_type = data.get('response_type', 'text')

        if not file_id or not question:
            return jsonify({'success': False, 'message': 'Missing file_id or question'}), 400

        file_record = UploadedFile.query.filter_by(
            id=file_id, user_id=current_user.id
        ).first()

        if not file_record:
            return jsonify({'success': False, 'message': 'File not found'}), 404

        schema = get_table_schema(file_record.table_name)

        start_time = time.time()
        result = process_natural_language_query(
            question=question,
            table_name=file_record.table_name,
            schema=schema
        )
        execution_time = time.time() - start_time

        query_record = QueryHistory(
            user_id=current_user.id,
            file_id=file_id,
            question=question,
            sql_query=result.get('sql_query'),
            execution_time=execution_time
        )
        db.session.add(query_record)
        db.session.commit()

        if result['result_type'] == 'table':
            if response_type == 'chart':
                chart_config = generate_chart_from_data(
                    result['columns'],
                    result['rows'],
                    question,
                    sql_query=result.get('sql_query')
                )
                if chart_config:
                    return jsonify({
                        'success': True,
                        'result_type': 'chart',
                        'answer': f"Generated chart with {len(result['rows'])} data points",
                        'chart': chart_config,
                        'data': {
                            'columns': result['columns'],
                            'rows': result['rows'],
                            'row_count': len(result['rows'])
                        },
                        'sql_query': result.get('sql_query')
                    })
                else:
                    return jsonify({
                        'success': True,
                        'result_type': 'table',
                        'answer': result['answer'] + " (Chart not available for this data)",
                        'data': {
                            'columns': result['columns'],
                            'rows': result['rows'],
                            'row_count': len(result['rows'])
                        },
                        'sql_query': result.get('sql_query')
                    })
            else:
                return jsonify({
                    'success': True,
                    'result_type': 'table',
                    'answer': result['answer'],
                    'data': {
                        'columns': result['columns'],
                        'rows': result['rows'],
                        'row_count': len(result['rows'])
                    },
                    'sql_query': result.get('sql_query')
                })
        else:
            return jsonify({
                'success': True,
                'result_type': 'value',
                'answer': result['answer'],
                'value': result.get('value'),
                'sql_query': result.get('sql_query')
            })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Error processing query: {str(e)}'}), 500