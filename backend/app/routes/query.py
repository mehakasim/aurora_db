"""
Query Routes - AI-Powered Spreadsheet Queries
Handles natural language queries and returns results
"""
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from ..models.user import db, UploadedFile, QueryHistory
from ..utils.ai_processor import process_natural_language_query
from ..utils.db_utils import execute_query, get_table_schema
import time

bp = Blueprint('query', __name__)


@bp.route('/api/query', methods=['POST'])
@login_required
def handle_query():
    """
    Handle AI query from spreadsheet chat
    Returns either:
    - Tabular results (for filtering/aggregations that return rows)
    - Single value (for counts/sums/averages)
    """
    try:
        data = request.get_json()
        file_id = data.get('file_id')
        question = data.get('question')
        
        if not file_id or not question:
            return jsonify({
                'success': False,
                'message': 'Missing file_id or question'
            }), 400
        
        # Get file record
        file_record = UploadedFile.query.filter_by(
            id=file_id,
            user_id=current_user.id
        ).first()
        
        if not file_record:
            return jsonify({
                'success': False,
                'message': 'File not found'
            }), 404
        
        # Get table schema for context
        schema = get_table_schema(file_record.table_name)
        
        # Process query with AI
        start_time = time.time()
        result = process_natural_language_query(
            question=question,
            table_name=file_record.table_name,
            schema=schema
        )
        execution_time = time.time() - start_time
        
        # Save to query history
        query_record = QueryHistory(
            user_id=current_user.id,
            file_id=file_id,
            question=question,
            sql_query=result.get('sql_query'),
            execution_time=execution_time
        )
        db.session.add(query_record)
        db.session.commit()
        
        # Return response based on result type
        if result['result_type'] == 'table':
            # Tabular data - will be displayed in spreadsheet
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
            # Single value - display in chat only
            return jsonify({
                'success': True,
                'result_type': 'value',
                'answer': result['answer'],
                'value': result.get('value'),
                'sql_query': result.get('sql_query')
            })
            
    except Exception as e:
        print(f"Query error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return jsonify({
            'success': False,
            'message': f'Error processing query: {str(e)}'
        }), 500