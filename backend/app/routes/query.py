"""Query Routes - AI-powered SQL generation"""
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user

bp = Blueprint('query', __name__, url_prefix='/api')

@bp.route('/query', methods=['POST'])
@login_required
def process_query():
    data = request.get_json()
    question = data.get('question', '')
    
    if not question:
        return jsonify({'error': 'Question required'}), 400
    
    # TODO: Implement AI query processing
    return jsonify({
        'message': 'AI query feature coming soon!',
        'question': question
    })
