from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, send_file
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from ..models.user import db, UploadedFile
from ..utils.file_processor import process_uploaded_file
from ..utils.db_utils import get_table_preview, get_table_schema, execute_query, drop_table
import os
import csv
import io

bp = Blueprint('main', __name__)

ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@bp.route('/')
def index():
    """
    Homepage - Landing page with Sign In / Sign Up
    If user is logged in, redirect to dashboard
    """
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    
    return render_template('index.html')


@bp.route('/dashboard')
@login_required
def dashboard():
    """Main dashboard - shows user's uploaded files"""
    # Get all files for current user
    files = UploadedFile.query.filter_by(user_id=current_user.id)\
                               .order_by(UploadedFile.uploaded_at.desc())\
                               .all()
    
    # Calculate statistics
    total_files = len(files)
    total_queries = len(current_user.query_history) if hasattr(current_user, 'query_history') else 0
    total_storage = sum([f.file_size or 0 for f in files])
    
    stats = {
        'total_files': total_files,
        'total_queries': total_queries,
        'storage_used': total_storage,
        'storage_mb': round(total_storage / (1024 * 1024), 2)
    }
    
    return render_template('dashboard.html', 
                         user=current_user,
                         files=files,
                         stats=stats)


@bp.route('/upload', methods=['POST'])
@login_required
def upload_file():
    """Handle file upload - ALWAYS returns JSON for AJAX"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file selected'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'success': False, 'message': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'success': False, 'message': 'Only Excel (.xlsx, .xls) and CSV files are allowed'}), 400
        
        result = process_uploaded_file(file, current_user.id)
        
        return jsonify({
            'success': True,
            'message': 'File uploaded successfully!',
            'file': {
                'id': result['id'],
                'filename': result['original_filename'],
                'rows': result['rows'],
                'columns': result['columns']
            }
        })
        
    except Exception as e:
        print(f"Upload error: {str(e)}")
        return jsonify({'success': False, 'message': f'Error uploading file: {str(e)}'}), 500


@bp.route('/file/<int:file_id>')
@login_required
def view_file(file_id):
    """View uploaded file in spreadsheet interface"""
    try:
        file_record = UploadedFile.query.filter_by(id=file_id, user_id=current_user.id).first_or_404()
        schema = get_table_schema(file_record.table_name)
        preview_data = get_table_preview(file_record.table_name, limit=100)
        
        return render_template('spreadsheet.html',
                             user=current_user,
                             file=file_record,
                             schema=schema,
                             preview_data=preview_data)
    except Exception as e:
        print(f"View file error: {str(e)}")
        flash(f'Error loading file: {str(e)}', 'error')
        return redirect(url_for('main.dashboard'))


@bp.route('/api/file/<int:file_id>/data')
@login_required
def get_file_data(file_id):
    """
    API endpoint to get file data with pagination
    NEW: Added for "Load More" functionality
    """
    try:
        # Verify file belongs to user
        file_record = UploadedFile.query.filter_by(
            id=file_id,
            user_id=current_user.id
        ).first_or_404()
        
        # Get pagination parameters from URL
        offset = request.args.get('offset', 0, type=int)
        limit = request.args.get('limit', 100, type=int)
        
        # Limit maximum rows per request (prevent overload)
        limit = min(limit, 5000)
        
        # Get data from database
        data = get_table_preview(file_record.table_name, limit=limit, offset=offset)
        
        return jsonify({
            'success': True,
            'data': data,
            'offset': offset,
            'limit': limit,
            'rows_returned': len(data['rows'])
        })
        
    except Exception as e:
        print(f"Get data error: {str(e)}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


@bp.route('/file/<int:file_id>/delete', methods=['POST'])
@login_required
def delete_file(file_id):
    """Delete uploaded file"""
    try:
        file_record = UploadedFile.query.filter_by(id=file_id, user_id=current_user.id).first_or_404()
        
        if os.path.exists(file_record.file_path):
            os.remove(file_record.file_path)
        
        drop_table(file_record.table_name)
        db.session.delete(file_record)
        db.session.commit()
        
        return jsonify({'success': True, 'message': 'File deleted successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@bp.route('/file/<int:file_id>/download')
@login_required
def download_file(file_id):
    try:
        file_record = UploadedFile.query.filter_by(
            id=file_id,
            user_id=current_user.id
        ).first_or_404()

        # Always resolve to an absolute path so send_file works correctly
        abs_path = os.path.abspath(file_record.file_path)
        print(f"[DOWNLOAD] Resolved path: {abs_path}")

        if os.path.exists(abs_path):
            return send_file(
                abs_path,
                as_attachment=True,
                download_name=file_record.original_filename
            )

        # Fallback: regenerate CSV from the database table
        print("[DOWNLOAD] File missing on disk — regenerating from DB")
        import csv, io

        data = get_table_preview(file_record.table_name, limit=100_000)

        if not data or not data.get('columns'):
            flash('File data could not be found.', 'error')
            return redirect(url_for('main.dashboard'))

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(data['columns'])
        writer.writerows(data['rows'])
        buf.seek(0)

        base_name = os.path.splitext(file_record.original_filename)[0]

        return send_file(
            io.BytesIO(buf.getvalue().encode('utf-8')),
            as_attachment=True,
            download_name=base_name + '.csv',
            mimetype='text/csv'
        )

    except Exception as e:
        import traceback
        print(f"[DOWNLOAD] Error: {e}")
        traceback.print_exc()
        flash(f'Error downloading file: {str(e)}', 'error')
        return redirect(url_for('main.dashboard'))