from flask import Blueprint, render_template, redirect, url_for, flash
from flask_login import login_required, current_user
from ..models.user import UploadedFile
from ..utils.chart_generator import generate_visualizations_for_file

bp = Blueprint('visualizations', __name__, url_prefix='/visualizations')


@bp.route('/')
@login_required
def index():
    """
    Main visualizations gallery page
    Shows charts from all user's uploaded files
    """
    # Get user's most recent file
    latest_file = UploadedFile.query.filter_by(user_id=current_user.id)\
                                     .order_by(UploadedFile.uploaded_at.desc())\
                                     .first()
    
    if not latest_file:
        flash('Please upload a file first to see visualizations', 'info')
        return redirect(url_for('main.dashboard'))
    
    # Generate visualizations
    try:
        viz_data = generate_visualizations_for_file(latest_file)
        
        return render_template('visualizations.html',
                             discovery_charts=viz_data['discovery_charts'],
                             detailed_reports=viz_data['detailed_reports'],
                             chart_data=viz_data['chart_data'])
    except Exception as e:
        print(f"Visualization error: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error generating visualizations: {str(e)}', 'error')
        return redirect(url_for('main.dashboard'))


@bp.route('/file/<int:file_id>')
@login_required
def view_file_charts(file_id):
    """
    View visualizations for a specific file
    """
    file_record = UploadedFile.query.filter_by(
        id=file_id,
        user_id=current_user.id
    ).first_or_404()
    
    try:
        viz_data = generate_visualizations_for_file(file_record)
        
        return render_template('visualizations.html',
                             file=file_record,
                             discovery_charts=viz_data['discovery_charts'],
                             detailed_reports=viz_data['detailed_reports'],
                             chart_data=viz_data['chart_data'])
    except Exception as e:
        print(f"Visualization error: {str(e)}")
        flash(f'Error generating visualizations: {str(e)}', 'error')
        return redirect(url_for('main.dashboard'))