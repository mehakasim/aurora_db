"""
Smart Chart Generator - Dataset-Specific Visualizations
Analyzes data and creates relevant charts automatically
"""
import pandas as pd
import numpy as np
from ..utils.db_utils import execute_query, get_table_preview
import json


def generate_visualizations_for_file(file_record):
    """
    Analyze dataset and generate specific, relevant visualizations
    NO GENERIC CHARTS - Only what makes sense for the data
    """
    table_name = file_record.table_name
    data = get_table_preview(table_name, limit=1000)
    
    # Convert to DataFrame
    df = pd.DataFrame(data['rows'], columns=data['columns'])
    
    # Analyze dataset to determine chart types
    analysis = analyze_dataset(df)
    
    # Generate discovery cards based on dataset
    discovery_charts = generate_smart_discovery_cards(df, analysis)
    
    # Generate detailed reports based on dataset
    detailed_reports = generate_smart_detailed_reports(df, analysis, table_name, file_record.original_filename)
    
    return {
        'discovery_charts': discovery_charts,
        'detailed_reports': detailed_reports,
        'chart_data': {},
        'dataset_type': analysis['dataset_type']
    }


def analyze_dataset(df):
    """
    Analyze dataset to understand what type of data it contains
    Returns insights about the data structure
    """
    analysis = {
        'dataset_type': 'unknown',
        'numeric_columns': [],
        'categorical_columns': [],
        'date_columns': [],
        'score_columns': [],
        'demographic_columns': [],
        'time_columns': [],
        'key_metrics': []
    }
    
    for col in df.columns:
        col_lower = col.lower()
        
        # Identify column types
        if df[col].dtype in ['int64', 'float64']:
            analysis['numeric_columns'].append(col)
            
            # Check if it's a score/metric
            if any(term in col_lower for term in ['score', 'mark', 'grade', 'rating', 'percent']):
                analysis['score_columns'].append(col)
                analysis['key_metrics'].append(col)
            
            # Check if it's time-related
            elif any(term in col_lower for term in ['hour', 'minute', 'time', 'duration']):
                analysis['time_columns'].append(col)
                analysis['key_metrics'].append(col)
            
            # Other metrics
            elif any(term in col_lower for term in ['level', 'index', 'count', 'amount', 'intake']):
                analysis['key_metrics'].append(col)
        
        else:
            analysis['categorical_columns'].append(col)
            
            # Demographic columns
            if any(term in col_lower for term in ['gender', 'age', 'level', 'type', 'category', 'status']):
                analysis['demographic_columns'].append(col)
    
    # Determine dataset type
    if any('student' in col.lower() for col in df.columns) or any('exam' in col.lower() for col in df.columns):
        analysis['dataset_type'] = 'student_performance'
    elif any('sales' in col.lower() for col in df.columns) or any('revenue' in col.lower() for col in df.columns):
        analysis['dataset_type'] = 'sales'
    elif any('customer' in col.lower() for col in df.columns):
        analysis['dataset_type'] = 'customer'
    elif any('employee' in col.lower() for col in df.columns):
        analysis['dataset_type'] = 'employee'
    else:
        analysis['dataset_type'] = 'general'
    
    return analysis


def generate_smart_discovery_cards(df, analysis):
    """
    Generate discovery cards based on actual data characteristics
    """
    cards = []
    
    # Card 1: Main score/metric distribution (if exists)
    if analysis['score_columns']:
        main_score = analysis['score_columns'][0]
        
        # Create bins for score distribution
        bins = [0, 40, 60, 80, 100]
        labels = ['Low (0-40)', 'Medium (40-60)', 'Good (60-80)', 'Excellent (80-100)']
        
        try:
            df['score_category'] = pd.cut(df[main_score], bins=bins, labels=labels)
            distribution = df['score_category'].value_counts().sort_index()
            
            cards.append({
                'id': 'discovery-1',
                'title': f'{main_score} Distribution',
                'type': 'Bar Chart',
                'icon': 'bar_chart',
                'preview_data': {
                    'type': 'bar',
                    'labels': distribution.index.tolist(),
                    'values': distribution.values.tolist(),
                    'colors': ['#ef4444', '#f59e0b', '#10b981', '#0ea5e9']
                }
            })
        except:
            pass
    
    # Card 2: Demographic breakdown (if exists)
    if analysis['demographic_columns']:
        demo_col = analysis['demographic_columns'][0]
        distribution = df[demo_col].value_counts().head(5)
        
        cards.append({
            'id': 'discovery-2',
            'title': f'{demo_col} Distribution',
            'type': 'Pie Chart',
            'icon': 'pie_chart',
            'preview_data': {
                'type': 'doughnut',
                'labels': distribution.index.tolist(),
                'values': distribution.values.tolist(),
                'colors': generate_gradient_colors(len(distribution), '#0D9488')
            }
        })
    
    # Card 3: Time-based metric (if exists)
    if analysis['time_columns']:
        time_col = analysis['time_columns'][0]
        avg_value = df[time_col].mean()
        
        # Show average vs categories
        if analysis['categorical_columns']:
            cat_col = analysis['categorical_columns'][0]
            time_by_category = df.groupby(cat_col)[time_col].mean().head(5)
            
            cards.append({
                'id': 'discovery-3',
                'title': f'Average {time_col} by {cat_col}',
                'type': 'Bar Chart',
                'icon': 'schedule',
                'preview_data': {
                    'type': 'bar',
                    'labels': time_by_category.index.tolist(),
                    'values': time_by_category.values.tolist(),
                    'colors': generate_gradient_colors(len(time_by_category), '#8b5cf6')
                }
            })
    
    # If no specific cards created, use top numeric columns
    if not cards and analysis['numeric_columns']:
        for i, col in enumerate(analysis['numeric_columns'][:3]):
            top_values = df.nlargest(5, col)
            
            cards.append({
                'id': f'discovery-{i+1}',
                'title': f'Top 5 by {col}',
                'type': 'Bar Chart',
                'icon': 'bar_chart',
                'preview_data': {
                    'type': 'bar',
                    'labels': [str(j+1) for j in range(len(top_values))],
                    'values': top_values[col].tolist(),
                    'colors': generate_gradient_colors(len(top_values), '#0D9488')
                }
            })
    
    return cards[:3]  # Max 3 discovery cards


def generate_smart_detailed_reports(df, analysis, table_name, filename):
    """
    Generate detailed reports based on dataset characteristics
    """
    reports = []
    
    # Report 1: Performance Analysis (if scores exist)
    if analysis['score_columns']:
        main_score = analysis['score_columns'][0]
        
        # Top performers
        top_performers = df.nlargest(10, main_score)
        
        labels = []
        values = []
        
        for idx, row in top_performers.iterrows():
            # Try to find a name/ID column
            label_col = None
            for col in df.columns:
                if 'id' in col.lower() or 'name' in col.lower():
                    label_col = col
                    break
            
            if label_col:
                label = str(row[label_col])[:20]
            else:
                label = f"Record {idx+1}"
            
            labels.append(label)
            values.append(float(row[main_score]))
        
        colors = generate_performance_colors(values, max(values))
        
        # Generate insight
        avg_score = df[main_score].mean()
        max_score = max(values)
        min_score = df[main_score].min()
        std_score = df[main_score].std()
        
        insight = f"Top performer achieved {max_score:.1f}. "
        insight += f"Average {main_score.replace('_', ' ')} is {avg_score:.1f}. "
        
        if std_score < 10:
            insight += f"Scores are consistent (low variation). "
        elif std_score > 20:
            insight += f"Wide range of performance observed. "
        
        if avg_score > 75:
            insight += "Overall performance is strong."
        elif avg_score > 50:
            insight += "Overall performance is moderate."
        else:
            insight += "Consider interventions to improve performance."
        
        reports.append({
            'id': 'report-1',
            'category': 'PERFORMANCE ANALYSIS',
            'title': f'Top 10 by {main_score}',
            'data': [{'label': l, 'value': format_number(v)} for l, v in zip(labels, values)],
            'chart_data': {
                'labels': labels,
                'values': values,
                'colors': colors
            },
            'insight': insight
        })
    
    # Report 2: Correlation Analysis (if multiple metrics exist)
    if len(analysis['key_metrics']) >= 2:
        metric1 = analysis['key_metrics'][0]
        metric2 = analysis['key_metrics'][1]
        
        # Calculate correlation
        correlation = df[metric1].corr(df[metric2])
        
        # Group data for visualization
        if analysis['categorical_columns']:
            cat_col = analysis['categorical_columns'][0]
            grouped = df.groupby(cat_col)[[metric1, metric2]].mean().head(5)
            
            labels = grouped.index.tolist()
            values = grouped[metric1].tolist()
            colors = generate_gradient_colors(len(labels), '#14B8A6', reverse=True)
            
            # Generate insight
            insight = f"Analysis shows "
            if abs(correlation) > 0.7:
                direction = "strong positive" if correlation > 0 else "strong negative"
                insight += f"{direction} correlation ({correlation:.2f}) between {metric1.replace('_', ' ')} and {metric2.replace('_', ' ')}. "
            elif abs(correlation) > 0.4:
                direction = "moderate positive" if correlation > 0 else "moderate negative"
                insight += f"{direction} correlation ({correlation:.2f}) between these metrics. "
            else:
                insight += f"weak correlation ({correlation:.2f}) between these metrics. "
            
            top_category = grouped[metric1].idxmax()
            insight += f"{top_category} shows highest {metric1.replace('_', ' ')}."
            
            reports.append({
                'id': 'report-2',
                'category': 'COMPARATIVE ANALYSIS',
                'title': f'{metric1} by Category',
                'data': [{'label': l, 'value': format_number(v)} for l, v in zip(labels, values)],
                'chart_data': {
                    'labels': labels,
                    'values': values,
                    'colors': colors
                },
                'insight': insight
            })
    
    # Report 3: Time-based analysis (if time columns exist)
    if analysis['time_columns'] and analysis['categorical_columns']:
        time_col = analysis['time_columns'][0]
        cat_col = analysis['categorical_columns'][0]
        
        time_distribution = df.groupby(cat_col)[time_col].mean().sort_values(ascending=False).head(5)
        
        labels = time_distribution.index.tolist()
        values = time_distribution.values.tolist()
        colors = generate_gradient_colors(len(labels), '#8b5cf6')
        
        avg_time = df[time_col].mean()
        max_time = max(values)
        max_category = labels[0]
        
        insight = f"{max_category} shows highest {time_col.replace('_', ' ')} at {max_time:.1f}. "
        insight += f"Average across all categories is {avg_time:.1f}. "
        
        if max_time > avg_time * 1.5:
            insight += f"Significant variation detected - {max_category} is 50% above average."
        else:
            insight += "Distribution is relatively balanced across categories."
        
        reports.append({
            'id': 'report-3',
            'category': 'TIME ANALYSIS',
            'title': f'Average {time_col} by {cat_col}',
            'data': [{'label': l, 'value': format_number(v)} for l, v in zip(labels, values)],
            'chart_data': {
                'labels': labels,
                'values': values,
                'colors': colors
            },
            'insight': insight
        })
    
    return reports


def generate_gradient_colors(count, base_color='#0D9488', reverse=False):
    """Generate gradient colors"""
    teal_gradient = ['#114B45', '#1A5C55', '#247A70', '#2EA396', '#39C9B9']
    light_gradient = ['#15B0A1', '#20C9B9', '#3AE0D0', '#6BF2E5', '#A8F8F0']
    purple_gradient = ['#6b21a8', '#7c3aed', '#8b5cf6', '#a78bfa', '#c4b5fd']
    
    if '#8b5cf6' in base_color:
        gradient = purple_gradient
    elif reverse:
        gradient = light_gradient
    else:
        gradient = teal_gradient
    
    if count <= len(gradient):
        return gradient[:count]
    
    result = []
    for i in range(count):
        result.append(gradient[i % len(gradient)])
    
    return result


def generate_performance_colors(values, max_value):
    """Generate colors based on performance levels"""
    colors = []
    for val in values:
        percentage = (val / max_value) * 100
        if percentage >= 90:
            colors.append('#059669')  # Green - Excellent
        elif percentage >= 75:
            colors.append('#0ea5e9')  # Blue - Good
        elif percentage >= 60:
            colors.append('#f59e0b')  # Orange - Average
        else:
            colors.append('#ef4444')  # Red - Needs improvement
    return colors


def format_number(num):
    """Format number for display"""
    try:
        num = float(num)
        if num >= 1000000:
            return f"{num/1000000:.1f}M"
        elif num >= 1000:
            return f"{num/1000:.1f}K"
        elif num == int(num):
            return str(int(num))
        else:
            return f"{num:.2f}"
    except:
        return str(num)