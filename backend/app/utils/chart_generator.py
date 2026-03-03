"""
Chart Generator - Automatically generates visualizations from uploaded data
"""
import pandas as pd
import numpy as np
from ..utils.db_utils import execute_query, get_table_preview
import random


def generate_visualizations_for_file(file_record):
    """
    Analyze uploaded file and generate appropriate visualizations
    
    Returns:
        dict with discovery_charts and detailed_reports
    """
    # Get data from database
    table_name = file_record.table_name
    data = get_table_preview(table_name, limit=1000)
    
    # Convert to DataFrame for analysis
    df = pd.DataFrame(data['rows'], columns=data['columns'])
    
    # Generate discovery cards (3 quick insights)
    discovery_charts = generate_discovery_cards(df, table_name)
    
    # Generate detailed reports (2-3 in-depth analyses)
    detailed_reports = generate_detailed_reports(df, table_name, file_record.original_filename)
    
    return {
        'discovery_charts': discovery_charts,
        'detailed_reports': detailed_reports,
        'chart_data': {}  # Additional metadata if needed
    }


def generate_discovery_cards(df, table_name):
    """Generate 3 discovery card previews"""
    cards = []
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    if len(numeric_cols) >= 1:
        # Card 1: First numeric column distribution
        col = numeric_cols[0]
        top_5 = df.nlargest(5, col) if len(df) >= 5 else df
        
        cards.append({
            'id': 'discovery-1',
            'title': f'Top Values - {col}',
            'type': 'Bar Chart',
            'icon': 'bar_chart',
            'preview_data': {
                'type': 'bar',
                'labels': [str(i+1) for i in range(len(top_5))],
                'values': top_5[col].tolist()[:5],
                'colors': generate_gradient_colors(len(top_5), '#0D9488')
            }
        })
    
    if len(numeric_cols) >= 2:
        # Card 2: Trend of second numeric column
        col = numeric_cols[1]
        sample_data = df[col].head(10).tolist() if len(df) >= 10 else df[col].tolist()
        
        cards.append({
            'id': 'discovery-2',
            'title': f'{col} Trend',
            'type': 'Line Chart',
            'icon': 'show_chart',
            'preview_data': {
                'type': 'line',
                'labels': [str(i+1) for i in range(len(sample_data))],
                'values': sample_data,
                'colors': ['#0D9488']
            }
        })
    
    # Card 3: Row count or category distribution
    if len(df) > 0:
        cards.append({
            'id': 'discovery-3',
            'title': f'Dataset Overview',
            'type': 'Summary',
            'icon': 'pie_chart',
            'preview_data': {
                'type': 'doughnut',
                'labels': ['Complete', 'Remaining'],
                'values': [min(len(df), 100), max(0, len(df) - 100)],
                'colors': ['#0D9488', '#e2e8f0']
            }
        })
    
    return cards


def generate_detailed_reports(df, table_name, filename):
    """Generate 2-3 detailed chart reports"""
    reports = []
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    # Report 1: Top 5 by first numeric column
    if len(numeric_cols) >= 1:
        col = numeric_cols[0]
        top_5 = df.nlargest(5, col) if len(df) >= 5 else df.head(5)
        
        labels = []
        values = []
        for idx, row in top_5.iterrows():
            # Try to find a good label column
            label_col = df.columns[0] if df.columns[0] != col else f"Row {idx+1}"
            label = str(row[label_col] if label_col in df.columns else f"Item {idx+1}")
            labels.append(label[:15])  # Truncate long labels
            values.append(float(row[col]))
        
        colors = generate_gradient_colors(len(labels), '#0D9488')
        
        # Generate AI insight
        avg_val = np.mean(values)
        max_val = max(values)
        max_label = labels[values.index(max_val)]
        
        insight = f"{max_label} shows the highest value at {format_number(max_val)}. "
        insight += f"The average across top performers is {format_number(avg_val)}. "
        insight += f"Consider focusing resources on maintaining {max_label}'s performance while improving lower-ranked items."
        
        reports.append({
            'id': 'report-1',
            'category': filename.upper().replace('.', '_'),
            'title': f'Top 5 by {col}',
            'data': [{'label': l, 'value': format_number(v)} for l, v in zip(labels, values)],
            'chart_data': {
                'labels': labels,
                'values': values,
                'colors': colors
            },
            'insight': insight
        })
    
    # Report 2: Second numeric column analysis
    if len(numeric_cols) >= 2:
        col = numeric_cols[1]
        top_5 = df.nlargest(5, col) if len(df) >= 5 else df.head(5)
        
        labels = []
        values = []
        for idx, row in top_5.iterrows():
            label_col = df.columns[0] if df.columns[0] != col else f"Row {idx+1}"
            label = str(row[label_col] if label_col in df.columns else f"Item {idx+1}")
            labels.append(label[:15])
            values.append(float(row[col]))
        
        colors = generate_gradient_colors(len(labels), '#14B8A6', reverse=True)
        
        total = sum(values)
        max_val = max(values)
        max_label = labels[values.index(max_val)]
        percentage = (max_val / total * 100) if total > 0 else 0
        
        insight = f"{max_label} accounts for {percentage:.1f}% of total {col}. "
        insight += f"Distribution shows {'strong concentration' if percentage > 40 else 'balanced spread'} across categories. "
        insight += "Recommendation: Monitor trends to maintain competitive positioning."
        
        reports.append({
            'id': 'report-2',
            'category': 'PERFORMANCE METRICS',
            'title': f'{col} Distribution',
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
    """Generate gradient colors from base color"""
    # Predefined gradients for teal
    teal_gradient = [
        '#114B45',  # Darkest
        '#1A5C55',
        '#247A70',
        '#2EA396',
        '#39C9B9',  # Lightest
    ]
    
    light_gradient = [
        '#15B0A1',
        '#20C9B9',
        '#3AE0D0',
        '#6BF2E5',
        '#A8F8F0',
    ]
    
    gradient = light_gradient if reverse else teal_gradient
    
    if count <= len(gradient):
        return gradient[:count]
    
    # If need more colors, repeat pattern
    result = []
    for i in range(count):
        result.append(gradient[i % len(gradient)])
    
    return result


def format_number(num):
    """Format number for display"""
    try:
        num = float(num)
        if num >= 1000000:
            return f"${num/1000000:.1f}M"
        elif num >= 1000:
            return f"${num/1000:.0f}K"
        elif num == int(num):
            return str(int(num))
        else:
            return f"{num:.2f}"
    except:
        return str(num)


def analyze_data_for_insights(df):
    """Generate AI-powered insights from data analysis"""
    insights = []
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    for col in numeric_cols[:3]:  # Analyze first 3 numeric columns
        # Calculate statistics
        mean_val = df[col].mean()
        std_val = df[col].std()
        max_val = df[col].max()
        min_val = df[col].min()
        
        # Generate insight based on distribution
        if std_val / mean_val < 0.2:  # Low variance
            insight = f"{col} shows consistent values with low variation (±{std_val:.1f}). Data is stable."
        elif std_val / mean_val > 1.0:  # High variance
            insight = f"{col} shows high variability. Consider investigating outliers or segmenting data."
        else:
            insight = f"{col} displays normal distribution. Mean: {mean_val:.1f}, Range: {min_val:.1f} to {max_val:.1f}."
        
        insights.append(insight)
    
    return insights