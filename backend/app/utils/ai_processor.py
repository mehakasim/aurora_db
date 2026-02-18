"""
AI Processor - Natural Language to SQL Conversion
Uses Ollama (Llama 3.2) to convert questions to SQL queries
"""
import ollama
import json
import re
from ..utils.db_utils import execute_query


def process_natural_language_query(question, table_name, schema):
    """
    Convert natural language question to SQL and execute
    
    Args:
        question: User's natural language question
        table_name: Name of the table to query
        schema: Table schema (columns and types)
    
    Returns:
        dict with:
        - result_type: 'table' or 'value'
        - answer: Natural language answer
        - columns/rows: If table result
        - value: If single value result
        - sql_query: Generated SQL
    """
    
    # Create prompt for Llama to generate SQL
    prompt = f"""You are a SQL expert. Convert the natural language question to a SQL query.

Table: {table_name}
Columns: {', '.join(schema['columns'])}
Column Types: {dict(zip(schema['columns'], schema['types']))}

Question: {question}

Rules:
1. Return ONLY valid SQLite SQL query
2. Use SELECT statement
3. Table name is exactly: {table_name}
4. Column names are exactly as listed above
5. For counting, use COUNT(*)
6. For filtering, use WHERE clause
7. For aggregations (sum, average, count), return single value
8. Limit results to 100 rows unless specifically asked for more

Return ONLY the SQL query, nothing else. No explanations, no markdown, just SQL.

SQL Query:"""

    try:
        # Call Ollama
        response = ollama.chat(
            model='llama3.2:3b',
            messages=[{
                'role': 'user',
                'content': prompt
            }]
        )
        
        # Extract SQL query from response
        sql_query = response['message']['content'].strip()
        
        # Clean up the SQL query
        sql_query = clean_sql_query(sql_query)
        
        print(f"Generated SQL: {sql_query}")
        
        # Execute the query
        result = execute_query(sql_query, table_name)
        
        # Determine result type and format response
        if is_single_value_query(sql_query):
            # Single value result (COUNT, SUM, AVG, etc.)
            value = result['rows'][0][0] if result['rows'] else 0
            answer = format_single_value_answer(question, value, result['columns'][0])
            
            return {
                'result_type': 'value',
                'answer': answer,
                'value': value,
                'sql_query': sql_query
            }
        else:
            # Tabular result
            answer = format_table_answer(question, len(result['rows']))
            
            return {
                'result_type': 'table',
                'answer': answer,
                'columns': result['columns'],
                'rows': result['rows'],
                'sql_query': sql_query
            }
            
    except Exception as e:
        print(f"AI processing error: {str(e)}")
        # Fallback: try simple pattern matching
        return fallback_query_processing(question, table_name, schema)


def clean_sql_query(sql):
    """Clean SQL query from AI response"""
    # Remove markdown code blocks
    sql = re.sub(r'```sql\n?', '', sql)
    sql = re.sub(r'```\n?', '', sql)
    
    # Remove any explanatory text after semicolon
    if ';' in sql:
        sql = sql.split(';')[0] + ';'
    
    # Remove leading/trailing whitespace
    sql = sql.strip()
    
    # Ensure it ends with semicolon
    if not sql.endswith(';'):
        sql += ';'
    
    return sql


def is_single_value_query(sql):
    """Check if query returns a single value"""
    sql_upper = sql.upper()
    
    # Check for aggregation functions
    aggregations = ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']
    for agg in aggregations:
        if agg in sql_upper:
            # Make sure it's not selecting multiple columns
            if sql_upper.count('SELECT') == 1 and sql_upper.count(',') == 0:
                return True
    
    return False


def format_single_value_answer(question, value, column_name):
    """Format natural language answer for single value"""
    question_lower = question.lower()
    
    if 'count' in question_lower or 'how many' in question_lower:
        return f"There are **{value}** matching records."
    elif 'sum' in question_lower or 'total' in question_lower:
        return f"The total is **{value}**."
    elif 'average' in question_lower or 'mean' in question_lower:
        return f"The average is **{value:.2f}**."
    elif 'maximum' in question_lower or 'max' in question_lower or 'highest' in question_lower:
        return f"The maximum value is **{value}**."
    elif 'minimum' in question_lower or 'min' in question_lower or 'lowest' in question_lower:
        return f"The minimum value is **{value}**."
    else:
        return f"The result is **{value}**."


def format_table_answer(question, row_count):
    """Format natural language answer for table results"""
    if row_count == 0:
        return "No matching records found."
    elif row_count == 1:
        return f"Found **1 record** matching your query. Results are displayed in the spreadsheet below."
    else:
        return f"Found **{row_count} records** matching your query. Results are displayed in the spreadsheet below."


def fallback_query_processing(question, table_name, schema):
    """
    Fallback method using simple pattern matching
    Used when Ollama is not available
    """
    question_lower = question.lower()
    
    # Simple count query
    if 'count' in question_lower or 'how many' in question_lower:
        sql = f"SELECT COUNT(*) as count FROM {table_name};"
        result = execute_query(sql)
        value = result['rows'][0][0]
        
        return {
            'result_type': 'value',
            'answer': f"There are **{value}** total records.",
            'value': value,
            'sql_query': sql
        }
    
    # Simple show all
    elif 'show' in question_lower or 'display' in question_lower:
        sql = f"SELECT * FROM {table_name} LIMIT 100;"
        result = execute_query(sql)
        
        return {
            'result_type': 'table',
            'answer': f"Showing first {len(result['rows'])} records.",
            'columns': result['columns'],
            'rows': result['rows'],
            'sql_query': sql
        }
    
    # Default: show first 10 rows
    else:
        sql = f"SELECT * FROM {table_name} LIMIT 10;"
        result = execute_query(sql)
        
        return {
            'result_type': 'table',
            'answer': "Here are the first 10 records.",
            'columns': result['columns'],
            'rows': result['rows'],
            'sql_query': sql
        }