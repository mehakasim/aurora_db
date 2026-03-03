"""
AI Processor - Natural Language to SQL Conversion
IMPROVED: Better error handling and column awareness
"""
import ollama
import json
import re
from ..utils.db_utils import execute_query


def process_natural_language_query(question, table_name, schema):
    """
    Convert natural language question to SQL and execute
    IMPROVED: Shows available columns if query fails
    """
    
    # Check if this is definitely a "show/display" query
    question_lower = question.lower()
    is_show_query = any(word in question_lower for word in ['show', 'display', 'list', 'find all', 'get all', 'give me'])
    is_count_only = 'count' in question_lower and 'show' not in question_lower and 'display' not in question_lower
    
    # First, try to map user's column names to actual columns
    column_mapping = fuzzy_match_columns(question_lower, schema['columns'])
    
    # Create enhanced prompt for Llama
    if is_show_query and not is_count_only:
        prompt = f"""You are a SQL expert. Convert this natural language question to a SQL SELECT query.

Table: {table_name}
ACTUAL COLUMNS (use these EXACT names): {', '.join(schema['columns'])}

Question: {question}

CRITICAL RULES:
1. This is a DISPLAY/SHOW query - use SELECT *
2. DO NOT use COUNT(*) - the user wants to see rows
3. Column names MUST be from the list above
4. If question mentions "students", "exam", "scores" etc but those columns don't exist, use Column_1, Column_2, Column_3 etc
5. For filtering, identify which column likely contains the data (e.g., Column_3 for scores)
6. Return ONLY the SQL query, nothing else
7. Limit to 1000 rows maximum

Example - if columns are Column_1, Column_2, Column_3:
Question: "Show students where exam scores > 80"
SQL: SELECT * FROM {table_name} WHERE Column_3 > 80 LIMIT 1000;

Now generate SQL for: {question}
Available columns: {', '.join(schema['columns'])}

SQL:"""
    else:
        prompt = f"""You are a SQL expert. Convert the natural language question to a SQL query.

Table: {table_name}
There might me be a Spelling Error in your query. Use Actual Column Names (use EXACTLY as listed): {', '.join(schema['columns'])}

Question: {question}

Rules:
1. Return ONLY valid SQLite SQL query
2. For "how many" or "count" queries → Use COUNT(*)
3. For "show", "display" queries → Use SELECT * with WHERE
4. Column names MUST be from the list above
5. If question refers to columns that don't exist (like "exam scores"), guess which actual column (like Column_3)
6. Limit SELECT queries to 1000 rows
7. Always end with semicolon

Available columns: {', '.join(schema['columns'])}

SQL Query:"""

    try:
        # Call Ollama
        response = ollama.chat(
            model='llama3.2:3b',
            messages=[{
                'role': 'user',
                'content': prompt
            }],
            options={
                'temperature': 0.1,
            }
        )
        
        sql_query = response['message']['content'].strip()
        sql_query = clean_sql_query(sql_query)
        
        print(f"Generated SQL: {sql_query}")
        
        # Execute the query
        try:
            result = execute_query(sql_query, table_name)
        except Exception as e:
            # Query failed - try fallback
            print(f"Query failed: {str(e)}")
            return create_helpful_error_response(question, schema, str(e))
        
        # Determine result type and format response
        if is_single_value_query(sql_query):
            value = result['rows'][0][0] if result['rows'] else 0
            answer = format_single_value_answer(question, value, result['columns'][0] if result['columns'] else 'count')
            
            return {
                'result_type': 'value',
                'answer': answer,
                'value': value,
                'sql_query': sql_query
            }
        else:
            if len(result['rows']) == 0:
                # No results - give helpful message
                return create_no_results_response(question, schema)
            
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
        import traceback
        traceback.print_exc()
        
        # Fallback: Use pattern matching
        return fallback_query_processing(question, table_name, schema)


def fuzzy_match_columns(question, columns):
    """Try to map terms in question to actual column names"""
    mapping = {}
    
    common_terms = {
        'score': ['Column_3', 'Column_4', 'Column_5'],
        'exam': ['Column_3', 'Column_4', 'Column_5'],
        'marks': ['Column_3', 'Column_4', 'Column_5'],
        'grade': ['Column_3', 'Column_4'],
        'name': ['Column_1', 'Column_2'],
        'student': ['Column_1'],
        'age': ['Column_2', 'Column_6'],
    }
    
    for term, possible_cols in common_terms.items():
        if term in question:
            for col in possible_cols:
                if col in columns:
                    mapping[term] = col
                    break
    
    return mapping


def create_helpful_error_response(question, schema, error):
    """Create helpful response when query fails"""
    return {
        'result_type': 'value',
        'answer': f"I couldn't execute that query. Your data has these columns: **{', '.join(schema['columns'])}**. Try asking about these specific columns instead!",
        'value': None,
        'sql_query': None
    }


def create_no_results_response(question, schema):
    """Create helpful response when no results found"""
    return {
        'result_type': 'value',
        'answer': f"No records found matching your criteria. Your data has columns: **{', '.join(schema['columns'])}**. Try checking which column contains the values you're filtering on (e.g., 'Show rows where Column_3 > 80').",
        'value': 0,
        'sql_query': None
    }


def clean_sql_query(sql):
    """Clean SQL query from AI response"""
    sql = re.sub(r'```sql\n?', '', sql)
    sql = re.sub(r'```\n?', '', sql)
    sql = re.sub(r'`', '', sql)
    
    lines = sql.split('\n')
    sql_lines = []
    found_sql = False
    
    for line in lines:
        line_upper = line.strip().upper()
        if line_upper.startswith(('SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE')):
            found_sql = True
        if found_sql:
            sql_lines.append(line)
    
    if sql_lines:
        sql = '\n'.join(sql_lines)
    
    if ';' in sql:
        sql = sql.split(';')[0] + ';'
    
    sql = sql.strip()
    
    if not sql.endswith(';'):
        sql += ';'
    
    return sql


def is_single_value_query(sql):
    """Check if query returns a single value"""
    sql_upper = sql.upper()
    
    aggregations = ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']
    
    for agg in aggregations:
        if agg in sql_upper:
            select_part = sql_upper.split('FROM')[0]
            if select_part.count(',') == 0:
                return True
    
    return False


def format_single_value_answer(question, value, column_name):
    """Format natural language answer for single value"""
    question_lower = question.lower()
    
    try:
        if isinstance(value, float):
            if value.is_integer():
                value = int(value)
            else:
                value = round(value, 2)
    except:
        pass
    
    if 'count' in question_lower or 'how many' in question_lower:
        return f"Total count is {value} matching your criteria."
    elif 'sum' in question_lower or 'total' in question_lower:
        return f"The total is **{value}**."
    elif 'average' in question_lower or 'mean' in question_lower or 'avg' in question_lower:
        return f"The average is **{value}**."
    elif 'maximum' in question_lower or 'max' in question_lower or 'highest' in question_lower:
        return f"The maximum value is **{value}**."
    elif 'minimum' in question_lower or 'min' in question_lower or 'lowest' in question_lower:
        return f"The minimum value is **{value}**."
    else:
        return f"The result is **{value}**."


def format_table_answer(question, row_count):
    """Format natural language answer for table results"""
    if row_count == 0:
        return "No records found matching your criteria."
    elif row_count == 1:
        return f"Found **1 record** matching your query. Displaying below in the spreadsheet."
    elif row_count >= 1000:
        return f"Found **{row_count}** records (showing first 1000). Results are displayed in the spreadsheet below."
    else:
        return f"Found **{row_count}** records matching your query. Results are displayed in the spreadsheet below."


def fallback_query_processing(question, table_name, schema):
    """
    Fallback method using simple pattern matching
    IMPROVED: Better column awareness
    """
    question_lower = question.lower()
    columns = schema['columns']
    
    # Pattern: "show/display ... where COLUMN OPERATOR VALUE"
    if any(word in question_lower for word in ['show', 'display', 'list', 'find', 'get']):
        where_clause = ""
        
        # Try to extract number from question
        numbers = re.findall(r'\d+', question_lower)
        
        if numbers and ('greater' in question_lower or '>' in question_lower or 'more' in question_lower or 'above' in question_lower):
            # Assume Column_3 for scores (common pattern)
            value = numbers[0]
            if 'Column_3' in columns:
                where_clause = f"WHERE Column_3 > {value}"
            elif len(columns) >= 3:
                where_clause = f"WHERE {columns[2]} > {value}"
        
        elif numbers and ('less' in question_lower or '<' in question_lower or 'below' in question_lower):
            value = numbers[0]
            if 'Column_3' in columns:
                where_clause = f"WHERE Column_3 < {value}"
            elif len(columns) >= 3:
                where_clause = f"WHERE {columns[2]} < {value}"
        
        sql = f"SELECT * FROM {table_name} {where_clause} LIMIT 1000;"
        
        try:
            result = execute_query(sql)
            
            if len(result['rows']) == 0:
                return create_no_results_response(question, schema)
            
            return {
                'result_type': 'table',
                'answer': format_table_answer(question, len(result['rows'])),
                'columns': result['columns'],
                'rows': result['rows'],
                'sql_query': sql
            }
        except Exception as e:
            return create_helpful_error_response(question, schema, str(e))
    
    # Count query
    elif 'count' in question_lower or 'how many' in question_lower:
        sql = f"SELECT COUNT(*) as count FROM {table_name};"
        result = execute_query(sql)
        value = result['rows'][0][0]
        
        return {
            'result_type': 'value',
            'answer': f"Found **{value}** total records.",
            'value': value,
            'sql_query': sql
        }
    
    # Default: show first 100 rows
    else:
        sql = f"SELECT * FROM {table_name} LIMIT 100;"
        result = execute_query(sql)
        
        return {
            'result_type': 'table',
            'answer': "Showing first 100 records.",
            'columns': result['columns'],
            'rows': result['rows'],
            'sql_query': sql
        }