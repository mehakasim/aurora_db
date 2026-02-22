"""
AI Processor - Natural Language to SQL Conversion
IMPROVED: Better query type detection and SQL generation
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
    
    # Check if this is definitely a "show/display" query
    question_lower = question.lower()
    is_show_query = any(word in question_lower for word in ['show', 'display', 'list', 'find all', 'get all', 'give me'])
    is_count_only = 'count' in question_lower and 'show' not in question_lower and 'display' not in question_lower
    
    # Create enhanced prompt for Llama
    if is_show_query and not is_count_only:
        # Force SELECT * for show queries
        prompt = f"""You are a SQL expert. Convert this natural language question to a SQL SELECT query.

Table: {table_name}
Columns: {', '.join(schema['columns'])}

Question: {question}

IMPORTANT RULES:
1. This is a DISPLAY/SHOW query - use SELECT * or SELECT columns
2. DO NOT use COUNT(*) - the user wants to see the actual rows
3. Include WHERE clause for any filtering conditions
4. Return only the SQL query, no explanations
5. Limit to 1000 rows maximum
6. Use exact column names from the list above

Example:
Question: "Show me students where age is less than 18"
SQL: SELECT * FROM {table_name} WHERE age < 18 LIMIT 1000;

Now generate SQL for: {question}

SQL:"""
    else:
        prompt = f"""You are a SQL expert. Convert the natural language question to a SQL query.

Table: {table_name}
Columns: {', '.join(schema['columns'])}
Column Types: {dict(zip(schema['columns'], schema['types']))}

Question: {question}

Rules:
1. Return ONLY valid SQLite SQL query
2. For "how many" or "count" queries → Use COUNT(*)
3. For "show", "display", "list" queries → Use SELECT * with WHERE
4. For aggregations (sum, average) → Use appropriate function
5. Table name is exactly: {table_name}
6. Use exact column names from the list above
7. Limit SELECT queries to 1000 rows
8. Always end with semicolon

Return ONLY the SQL query, nothing else.

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
                'temperature': 0.1,  # Lower temperature for more consistent SQL
            }
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
            answer = format_single_value_answer(question, value, result['columns'][0] if result['columns'] else 'count')
            
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
        import traceback
        traceback.print_exc()
        
        # Fallback: Use pattern matching
        return fallback_query_processing(question, table_name, schema)


def clean_sql_query(sql):
    """Clean SQL query from AI response"""
    # Remove markdown code blocks
    sql = re.sub(r'```sql\n?', '', sql)
    sql = re.sub(r'```\n?', '', sql)
    sql = re.sub(r'`', '', sql)
    
    # Remove any explanatory text before SELECT/WITH
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
    
    # Remove any text after semicolon
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
    
    # Check for aggregation functions without other SELECT columns
    aggregations = ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']
    
    for agg in aggregations:
        if agg in sql_upper:
            # Check if it's the only thing being selected
            select_part = sql_upper.split('FROM')[0]
            # Count commas in SELECT clause (excluding within parentheses)
            if select_part.count(',') == 0:
                return True
    
    return False


def format_single_value_answer(question, value, column_name):
    """Format natural language answer for single value"""
    question_lower = question.lower()
    
    # Try to make the value more readable
    try:
        if isinstance(value, float):
            if value.is_integer():
                value = int(value)
            else:
                value = round(value, 2)
    except:
        pass
    
    if 'count' in question_lower or 'how many' in question_lower:
        return f"Found **{value}** records matching your criteria."
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
    Used when Ollama fails or for simple queries
    """
    question_lower = question.lower()
    
    # Extract column names and conditions using simple patterns
    columns = schema['columns']
    
    # Pattern: "show/display ... where COLUMN OPERATOR VALUE"
    if any(word in question_lower for word in ['show', 'display', 'list', 'find', 'get']):
        # Try to find WHERE conditions
        where_clause = ""
        
        # Look for simple conditions
        for col in columns:
            col_lower = col.lower()
            if col_lower in question_lower:
                # Check for common operators
                if f'{col_lower} >' in question_lower or f'{col_lower} is greater than' in question_lower:
                    # Extract value
                    parts = question_lower.split(col_lower)
                    if len(parts) > 1:
                        value_part = parts[1].replace('>', '').replace('is greater than', '').strip()
                        value = value_part.split()[0] if value_part.split() else None
                        if value:
                            try:
                                value = float(value)
                                where_clause = f"WHERE {col} > {value}"
                            except:
                                where_clause = f"WHERE {col} > '{value}'"
                
                elif f'{col_lower} <' in question_lower or f'{col_lower} is less than' in question_lower:
                    parts = question_lower.split(col_lower)
                    if len(parts) > 1:
                        value_part = parts[1].replace('<', '').replace('is less than', '').strip()
                        value = value_part.split()[0] if value_part.split() else None
                        if value:
                            try:
                                value = float(value)
                                where_clause = f"WHERE {col} < {value}"
                            except:
                                where_clause = f"WHERE {col} < '{value}'"
                
                elif f'{col_lower} =' in question_lower or f'{col_lower} equals' in question_lower or f'{col_lower} is' in question_lower:
                    parts = question_lower.split(col_lower)
                    if len(parts) > 1:
                        value_part = parts[1].replace('=', '').replace('equals', '').replace('is', '').strip()
                        value = value_part.split()[0] if value_part.split() else None
                        if value:
                            where_clause = f"WHERE {col} = '{value}'"
        
        sql = f"SELECT * FROM {table_name} {where_clause} LIMIT 1000;"
        result = execute_query(sql)
        
        return {
            'result_type': 'table',
            'answer': format_table_answer(question, len(result['rows'])),
            'columns': result['columns'],
            'rows': result['rows'],
            'sql_query': sql
        }
    
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