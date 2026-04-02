"""
AI Processor - Natural Language to SQL Conversion
"""
import ollama
import json
import re
from ..utils.db_utils import execute_query
import random


def process_natural_language_query(question, table_name, schema):
    """
    Convert natural language question to SQL and execute
    ENHANCED: Returns detailed, multi-sentence responses
    """
    
    question_lower = question.lower()
    is_show_query = any(word in question_lower for word in ['show', 'display', 'list', 'find all', 'get all', 'give me', 'all'])
    is_count_only = 'count' in question_lower and not any(word in question_lower for word in ['show', 'display', 'list'])
    
    # Get actual column names
    columns_str = ', '.join(schema['columns'])
    
    # Create mapping of common terms to actual columns
    column_hints = create_column_hints(question_lower, schema['columns'])
    
    # Create enhanced prompt
    if is_show_query and not is_count_only:
        prompt = f"""Convert this question to SQL. Return ONLY the SQL query, nothing else.

Table: {table_name}
Exact columns: {columns_str}

Question: {question}

CRITICAL RULES:
1. Use SELECT * to show all data
2. Column names MUST be EXACTLY from the list above (case-sensitive)
3. For filtering, use WHERE with the exact column name
4. Limit to 1000 rows

{column_hints}

SQL:"""
    else:
        prompt = f"""Convert to SQL. Return ONLY the SQL query.

Table: {table_name}
Exact columns: {columns_str}

Question: {question}

Rules:
1. For "how many" → COUNT(*)
2. For "show/display" → SELECT * WHERE
3. Use EXACT column names from list above
4. Limit SELECT to 1000 rows

{column_hints}

SQL:"""

    try:
        # Try with Ollama first
        response = ollama.chat(
            model='llama3.2:3b',
            messages=[{'role': 'user', 'content': prompt}],
            options={'temperature': 0.1}
        )
        
        sql_query = clean_sql_query(response['message']['content'].strip())
        print(f"Generated SQL: {sql_query}")
        
        # Execute query
        try:
            result = execute_query(sql_query, table_name)
        except Exception as e:
            print(f"Query execution failed: {str(e)}")
            return fallback_query_processing(question, table_name, schema)
        
        # Format response with detailed, conversational answer
        if is_single_value_query(sql_query):
            value = result['rows'][0][0] if result['rows'] else 0
            answer = format_detailed_value_answer(question, value, result['columns'][0] if result['columns'] else 'count', schema)
            
            return {
                'result_type': 'value',
                'answer': answer,
                'value': value,
                'sql_query': sql_query
            }
        else:
            if len(result['rows']) == 0:
                return create_helpful_no_results_response(question, schema, sql_query)
            
            answer = format_detailed_table_answer(question, len(result['rows']), result['columns'], result['rows'], schema)
            
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
        
        return fallback_query_processing(question, table_name, schema)


def format_detailed_value_answer(question, value, column_name, schema):
    """
    Format detailed, conversational answer for single values
    LIKE ChatGPT: Multiple sentences with context and insights
    """
    question_lower = question.lower()
    
    try:
        if isinstance(value, float):
            if value.is_integer():
                value = int(value)
            else:
                value = round(value, 2)
    except:
        pass
    
    responses = []
    
    # Main answer with context
    if 'count' in question_lower or 'how many' in question_lower:
        responses.append(f"Based on your query, I found {value} records that match your criteria.")
        
        if value == 0:
            responses.append("This means there are no records in the dataset that meet the specified conditions.")
            responses.append("You might want to try relaxing your filter criteria or checking if the data exists.")
        elif value == 1:
            responses.append("There is exactly one record that matches what you're looking for.")
        elif value < 10:
            responses.append(f"This is a relatively small subset of your data, representing just {value} records.")
        elif value < 100:
            responses.append(f"This represents a moderate number of records from your dataset.")
        else:
            responses.append(f"This is a substantial number of records. The data has been filtered to show you exactly what you requested.")
    
    elif 'average' in question_lower or 'mean' in question_lower or 'avg' in question_lower:
        responses.append(f"The average value for {column_name.replace('_', ' ')} is **{value}**.")
        responses.append(f"This represents the mean across all records in your dataset.")
        
        if isinstance(value, (int, float)):
            if value > 80:
                responses.append("This is a relatively high average, indicating generally strong performance across the dataset.")
            elif value > 50:
                responses.append("This is a moderate average, suggesting a balanced distribution of values.")
            else:
                responses.append("This is a relatively low average, which might indicate room for improvement.")
    
    elif 'sum' in question_lower or 'total' in question_lower:
        responses.append(f"The total sum is {value:,} when adding up all values.")
        responses.append(f"This represents the complete aggregate of {column_name.replace('_', ' ')} across your entire dataset.")
        
        if isinstance(value, (int, float)) and value > 1000:
            responses.append("This is a substantial total, reflecting significant cumulative activity or value in your data.")
    
    elif 'maximum' in question_lower or 'max' in question_lower or 'highest' in question_lower:
        responses.append(f"The highest value found is {value}.")
        responses.append(f"This represents the maximum {column_name.replace('_', ' ')} in your entire dataset.")
        responses.append("This could be useful for understanding the upper bound or best performance in your data.")
    
    elif 'minimum' in question_lower or 'min' in question_lower or 'lowest' in question_lower:
        responses.append(f"The lowest value found is {value}.")
        responses.append(f"This represents the minimum {column_name.replace('_', ' ')} across all records.")
        responses.append("This helps identify the lower bound or areas that might need attention.")
    
    else:
        responses.append(f"The result of your query is {value}.")
        responses.append("This value has been calculated based on the specific criteria you provided.")
    
    # Add helpful closing
    responses.append("Feel free to ask follow-up questions or request different analyses of your data!")
    
    return " ".join(responses)


def format_detailed_table_answer(question, row_count, columns, rows, schema):
    """
    Format detailed, conversational answer for table results
    LIKE ChatGPT: Multiple sentences with insights
    """
    responses = []
    
    # Opening statement
    if row_count == 1:
        responses.append("I found 1 record that matches your query criteria.")
        responses.append("The complete details for this single record are now displayed in the spreadsheet below.")
    elif row_count >= 1000:
        responses.append(f"Your query returned {row_count:,} records from the database.")
        responses.append("For performance reasons, I'm displaying the first 1,000 records in the spreadsheet below.")
        responses.append("You can refine your search if you need to see specific subsets of this data.")
    elif row_count >= 100:
        responses.append(f"I've successfully retrieved {row_count:,} records that match your criteria.")
        responses.append("All of these records are now displayed in the spreadsheet below for your review.")
        responses.append("This is a substantial dataset that you can scroll through and analyze.")
    elif row_count >= 10:
        responses.append(f"Found {row_count} records that meet your specifications.")
        responses.append("The complete results are shown in the spreadsheet below, where you can examine each record in detail.")
    else:
        responses.append(f"I found {row_count} records matching your query.")
        responses.append("This small result set is perfect for detailed examination.")
    
    # Add column context
    responses.append(f"The dataset includes {len(columns)} columns of information for each record.")
    
    # Quick data insight if possible
    try:
        if rows and len(rows) > 0:
            # Find numeric columns for quick stats
            numeric_col_idx = None
            for idx, col in enumerate(columns):
                if rows[0][idx] is not None and isinstance(rows[0][idx], (int, float)):
                    numeric_col_idx = idx
                    break
            
            if numeric_col_idx is not None:
                col_name = columns[numeric_col_idx]
                values = [row[numeric_col_idx] for row in rows if row[numeric_col_idx] is not None]
                
                if values:
                    avg_val = sum(values) / len(values)
                    max_val = max(values)
                    min_val = min(values)
                    
                    responses.append(f"Quick insight: {col_name.replace('_', ' ')} ranges from {min_val:.1f} to {max_val:.1f} with an average of {avg_val:.1f}.")
    except:
        pass
    
    # Closing with help
    responses.append("You can scroll through the data, ask another question, or ask me to visualize this information with a chart by switching to Chart mode.")
    
    return " ".join(responses)


def create_helpful_no_results_response(question, schema, sql_query):
    """
    Create detailed, helpful response when no results found
    """
    numbers = re.findall(r'\d+', question)
    filter_value = numbers[0] if numbers else None
    
    question_lower = question.lower()
    detected_column = None
    
    # Detect column from question
    if 'age' in question_lower:
        detected_column = 'age'
    elif 'exam' in question_lower or 'score' in question_lower:
        detected_column = 'exam_score'
    elif 'productivity' in question_lower:
        detected_column = 'productivity_score'
    elif 'burnout' in question_lower:
        detected_column = 'burnout_level'
    
    responses = []
    
    responses.append("No records were found that match your specific criteria.")
    
    if filter_value and detected_column:
        if 'less' in question_lower or 'below' in question_lower or '<' in question_lower:
            operator = 'less than'
            responses.append(f"I searched for all records where {detected_column} is {operator} {filter_value}, but no data meets this condition.")
            responses.append(f"💡 **Suggestion**: Try increasing the threshold value. For example, search for '{detected_column} < {int(filter_value) + 10}' to see if there are records just above your current filter.")
        elif 'greater' in question_lower or 'above' in question_lower or '>' in question_lower:
            operator = 'greater than'
            responses.append(f"I searched for records where {detected_column} is {operator} {filter_value}, but none exist in the dataset.")
            responses.append(f"💡 **Suggestion**: Try lowering the threshold. Search for '{detected_column} > {int(filter_value) - 10}' to potentially find matching records.")
        else:
            operator = 'equal to'
            responses.append(f"I looked for records with {detected_column} exactly {operator} {filter_value}, but couldn't find any.")
            responses.append(f"💡 **Suggestion**: Try using a range instead, such as '{detected_column} between {int(filter_value)-2} and {int(filter_value)+2}'.")
    else:
        responses.append("The filters you specified don't match any records in the current dataset.")
    
    responses.append("Here are some things you can try:")
    responses.append("• View all data first: Ask 'Show all records' to see what's available")
    responses.append("• Check data range: Ask 'What is the minimum and maximum {column}?' to understand your data boundaries")
    responses.append("• Relax filters: Try broader criteria or different value ranges")
    
    return {
        'result_type': 'value',
        'answer': " ".join(responses),
        'value': 0,
        'sql_query': sql_query
    }


def create_column_hints(question, columns):
    """Create hints about which columns to use"""
    hints = []
    
    mappings = {
        'exam score': 'exam_score',
        'exam': 'exam_score',
        'score': 'exam_score',
        'marks': 'exam_score',
        'grade': 'exam_score',
        'student': 'student_id',
        'age': 'age',
        'gender': 'gender',
        'study hour': 'study_hours',
        'self study': 'self_study_hours',
        'online class': 'online_classes_hours',
        'social media': 'social_media_hours',
        'gaming': 'gaming_hours',
        'sleep': 'sleep_hours',
        'screen time': 'screen_time_hours',
        'exercise': 'exercise_minutes',
        'caffeine': 'caffeine_intake_mg',
        'job': 'part_time_job',
        'deadline': 'upcoming_deadline',
        'internet': 'internet_quality',
        'mental health': 'mental_health_score',
        'focus': 'focus_index',
        'burnout': 'burnout_level',
        'productivity': 'productivity_score',
    }
    
    for term, col in mappings.items():
        if term in question and col in columns:
            hints.append(f"For '{term}' use column '{col}'")
    
    if hints:
        return "Hints:\n" + "\n".join(hints)
    return ""


def fallback_query_processing(question, table_name, schema):
    """Fallback using pattern matching"""
    question_lower = question.lower()
    columns = schema['columns']
    
    numbers = re.findall(r'\d+', question_lower)
    target_column = None
    
    if 'exam' in question_lower or 'score' in question_lower or 'mark' in question_lower:
        target_column = 'exam_score'
    elif 'age' in question_lower:
        target_column = 'age'
    elif 'study hour' in question_lower:
        target_column = 'study_hours'
    elif 'sleep' in question_lower:
        target_column = 'sleep_hours'
    elif 'productivity' in question_lower:
        target_column = 'productivity_score'
    elif 'burnout' in question_lower:
        target_column = 'burnout_level'
    elif 'focus' in question_lower:
        target_column = 'focus_index'
    
    if target_column and target_column not in columns:
        target_column = None
    
    if any(word in question_lower for word in ['show', 'display', 'list', 'find', 'get', 'all']):
        where_clause = ""
        
        if numbers and target_column:
            value = numbers[0]
            
            if any(word in question_lower for word in ['greater', 'more', 'above', 'higher', '>']):
                where_clause = f"WHERE {target_column} > {value}"
            elif any(word in question_lower for word in ['less', 'below', 'lower', '<']):
                where_clause = f"WHERE {target_column} < {value}"
            elif any(word in question_lower for word in ['equal', 'exactly', '=']):
                where_clause = f"WHERE {target_column} = {value}"
        
        sql = f"SELECT * FROM {table_name} {where_clause} LIMIT 1000;"
        
        try:
            result = execute_query(sql)
            
            if len(result['rows']) == 0:
                return create_helpful_no_results_response(question, schema, sql)
            
            return {
                'result_type': 'table',
                'answer': format_detailed_table_answer(question, len(result['rows']), result['columns'], result['rows'], schema),
                'columns': result['columns'],
                'rows': result['rows'],
                'sql_query': sql
            }
        except Exception as e:
            return {
                'result_type': 'value',
                'answer': f"I encountered an error while processing your query: {str(e)}. Please try rephrasing your question or check the column names.",
                'value': None,
                'sql_query': sql
            }
    
    elif 'count' in question_lower or 'how many' in question_lower:
        where_clause = ""
        
        if numbers and target_column:
            value = numbers[0]
            if 'greater' in question_lower or 'more' in question_lower or 'above' in question_lower:
                where_clause = f"WHERE {target_column} > {value}"
            elif 'less' in question_lower or 'below' in question_lower:
                where_clause = f"WHERE {target_column} < {value}"
        
        sql = f"SELECT COUNT(*) as count FROM {table_name} {where_clause};"
        result = execute_query(sql)
        value = result['rows'][0][0]
        
        if value == 0:
            return create_helpful_no_results_response(question, schema, sql)
        
        return {
            'result_type': 'value',
            'answer': format_detailed_value_answer(question, value, 'count', schema),
            'value': value,
            'sql_query': sql
        }
    
    else:
        sql = f"SELECT * FROM {table_name} LIMIT 100;"
        result = execute_query(sql)
        
        return {
            'result_type': 'table',
            'answer': "I'm showing you the first 100 records from your dataset. This gives you a good overview of the data structure and content. Feel free to ask specific questions or apply filters to narrow down the results!",
            'columns': result['columns'],
            'rows': result['rows'],
            'sql_query': sql
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
        if line_upper.startswith(('SELECT', 'WITH')):
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
    """Check if query returns single value"""
    sql_upper = sql.upper()
    aggregations = ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']
    
    for agg in aggregations:
        if agg in sql_upper:
            select_part = sql_upper.split('FROM')[0]
            if select_part.count(',') == 0:
                return True
    
    return False