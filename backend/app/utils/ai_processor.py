"""
AI Processor - Natural Language to SQL Conversion
"""
import difflib
import json
import os
import re
from urllib import error, request

try:
    import ollama
except ImportError:
    ollama = None

from ..utils.db_utils import execute_query


def process_natural_language_query(question, table_name, schema):
    """
    Convert natural language question to SQL and execute
    """
    
    question_lower = question.lower()
    is_show_query = any(word in question_lower for word in ['show', 'display', 'list', 'find all', 'get all', 'give me', 'all'])
    is_count_only = 'count' in question_lower and not any(word in question_lower for word in ['show', 'display', 'list'])
    
    # Get actual column names
    columns_str = ', '.join(schema['columns'])
    
    # Create mapping of common terms to actual columns
    column_hints = create_column_hints(question_lower, schema['columns'])
    
    direct_sql_query = try_build_direct_sql_query(
        question=question,
        table_name=table_name,
        schema=schema,
        is_show_query=is_show_query,
        is_count_only=is_count_only
    )

    prompt = build_sql_prompt(
        question=question,
        table_name=table_name,
        columns_str=columns_str,
        column_hints=column_hints,
        is_show_query=is_show_query,
        is_count_only=is_count_only
    )

    try:
        if direct_sql_query:
            sql_query = direct_sql_query
        else:
            sql_query = generate_sql_query(prompt)
            sql_query = normalize_sql_for_question(sql_query, question, schema)
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
            response_payload = {
                'result_type': 'value',
                'answer': answer,
                'value': value,
                'sql_query': sql_query
            }

            detail_result = get_detail_result_for_value_query(sql_query, question, table_name)
            if detail_result:
                response_payload['data'] = detail_result

            return response_payload
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


def build_sql_prompt(question, table_name, columns_str, column_hints, is_show_query, is_count_only):
    """Build the SQL generation prompt shared across providers."""
    if is_show_query and not is_count_only:
        return f"""Convert this question to SQL. Return ONLY the SQL query, nothing else.

Table: {table_name}
Exact columns: {columns_str}

Question: {question}

CRITICAL RULES:
1. Use SELECT * to show all data
2. Column names MUST be EXACTLY from the list above (case-sensitive)
3. For filtering, use WHERE with the exact column name
4. Limit to 1000 rows
5. Add ORDER BY the relevant numeric column DESC for top, highest, best, greater than, above, or descending requests
6. Add ORDER BY the relevant numeric column ASC for bottom, lowest, least, less than, below, or ascending requests
7. Add LIMIT N when the question asks for top N or bottom N

{column_hints}

SQL:"""


def try_build_direct_sql_query(question, table_name, schema, is_show_query, is_count_only):
    """Build SQL directly for simple filter and ranking questions."""
    question_lower = question.lower()
    columns = schema.get('columns', [])
    target_column = infer_target_sort_column(question_lower, columns)
    aggregate_function = infer_aggregate_function(question_lower)
    filter_operator = infer_filter_operator(question_lower)
    filter_value = infer_filter_value(question_lower)
    text_filter_operator = infer_text_filter_operator(question_lower)
    text_filter_value = infer_text_filter_value(question_lower)
    contextual_filters = infer_contextual_filters(question_lower, columns)
    top_limit = infer_top_limit(question_lower)
    sort_direction = infer_sort_direction(question_lower)
    ranking_aggregate = aggregate_function in ('MAX', 'MIN') and sort_direction is not None

    if top_limit and sort_direction:
        filter_columns = {column for column, _, _, _ in contextual_filters}
        if target_column in filter_columns:
            fallback_metric = infer_default_ranking_column(columns)
            if fallback_metric:
                target_column = fallback_metric

    if is_show_query and target_column and sort_direction and (top_limit or contextual_filters or ranking_aggregate):
        where_clause = build_where_clause(contextual_filters)
        limit_clause = f" LIMIT {top_limit}" if top_limit else ""
        return (
            f"SELECT * FROM {table_name} "
            f"{where_clause} "
            f"ORDER BY {target_column} {sort_direction}{limit_clause};"
        )

    if not target_column:
        return None

    if aggregate_function:
        aggregate_filters = list(contextual_filters)

        if filter_operator and filter_value is not None:
            aggregate_filters.append((target_column, filter_operator, filter_value, 'numeric'))
        elif text_filter_operator and text_filter_value is not None:
            aggregate_filters.append((target_column, text_filter_operator, text_filter_value, 'text'))

        where_clause = build_where_clause(aggregate_filters)
        alias = aggregate_function.lower()
        aggregate_target = '*' if aggregate_function == 'COUNT' else target_column
        return (
            f"SELECT {aggregate_function}({aggregate_target}) AS {alias} FROM {table_name} "
            f"{where_clause};"
        )

    if is_show_query and filter_operator and filter_value is not None:
        order_direction = 'DESC' if filter_operator in ('>', '>=') else 'ASC'
        return (
            f"SELECT * FROM {table_name} "
            f"WHERE {target_column} {filter_operator} {format_sql_literal(filter_value)} "
            f"ORDER BY {target_column} {order_direction} LIMIT 1000;"
        )

    if is_show_query and text_filter_operator and text_filter_value is not None:
        return (
            f"SELECT * FROM {table_name} "
            f"WHERE LOWER({target_column}) {text_filter_operator} LOWER({format_sql_literal(text_filter_value)}) "
            f"LIMIT 1000;"
        )

    if is_count_only and filter_operator and filter_value is not None:
        return (
            f"SELECT COUNT(*) AS count FROM {table_name} "
            f"WHERE {target_column} {filter_operator} {format_sql_literal(filter_value)};"
        )

    if is_count_only and text_filter_operator and text_filter_value is not None:
        return (
            f"SELECT COUNT(*) AS count FROM {table_name} "
            f"WHERE LOWER({target_column}) {text_filter_operator} LOWER({format_sql_literal(text_filter_value)});"
        )

    if is_show_query and top_limit and sort_direction:
        where_clause = build_where_clause(contextual_filters)
        return (
            f"SELECT * FROM {table_name} "
            f"{where_clause} "
            f"ORDER BY {target_column} {sort_direction} LIMIT {top_limit};"
        )

    return None
    return f"""Convert to SQL. Return ONLY the SQL query.

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


def generate_sql_query(prompt):
    """Generate SQL using a cloud provider first when configured, then local Ollama."""
    use_cloud_api = should_use_cloud_api()
    attempts = ['cloud', 'ollama'] if use_cloud_api else ['ollama', 'cloud']

    errors = []
    for attempt in attempts:
        try:
            if attempt == 'cloud':
                content = generate_sql_via_cloud(prompt)
            else:
                content = generate_sql_via_ollama(prompt)
            return clean_sql_query(content.strip())
        except Exception as exc:
            errors.append(f"{attempt}: {exc}")

    raise RuntimeError(" | ".join(errors))


def should_use_cloud_api():
    """Prefer cloud mode when explicitly enabled or when cloud credentials are present."""
    if os.getenv('USE_CLOUD_API', 'false').lower() == 'true':
        return True

    return any([
        os.getenv('GROQ_API_KEY'),
        os.getenv('OPENAI_API_KEY'),
        os.getenv('OPENROUTER_API_KEY')
    ])


def generate_sql_via_cloud(prompt):
    """Call an OpenAI-compatible provider such as Groq."""
    provider = os.getenv('API_PROVIDER', 'groq').lower()

    if provider == 'groq':
        api_key = os.getenv('GROQ_API_KEY')
        model = os.getenv('GROQ_MODEL', 'llama-3.1-8b-instant')
        base_url = os.getenv('GROQ_BASE_URL', 'https://api.groq.com/openai/v1/chat/completions')
    elif provider == 'openai':
        api_key = os.getenv('OPENAI_API_KEY')
        model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
        base_url = os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1/chat/completions')
    elif provider == 'openrouter':
        api_key = os.getenv('OPENROUTER_API_KEY')
        model = os.getenv('OPENROUTER_MODEL', 'meta-llama/llama-3.1-8b-instruct')
        base_url = os.getenv('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1/chat/completions')
    else:
        raise RuntimeError(f"Unsupported API_PROVIDER: {provider}")

    if not api_key:
        raise RuntimeError(f"Missing API key for provider '{provider}'")

    payload = {
        'model': model,
        'messages': [
            {
                'role': 'system',
                'content': 'You translate spreadsheet questions into SQLite SQL. Return only executable SQL with no explanation.'
            },
            {'role': 'user', 'content': prompt}
        ],
        'temperature': 0.1
    }

    req = request.Request(
        base_url,
        data=json.dumps(payload).encode('utf-8'),
        headers={
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        },
        method='POST'
    )

    try:
        with request.urlopen(req, timeout=45) as response:
            body = json.loads(response.read().decode('utf-8'))
    except error.HTTPError as exc:
        details = exc.read().decode('utf-8', errors='ignore')
        raise RuntimeError(f"HTTP {exc.code}: {details}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Network error: {exc.reason}") from exc

    try:
        return body['choices'][0]['message']['content']
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"Unexpected cloud response: {body}") from exc


def generate_sql_via_ollama(prompt):
    """Call the local Ollama service when available."""
    if ollama is None:
        raise RuntimeError("ollama package is not installed")

    model = os.getenv('OLLAMA_MODEL', 'llama3.2:3b')
    response = ollama.chat(
        model=model,
        messages=[{'role': 'user', 'content': prompt}],
        options={'temperature': 0.1}
    )
    return response['message']['content']


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
        return format_count_answer(question, value)
    
    elif 'average' in question_lower or 'mean' in question_lower or 'avg' in question_lower:
        responses.append(f"The average value for {column_name.replace('_', ' ')} is {value}.")
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
    
    return " ".join(responses)


def format_count_answer(question, value):
    """Turn count questions into a direct natural-language answer."""
    question_lower = question.lower().strip().rstrip('?.!')

    public_universities_match = re.search(
        r'(?:count|number of|how many)\s+universit(?:y|ies)\b.*?\binstitution type\s+is\s+([a-z\s-]+)',
        question_lower
    )
    if public_universities_match:
        institution_type = public_universities_match.group(1).strip()
        return f"The count of universities that are {institution_type} is {value}."

    count_of_match = re.search(r'(?:count|number of)\s+(.+)', question_lower)
    if count_of_match:
        subject = re.sub(r'^of\s+', '', count_of_match.group(1).strip())
        return f"The count of {subject} is {value}."

    how_many_match = re.search(r'how many\s+(.+)', question_lower)
    if how_many_match:
        subject = how_many_match.group(1).strip()
        return f"The number of {subject} is {value}."

    return f"The count is {value}."


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
        'employment rate': 'employment_rate',
        'employment': 'employment_rate',
        'placement rate': 'employment_rate',
        'founded year': 'founded_year',
        'founded': 'founded_year',
        'established year': 'founded_year',
        'year founded': 'founded_year',
        'discount percentage': 'discount_percentage',
        'discount percent': 'discount_percentage',
        'discount': 'discount_percentage',
        'product': 'product',
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
    elif 'founded' in question_lower or 'established' in question_lower:
        target_column = find_matching_column(
            ['founded_year', 'year_founded', 'established_year', 'foundation_year'],
            columns
        )
    elif 'focus' in question_lower:
        target_column = 'focus_index'
    elif 'employment' in question_lower or 'placement' in question_lower:
        target_column = 'employment_rate'
    elif 'discount' in question_lower:
        target_column = find_matching_column(
            ['discount_percentage', 'discount_percent', 'discount', 'percentage_discount'],
            columns
        )
    
    if target_column and target_column not in columns:
        target_column = None
    
    if any(word in question_lower for word in ['show', 'display', 'list', 'find', 'get', 'all']):
        where_clause = ""
        order_clause = ""
        limit_clause = "LIMIT 1000"
        
        if numbers and target_column:
            value = numbers[0]
            
            if any(word in question_lower for word in ['greater', 'more', 'above', 'higher', '>']):
                where_clause = f"WHERE {target_column} > {value}"
                order_clause = f"ORDER BY {target_column} DESC"
            elif any(word in question_lower for word in ['less', 'below', 'lower', '<']):
                where_clause = f"WHERE {target_column} < {value}"
                order_clause = f"ORDER BY {target_column} ASC"
            elif any(word in question_lower for word in ['equal', 'exactly', '=']):
                where_clause = f"WHERE {target_column} = {value}"

        if 'top' in question_lower and target_column:
            order_clause = f"ORDER BY {target_column} DESC"
            top_limit = infer_top_limit(question_lower)
            if top_limit:
                limit_clause = f"LIMIT {top_limit}"
        elif 'bottom' in question_lower and target_column:
            order_clause = f"ORDER BY {target_column} ASC"
            bottom_limit = infer_top_limit(question_lower)
            if bottom_limit:
                limit_clause = f"LIMIT {bottom_limit}"
        
        sql = f"SELECT * FROM {table_name} {where_clause} {order_clause} {limit_clause};"
        
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


def get_detail_result_for_value_query(sql_query, question, table_name):
    """Return matching rows for count queries so the spreadsheet can still update."""
    question_lower = question.lower()
    sql_upper = sql_query.upper()

    if 'COUNT(' not in sql_upper:
        return None

    if not any(term in question_lower for term in ['count', 'how many', 'number of']):
        return None

    detail_sql = build_detail_query_from_count_sql(sql_query, table_name)
    if not detail_sql:
        return None

    try:
        return execute_query(detail_sql, table_name)
    except Exception:
        return None


def build_detail_query_from_count_sql(sql_query, table_name):
    """Convert a COUNT query into a bounded SELECT * query that preserves filters."""
    normalized_sql = sql_query.strip().rstrip(';')
    pattern = re.compile(
        rf'^\s*SELECT\s+COUNT\s*\(\s*(?:\*|[^)]+)\s*\)\s+AS\s+\w+\s+FROM\s+{re.escape(table_name)}\b',
        re.IGNORECASE
    )
    detail_sql = pattern.sub(f"SELECT * FROM {table_name}", normalized_sql, count=1)

    if detail_sql == normalized_sql:
        return None

    if 'LIMIT' not in detail_sql.upper():
        detail_sql = f"{detail_sql} LIMIT 1000"

    return f"{detail_sql};"


def normalize_sql_for_question(sql, question, schema):
    """Make sort-sensitive queries deterministic when the model omits ordering."""
    sql_upper = sql.upper()
    if 'SELECT' not in sql_upper or 'COUNT(' in sql_upper:
        return sql

    question_lower = question.lower()
    target_column = infer_target_sort_column(question_lower, schema.get('columns', []))
    sort_direction = infer_sort_direction(question_lower)

    normalized_sql = sql.rstrip().rstrip(';')
    if should_force_full_row_selection(question_lower):
        normalized_sql = force_select_all(normalized_sql)

    if not target_column or not sort_direction:
        return f"{normalized_sql};"

    if 'ORDER BY' not in sql_upper:
        limit_match = re.search(r'\sLIMIT\s+\d+\s*$', normalized_sql, re.IGNORECASE)
        order_clause = f" ORDER BY {target_column} {sort_direction}"
        if limit_match:
            normalized_sql = (
                f"{normalized_sql[:limit_match.start()]}"
                f"{order_clause}"
                f"{normalized_sql[limit_match.start():]}"
            )
        else:
            normalized_sql = f"{normalized_sql}{order_clause}"

    limit_value = infer_top_limit(question_lower)
    if limit_value:
        if 'LIMIT' in sql_upper:
            normalized_sql = re.sub(r'\sLIMIT\s+\d+\s*$', f" LIMIT {limit_value}", normalized_sql, flags=re.IGNORECASE)
        else:
            normalized_sql = f"{normalized_sql} LIMIT {limit_value}"
    elif 'LIMIT' not in sql_upper and should_apply_default_limit(question_lower):
        normalized_sql = f"{normalized_sql} LIMIT 1000"

    return f"{normalized_sql};"


def infer_target_sort_column(question_lower, columns):
    """Infer the most likely metric column mentioned in the question."""
    phrase_mappings = {
        'employment rate': 'employment_rate',
        'employment': 'employment_rate',
        'placement rate': 'employment_rate',
        'founded year': 'founded_year',
        'founded': 'founded_year',
        'established year': 'founded_year',
        'year founded': 'founded_year',
        'gender': 'gender',
        'exam score': 'exam_score',
        'score': 'exam_score',
        'marks': 'exam_score',
        'grade': 'exam_score',
        'study hour': 'study_hours',
        'sleep': 'sleep_hours',
        'productivity': 'productivity_score',
        'burnout': 'burnout_level',
        'focus': 'focus_index',
        'gpa': 'gpa',
        'rating': 'rating',
        'salary': 'salary',
        'revenue': 'revenue',
        'na sales': 'na_sales',
        'north america sales': 'na_sales',
        'eu sales': 'eu_sales',
        'europe sales': 'eu_sales',
        'jp sales': 'jp_sales',
        'japan sales': 'jp_sales',
        'other sales': 'other_sales',
        'discount percentage': 'discount_percentage',
        'discount percent': 'discount_percentage',
        'discount': 'discount_percentage',
        'global sales': 'global_sales',
        'sales': 'global_sales',
        'total sales': 'global_sales',
    }

    columns_lower = {column.lower(): column for column in columns}
    for phrase, expected_column in phrase_mappings.items():
        if phrase in question_lower:
            matched_column = find_matching_column([expected_column], columns)
            if matched_column:
                return matched_column

    question_tokens = tokenize_identifier(question_lower)
    ranking_words = {
        'top', 'bottom', 'highest', 'lowest', 'best', 'worst',
        'greater', 'less', 'above', 'below', 'desc', 'descending',
        'asc', 'ascending', 'display', 'show', 'where', 'than'
    }
    best_match = None
    best_score = 0

    for column in columns:
        column_tokens = tokenize_identifier(column)
        score = len((question_tokens & column_tokens) - ranking_words)
        if score > best_score:
            best_score = score
            best_match = column

    if best_match:
        return best_match

    default_metric = infer_default_ranking_column(columns)
    if default_metric and any(term in question_lower for term in ['top', 'bottom', 'highest', 'lowest', 'best', 'worst']):
        return default_metric

    phrase_guess = fuzzy_match_phrase_to_column(question_lower, columns)
    if phrase_guess:
        return phrase_guess

    return None


def infer_sort_direction(question_lower):
    """Determine whether a question implies ascending or descending order."""
    desc_terms = ['top', 'highest', 'best', 'greater', 'more', 'above', 'desc', 'descending']
    asc_terms = ['bottom', 'lowest', 'least', 'less', 'below', 'asc', 'ascending']

    if any(term in question_lower for term in desc_terms):
        return 'DESC'
    if any(term in question_lower for term in asc_terms):
        return 'ASC'
    return None


def infer_top_limit(question_lower):
    """Read explicit sizes such as 'top 5' or 'bottom 10'."""
    match = re.search(r'\b(?:top|bottom)\s+(\d+)\b', question_lower)
    if match:
        return int(match.group(1))
    return None


def infer_aggregate_function(question_lower):
    """Infer an aggregate function from the question text."""
    if any(term in question_lower for term in ['average', 'avg', 'mean']):
        return 'AVG'
    if any(term in question_lower for term in ['sum', 'total']):
        return 'SUM'
    if any(term in question_lower for term in ['maximum', 'max', 'highest']):
        return 'MAX'
    if any(term in question_lower for term in ['minimum', 'min', 'lowest']):
        return 'MIN'
    if any(term in question_lower for term in ['count', 'how many', 'number of']):
        return 'COUNT'
    return None


def infer_contextual_filters(question_lower, columns):
    """Infer supporting filters such as year constraints from the question."""
    filters = []
    year_column = find_matching_column(['year', 'release_year', 'founded_year', 'year_founded'], columns)
    year_regex = r'((?:1[0-9]{3}|20[0-9]{2}))'

    founded_year_column = find_matching_column(
        ['founded_year', 'year_founded', 'established_year', 'foundation_year'],
        columns
    )
    if founded_year_column:
        founded_matchers = [
            (rf'\b(?:founded|established)\s+before\s+{year_regex}\b', '<'),
            (rf'\b(?:founded|established)\s+after\s+{year_regex}\b', '>'),
            (rf'\b(?:founded|established)\s+(?:in|during|for|from)\s+{year_regex}\b', '='),
            (rf'\b(?:before)\s+{year_regex}\b', '<'),
            (rf'\b(?:after)\s+{year_regex}\b', '>'),
        ]
        for pattern, operator in founded_matchers:
            match = re.search(pattern, question_lower)
            if match:
                filters.append((founded_year_column, operator, int(match.group(1)), 'numeric'))
                break

    if year_column:
        year_patterns = [
            (rf'\b(?:in|for|from)\s+(?:the\s+year\s+)?{year_regex}\b', '='),
            (rf'\b(?:before|earlier than|prior to)\s+{year_regex}\b', '<'),
            (rf'\b(?:after|later than|since)\s+{year_regex}\b', '>'),
        ]
        filtered_columns = {column for column, _, _, _ in filters}
        if year_column not in filtered_columns:
            for pattern, operator in year_patterns:
                year_match = re.search(pattern, question_lower)
                if year_match:
                    filters.append((year_column, operator, int(year_match.group(1)), 'numeric'))
                    break

    text_context_mappings = [
        (
            find_matching_column(['academic_level', 'education_level', 'student_level'], columns),
            {
                'high school': 'High School',
                'college': 'College',
                'undergraduate': 'Undergraduate',
                'graduate': 'Graduate',
            }
        ),
        (
            find_matching_column(['institution_type', 'type', 'school_type'], columns),
            {
                'public': 'Public',
                'private': 'Private',
            }
        ),
    ]

    for column_name, value_mappings in text_context_mappings:
        if not column_name:
            continue
        if any(existing_column == column_name for existing_column, _, _, _ in filters):
            continue

        for phrase, value in value_mappings.items():
            patterns = [
                rf'\bin\s+{re.escape(phrase)}\b',
                rf'\bwhere\s+{re.escape(column_name.lower().replace("_", " "))}\s+is\s+{re.escape(phrase)}\b',
                rf'\bwith\s+{re.escape(phrase)}\b',
            ]
            if any(re.search(pattern, question_lower) for pattern in patterns):
                filters.append((column_name, '=', value, 'text'))
                break

    return filters


def build_where_clause(filters):
    """Build a WHERE clause from inferred filters."""
    if not filters:
        return ''

    parts = []
    for column, operator, value, value_type in filters:
        if value_type == 'text':
            parts.append(f"LOWER({column}) {operator} LOWER({format_sql_literal(value)})")
        else:
            parts.append(f"{column} {operator} {format_sql_literal(value)}")
    return f"WHERE {' AND '.join(parts)}"


def infer_default_ranking_column(columns):
    """Pick a sensible default metric column for top/bottom questions."""
    preferred_columns = [
        'global_sales',
        'total_sales',
        'sales',
        'revenue',
        'profit',
        'score',
        'rating',
        'gpa',
        'employment_rate'
    ]
    return find_matching_column(preferred_columns, columns)


def infer_filter_operator(question_lower):
    """Infer a comparison operator from the question text."""
    if any(term in question_lower for term in ['greater than or equal to', 'at least', 'not less than', '>=']):
        return '>='
    if any(term in question_lower for term in ['less than or equal to', 'at most', 'not more than', '<=']):
        return '<='
    if any(term in question_lower for term in ['greater than', 'more than', 'above', 'higher than', '>']):
        return '>'
    if any(term in question_lower for term in ['less than', 'below', 'lower than', '<']):
        return '<'
    if any(term in question_lower for term in ['equal to', 'equals', 'exactly', '=']):
        return '='
    return None


def infer_filter_value(question_lower):
    """Extract the first numeric value used in a comparison query."""
    match = re.search(r'-?\d+(?:\.\d+)?', question_lower)
    if not match:
        return None
    value_text = match.group(0)
    return float(value_text) if '.' in value_text else int(value_text)


def infer_text_filter_operator(question_lower):
    """Infer equality or inequality for simple text filters."""
    if any(term in question_lower for term in [' is not ', ' are not ', ' not equal to ', ' != ']):
        return '!='
    if any(term in question_lower for term in [' is ', ' are ', ' equals ', ' equal to ', '=']):
        return '='
    return None


def infer_text_filter_value(question_lower):
    """Extract simple text equality filters such as 'gender is female'."""
    match = re.search(
        r'\b(?:is not|are not|not equal to|is|are|equals?|equal to|!=|=)\s+([a-z][a-z0-9_\-\s]*)',
        question_lower
    )
    if not match:
        return None

    value = match.group(1).strip()
    value = re.split(
        r'\b(?:order by|sorted by|limit|top|bottom|highest|lowest|greater|less|above|below)\b',
        value,
        maxsplit=1
    )[0].strip()

    if not value:
        return None

    return value


def format_sql_literal(value):
    """Format a Python value as a SQL literal."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def should_apply_default_limit(question_lower):
    """Keep explicit ranking queries bounded when the model omits a limit."""
    return any(term in question_lower for term in ['top', 'bottom', 'highest', 'lowest'])


def tokenize_identifier(value):
    """Split free text and snake_case identifiers into comparable tokens."""
    return set(token for token in re.split(r'[^a-z0-9]+', value.lower()) if token)


def should_force_full_row_selection(question_lower):
    """Return full rows for display and ranking requests instead of narrow projections."""
    return any(term in question_lower for term in [
        'show', 'display', 'list', 'give me', 'find', 'get',
        'top', 'bottom', 'highest', 'lowest', 'greater', 'above', 'less', 'below'
    ])


def force_select_all(sql):
    """Replace a narrow SELECT list with SELECT * while preserving the rest of the query."""
    if re.search(r'^\s*SELECT\s+\*\s+FROM\b', sql, re.IGNORECASE):
        return sql
    return re.sub(r'^\s*SELECT\s+.+?\s+FROM\b', 'SELECT * FROM', sql, count=1, flags=re.IGNORECASE | re.DOTALL)


def find_matching_column(candidates, columns):
    """Find the first exact or close schema match for one of the candidate names."""
    columns_lower = {column.lower(): column for column in columns}
    for candidate in candidates:
        if candidate.lower() in columns_lower:
            return columns_lower[candidate.lower()]

    close_match = difflib.get_close_matches(
        candidates[0].lower(),
        list(columns_lower.keys()),
        n=1,
        cutoff=0.75
    )
    if close_match:
        return columns_lower[close_match[0]]

    return None


def fuzzy_match_phrase_to_column(question_lower, columns):
    """Use fuzzy token matching so minor typos still find the right metric column."""
    cleaned_question = " ".join(tokenize_identifier(question_lower))
    best_column = None
    best_score = 0.0

    for column in columns:
        column_phrase = " ".join(tokenize_identifier(column))
        if not column_phrase:
            continue
        score = difflib.SequenceMatcher(None, cleaned_question, column_phrase).ratio()
        if score > best_score:
            best_score = score
            best_column = column

    if best_score >= 0.45:
        return best_column
    return None


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
