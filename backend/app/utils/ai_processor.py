"""
AI Processor - Natural Language to SQL Conversion
IMPROVED: Fully generic, schema-aware, handles metadata questions
"""
import ollama
import re
from ..utils.db_utils import execute_query


# ---------------------------------------------------------------------------
# Intent detection
# ---------------------------------------------------------------------------

def detect_intent(question: str, schema: dict) -> str:
    """
    Classify the question before touching SQL.
    Returns one of: 'schema', 'count', 'aggregate', 'show'
    """
    q = question.lower()

    schema_keywords = [
        'what are the columns', 'list columns', 'show columns',
        'what columns', 'column names', 'fields', 'what data',
        'what information', 'structure', 'schema', 'headers',
        'what is in', "what's in", 'describe',
    ]
    if any(kw in q for kw in schema_keywords):
        return 'schema'

    if re.search(r'\bhow many\b|\bcount\b|\btotal number\b', q) and \
       not any(w in q for w in ['show', 'display', 'list', 'find', 'get']):
        return 'count'

    agg_keywords = ['average', 'avg', 'mean', 'sum', 'total', 'maximum',
                    'minimum', 'max', 'min', 'highest', 'lowest']
    if any(kw in q for kw in agg_keywords):
        return 'aggregate'

    return 'show'


# ---------------------------------------------------------------------------
# Schema question handler — no SQL needed
# ---------------------------------------------------------------------------

def handle_schema_question(schema: dict) -> dict:
    cols = schema['columns']
    sample = schema.get('sample_values', {})  # optional: {col: [val, val, ...]}

    lines = []
    for col in cols:
        if sample and col in sample:
            examples = ', '.join(str(v) for v in sample[col][:3])
            lines.append(f"  • {col} — e.g. {examples}")
        else:
            lines.append(f"  • {col}")

    answer = f"Your table has {len(cols)} column(s):\n" + '\n'.join(lines)

    return {
        'result_type': 'value',
        'answer': answer,
        'value': None,
        'sql_query': None,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process_natural_language_query(question: str, table_name: str, schema: dict) -> dict:
    """
    Convert a natural language question to SQL and execute it.
    Handles metadata questions without touching the DB.
    """
    intent = detect_intent(question, schema)

    # Answer schema questions immediately — no SQL required
    if intent == 'schema':
        return handle_schema_question(schema)

    # Build a fully generic prompt — no hardcoded column assumptions
    prompt = build_prompt(question, table_name, schema, intent)

    try:
        response = ollama.chat(
            model='llama3.2:3b',
            messages=[{'role': 'user', 'content': prompt}],
            options={'temperature': 0.1},
        )
        sql_query = clean_sql_query(response['message']['content'].strip())
        print(f"Generated SQL: {sql_query}")

        try:
            result = execute_query(sql_query, table_name)
        except Exception as e:
            print(f"Query failed: {e}")
            return create_helpful_error_response(question, schema, str(e))

        return format_result(question, sql_query, result, schema, intent)

    except Exception as e:
        print(f"AI processing error: {e}")
        import traceback; traceback.print_exc()
        return fallback_query_processing(question, table_name, schema, intent)


# ---------------------------------------------------------------------------
# Generic prompt builder
# ---------------------------------------------------------------------------

def build_prompt(question: str, table_name: str, schema: dict, intent: str) -> str:
    cols = schema['columns']
    col_list = ', '.join(f'"{c}"' for c in cols)

    # Provide sample values if available so the LLM can pick the right column
    sample_hints = ''
    if schema.get('sample_values'):
        rows = []
        for col, vals in schema['sample_values'].items():
            rows.append(f'  "{col}": {vals[:3]}')
        sample_hints = 'Sample values per column:\n' + '\n'.join(rows) + '\n\n'

    if intent in ('show',):
        instruction = (
            "This is a SHOW/FILTER query. Use SELECT * with appropriate WHERE clause.\n"
            "Do NOT use COUNT(*). Return matching rows.\n"
            "Add LIMIT 1000."
        )
    elif intent == 'count':
        instruction = "This is a COUNT query. Use SELECT COUNT(*) ... or COUNT with a filter."
    elif intent == 'aggregate':
        instruction = (
            "This is an AGGREGATE query. Use SUM, AVG, MAX, MIN etc. as appropriate.\n"
            "Pick the most relevant numeric column based on the question."
        )
    else:
        instruction = "Generate the most appropriate SQL query."

    return f"""You are a SQL expert working with SQLite.

Table name: {table_name}
Columns (use EXACTLY as written, including case): {col_list}

{sample_hints}Task: {instruction}

RULES:
1. Use ONLY columns from the list above — do not invent column names.
2. If the question references a concept (e.g. "salary", "age") look for the closest matching column by name.
3. String comparisons: use LIKE for partial matches, = for exact.
4. Return ONLY the raw SQL query — no explanation, no markdown fences.
5. End with a semicolon.

Question: {question}

SQL:"""


# ---------------------------------------------------------------------------
# Result formatting
# ---------------------------------------------------------------------------

def format_result(question: str, sql_query: str, result: dict, schema: dict, intent: str) -> dict:
    if is_single_value_query(sql_query):
        value = result['rows'][0][0] if result['rows'] else 0
        col_name = result['columns'][0] if result['columns'] else 'result'
        return {
            'result_type': 'value',
            'answer': format_single_value_answer(question, value, col_name),
            'value': value,
            'sql_query': sql_query,
        }

    if not result['rows']:
        return create_no_results_response(question, schema)

    return {
        'result_type': 'table',
        'answer': format_table_answer(question, len(result['rows'])),
        'columns': result['columns'],
        'rows': result['rows'],
        'sql_query': sql_query,
    }


# ---------------------------------------------------------------------------
# Fallback — simple pattern matching, no column assumptions
# ---------------------------------------------------------------------------

def fallback_query_processing(question: str, table_name: str, schema: dict, intent: str) -> dict:
    q = question.lower()
    cols = schema['columns']

    # Numeric filter heuristic: find numeric columns from sample values
    numeric_cols = [
        c for c in cols
        if schema.get('sample_values') and
        all(isinstance(v, (int, float)) for v in schema['sample_values'].get(c, []))
    ]
    filter_col = numeric_cols[0] if numeric_cols else (cols[0] if cols else 'id')

    numbers = re.findall(r'\d+\.?\d*', q)
    where = ''
    if numbers:
        val = numbers[0]
        if any(w in q for w in ['greater', 'above', 'more', 'over', '>']):
            where = f'WHERE "{filter_col}" > {val}'
        elif any(w in q for w in ['less', 'below', 'under', '<']):
            where = f'WHERE "{filter_col}" < {val}'

    if intent in ('show',):
        sql = f'SELECT * FROM {table_name} {where} LIMIT 1000;'
    elif intent == 'count':
        sql = f'SELECT COUNT(*) as count FROM {table_name} {where};'
    else:
        sql = f'SELECT * FROM {table_name} LIMIT 100;'

    try:
        result = execute_query(sql, table_name)
        return format_result(question, sql, result, schema, intent)
    except Exception as e:
        return create_helpful_error_response(question, schema, str(e))


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def clean_sql_query(sql: str) -> str:
    sql = re.sub(r'```sql\n?', '', sql)
    sql = re.sub(r'```\n?', '', sql)
    sql = sql.replace('`', '')

    # Extract from first SQL keyword
    for line in sql.split('\n'):
        if line.strip().upper().startswith(('SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE')):
            idx = sql.upper().find(line.strip().upper())
            sql = sql[idx:]
            break

    # Keep only up to first semicolon
    if ';' in sql:
        sql = sql.split(';')[0] + ';'

    return sql.strip() if sql.strip().endswith(';') else sql.strip() + ';'


def is_single_value_query(sql: str) -> bool:
    upper = sql.upper()
    if 'FROM' not in upper:
        return False
    select_part = upper.split('FROM')[0]
    return (
        any(agg in select_part for agg in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN('])
        and select_part.count(',') == 0
    )


def format_single_value_answer(question: str, value, col_name: str) -> str:
    q = question.lower()
    try:
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        elif isinstance(value, float):
            value = round(value, 2)
    except Exception:
        pass

    if any(w in q for w in ['how many', 'count']):
        return f"There are {value} records matching your criteria."
    elif any(w in q for w in ['sum', 'total']):
        return f"The total is {value}."
    elif any(w in q for w in ['average', 'mean', 'avg']):
        return f"The average is {value}."
    elif any(w in q for w in ['maximum', 'max', 'highest']):
        return f"The maximum is {value}."
    elif any(w in q for w in ['minimum', 'min', 'lowest']):
        return f"The minimum is {value}."
    else:
        return f"Result: {value}."


def format_table_answer(question: str, row_count: int) -> str:
    if row_count == 0:
        return "No records found matching your criteria."
    elif row_count == 1:
        return "Found 1 record matching your query."
    elif row_count >= 1000:
        return f"Found {row_count}+ records (showing first 1000)."
    else:
        return f"Found {row_count} records matching your query."


def create_helpful_error_response(question: str, schema: dict, error: str) -> dict:
    col_list = ', '.join(f'**{c}**' for c in schema['columns'])
    return {
        'result_type': 'value',
        'answer': (
            f"I couldn't run that query. Available columns are: {col_list}.\n"
            f"Try rephrasing using exact column names, e.g. *\"Show rows where Salary > 50000\"*."
        ),
        'value': None,
        'sql_query': None,
    }


def create_no_results_response(question: str, schema: dict) -> dict:
    col_list = ', '.join(f'**{c}**' for c in schema['columns'])
    return {
        'result_type': 'value',
        'answer': (
            f"No records matched your criteria. Your columns are: {col_list}.\n"
            "Try relaxing your filter or check the column name and value."
        ),
        'value': 0,
        'sql_query': None,
    }