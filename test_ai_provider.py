"""
Quick connectivity test for the configured AI provider.
Uses the same provider settings as the Flask app and prints the SQL returned.
"""
from backend.app.utils.ai_processor import build_sql_prompt, generate_sql_query


def main():
    schema_columns = ["student_id", "name", "gpa"]
    prompt = build_sql_prompt(
        question="Show all students with GPA above 3.5",
        table_name="students",
        columns_str=", ".join(schema_columns),
        column_hints="For 'gpa' use column 'gpa'",
        is_show_query=True,
        is_count_only=False
    )

    sql = generate_sql_query(prompt)
    print("Generated SQL:")
    print(sql)


if __name__ == "__main__":
    main()
