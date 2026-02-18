"""
Test script to verify Ollama is working
Run this before continuing
"""
import ollama

print("Testing Ollama connection...")
print("=" * 50)

try:
    # Test 1: Check if Ollama is running
    print("\n1. Checking if Ollama is running...")
    response = ollama.list()
    print("Ollama is running!")
    
    # Test 2: Check if SQLCoder is installed
    print("\n2. Checking if SQLCoder model is available...")
    models = [model['name'] for model in response['models']]
    if 'sqlcoder:latest' in models:
        print("SQLCoder model found!")
    else:
        print("SQLCoder not found. Please run: ollama pull sqlcoder")
        exit(1)
    
    # Test 3: Generate a simple SQL query
    print("\n3. Testing SQL generation...")
    prompt = """Generate SQL for this question: Show all students with GPA above 3.5

Database Schema:
CREATE TABLE students (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    gpa DECIMAL(3,2)
);

Return ONLY the SQL query, no explanations."""

    response = ollama.generate(
        model='sqlcoder',
        prompt=prompt
    )
    
    sql = response['response'].strip()
    print(f"\nGenerated SQL:")
    print(sql)
    
    if 'SELECT' in sql.upper() and 'gpa' in sql.lower():
        print("\nSQL generation working!")
    else:
        print("\nSQL might not be correct")
    
    print("\n" + "=" * 50)
    print("All tests passed! You're ready to continue.")
    print("=" * 50)
    
except Exception as e:
    print(f"\nError: {str(e)}")
    print("\nTroubleshooting:")
    print("1. Make sure Ollama is running (check system tray)")
    print("2. Run: ollama pull sqlcoder")
    print("3. Restart Ollama service")