# AuroraDB - AI-Powered Spreadsheet Analysis

Transform Excel files into SQL-backed insights with natural-language queries.

## Features

- Smart login and signup
- Excel and CSV upload
- Natural-language questions converted into SQL
- Auto-generated chart responses
- Query history tracking

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure AI for demo hosting
```bash
copy .env.example .env
```

Update `.env` with your provider key.

For Groq:
```env
USE_CLOUD_API=true
API_PROVIDER=groq
GROQ_API_KEY=your-key-here
GROQ_MODEL=llama-3.1-8b-instant
```

Optional local fallback:
```env
OLLAMA_MODEL=llama3.2:3b
```

### 3. Run locally
```bash
python run.py
```

Visit `http://localhost:5000`

## Deployment

This branch is set up for free-host friendly demos:

- `Procfile` starts the app with `gunicorn`
- `backend/app/utils/ai_processor.py` supports Groq, OpenAI, or OpenRouter through OpenAI-compatible HTTP APIs
- Ollama remains available as an optional local fallback

Suggested demo hosts:

- Hugging Face Spaces
- Render
- Koyeb

## Testing the AI path

Run:
```bash
python test_ai_provider.py
```

This sends a sample prompt through the configured provider and prints the SQL it returns.
