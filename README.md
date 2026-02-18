# 🌟 AuroraDB - AI-Powered Spreadsheet Analysis

Transform Excel chaos into clear insights with natural language queries!

## ✨ Features

✅ **Smart Login & Signup** - Secure authentication with your beautiful Stitch designs
✅ **File Upload** - Drag & drop Excel/CSV files
✅ **AI Queries** - Ask questions in plain English, get SQL results
✅ **Auto Visualizations** - Charts generated automatically
✅ **Query History** - Track all your analyses

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Setup Ollama (AI)
```bash
# Download from: https://ollama.ai/download
ollama pull llama3.2:3b
```

### 3. Configure
```bash
cp .env.example .env
# Edit .env and add your SECRET_KEY
```

### 4. Run!
```bash
python run.py
```

Visit: **http://localhost:5000**

## 📖 Full Documentation

- `INSTALLATION_GUIDE.md` - Step-by-step setup
- `COMPLETE_CODE_REFERENCE.md` - All code explained

## 🎯 Tech Stack

- **Backend:** Flask, SQLAlchemy, SQLite
- **Frontend:** HTML, TailwindCSS, JavaScript
- **AI:** Ollama + Llama 3.2
- **Data:** Pandas, NumPy, Plotly

## 📁 Project Structure

```
AuroraDB/
├── backend/          # Flask application
├── frontend/         # Your Stitch designs (functional!)
├── uploads/          # User files
└── run.py           # START HERE
```

## 🆘 Need Help?

1. Check `INSTALLATION_GUIDE.md`
2. Read `COMPLETE_CODE_REFERENCE.md`
3. Make sure Ollama is running

## 📝 License

MIT License - Feel free to use for your FYP!

---

**Built with ❤️ for your Final Year Project**
