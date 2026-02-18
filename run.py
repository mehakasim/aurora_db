from backend.app import create_app
import os

app = create_app()

if __name__ == '__main__':
    # Create uploads folder if it doesn't exist
    os.makedirs('uploads', exist_ok=True)
    
    # Run the application
    print("Starting AuroraDB...")
    print("Open your browser and go to: http://localhost:5000")
    
    app.run(
        debug=True,
        host='0.0.0.0',
        port=5000,
        use_reloader=True
    )