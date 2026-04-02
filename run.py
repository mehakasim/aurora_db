import os

from backend.app import create_app

app = create_app()


if __name__ == '__main__':
    os.makedirs('uploads', exist_ok=True)

    print("\n" + "=" * 60)
    print("AuroraDB Starting...")
    print("=" * 60)
    print("Homepage: http://localhost:5000")
    print("Sign In: http://localhost:5000/auth/login")
    print("Sign Up: http://localhost:5000/auth/signup")
    print("Press Ctrl+C to stop")
    print("=" * 60 + "\n")

    app.run(
        debug=True,
        host='0.0.0.0',
        port=5000,
        use_reloader=True
    )
