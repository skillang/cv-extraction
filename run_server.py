#!/usr/bin/env python3
"""
Simple server runner that loads .env file
"""

import os
from pathlib import Path

def load_env():
    """Load .env file"""
    env_file = Path(".env")
    if env_file.exists():
        print("📂 Loading .env file...")
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        print("✅ Environment variables loaded!")
    else:
        print("❌ .env file not found!")

if __name__ == "__main__":
    # Load environment variables first
    load_env()
    
    # Check if MongoDB URL was loaded
    mongodb_url = os.environ.get("MONGODB_URL")
    if mongodb_url:
        print(f"✅ MongoDB URL loaded: {mongodb_url[:50]}...")
    else:
        print("❌ MONGODB_URL not found in .env file!")
        exit(1)
    
    # Then start the server
    import uvicorn
    
    print("🚀 Starting server with MongoDB Atlas...")
    print("📍 Server URL: http://127.0.0.1:8000")
    print("📚 API Docs: http://127.0.0.1:8000/docs")
    print("🔧 Health Check: http://127.0.0.1:8000/health")
    print("=" * 50)
    
    # Use import string for reload to work properly
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)