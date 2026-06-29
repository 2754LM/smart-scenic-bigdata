"""
Dev Frontend Server with Live Reload
====================================
Watches app/frontend/ for file changes and auto-refreshes browser.
Usage: python scripts/dev-frontend.py
"""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app', 'backend'))

try:
    from livereload import Server
except ImportError:
    print("livereload not installed. Run: pip install livereload")
    print("Fallback: python -m http.server 8080")
    PORT = int(os.environ.get("FRONTEND_PORT", 8080))
    os.chdir(os.path.join(os.path.dirname(__file__), '..', 'app', 'frontend'))
    os.system(f"python -m http.server {PORT}")
    sys.exit(1)

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), '..', 'app', 'frontend')
PORT = int(os.environ.get("FRONTEND_PORT", 8080))

os.chdir(FRONTEND_DIR)
print(f"Dev frontend server: http://localhost:{PORT}")
print(f"Watching: {FRONTEND_DIR}")
print("Auto-reload enabled. Edit files and browser refreshes automatically.")

server = Server()
server.watch(FRONTEND_DIR + "/*.html")
server.watch(FRONTEND_DIR + "/static/**/*")
server.serve(port=PORT, host="0.0.0.0")