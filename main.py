"""
main.py — EduTest Pro Entry Point
Registers CBT and RDS blueprints on a single Flask app.
Deploy this file with gunicorn on Render — no URL changes needed.
"""

import os
import traceback
from flask import Flask, make_response
from flask_cors import CORS as FlaskCORS

from shared import get_db, cors_headers, make_rds_cors, ALL_ALLOWED_ORIGINS, RDS_URL
from main_cbt import cbt_bp
from main_rds import rds_bp

app = Flask(__name__)

# ── CORS (broad permissive — individual blueprints also set per-response headers)
FlaskCORS(app,
    origins=list(ALL_ALLOWED_ORIGINS) + [
        'https://*.web.app',
        'https://*.firebaseapp.com',
    ],
    supports_credentials=False,
    allow_headers=['Content-Type', 'Authorization', 'X-EduTest-Key'],
    methods=['GET', 'POST', 'OPTIONS'],
)

# ── Register blueprints
app.register_blueprint(cbt_bp)
app.register_blueprint(rds_bp)


# ── Health check
@app.route('/health', methods=['GET'])
def health():
    return make_response('{"ok":true}', 200, {'Content-Type': 'application/json'})


# ── Startup — initialise Firebase Admin eagerly so errors surface immediately
try:
    get_db()
    print('[STARTUP] Firebase Admin initialized successfully')
except Exception as _e:
    print('[STARTUP] WARNING: Firebase Admin init failed:', _e)


# ── Entrypoint
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
