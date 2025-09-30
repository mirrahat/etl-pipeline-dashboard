"""
WSGI wrapper for Streamlit on Azure Web Apps
Developed by: Mir Hasibul Hasan Rahat
"""

import subprocess
import sys
import os
from threading import Thread
import time

def run_streamlit():
    """Run Streamlit in a separate thread"""
    cmd = [
        sys.executable, "-m", "streamlit", "run", "unified_dashboard.py",
        "--server.port=8000",
        "--server.address=0.0.0.0", 
        "--server.headless=true",
        "--server.enableCORS=false",
        "--server.enableXsrfProtection=false"
    ]
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return process

def application(environ, start_response):
    """WSGI application entry point"""
    
    # Start Streamlit if not already running
    if not hasattr(application, 'streamlit_process'):
        application.streamlit_process = run_streamlit()
        time.sleep(5)  # Wait for Streamlit to start
    
    # Redirect to Streamlit
    status = '302 Found'
    headers = [('Location', 'http://localhost:8000')]
    start_response(status, headers)
    return [b'Redirecting to Streamlit...']

if __name__ == "__main__":
    # For direct execution, just run Streamlit
    print("Starting Streamlit on Azure...")
    process = run_streamlit()
    process.wait()