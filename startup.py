#!/usr/bin/env python3
"""
Azure Startup Script for Streamlit ETL Dashboard
Developed by: Mir Hasibul Hasan Rahat
"""

import os
import sys
import subprocess

def main():
    """Main entry point for Azure Web App"""
    
    print("=== Azure Streamlit Startup ===")
    print("Starting ETL Dashboard by Mir Hasibul Hasan Rahat")
    
    # Set environment variables for Azure
    os.environ['STREAMLIT_SERVER_PORT'] = '8000'
    os.environ['STREAMLIT_SERVER_ADDRESS'] = '0.0.0.0'
    os.environ['STREAMLIT_SERVER_HEADLESS'] = 'true'
    os.environ['STREAMLIT_BROWSER_GATHER_USAGE_STATS'] = 'false'
    
    # Build Streamlit command
    cmd = [
        sys.executable, '-m', 'streamlit', 'run', 'unified_dashboard.py',
        '--server.port=8000',
        '--server.address=0.0.0.0',
        '--server.headless=true',
        '--browser.gatherUsageStats=false',
        '--server.enableCORS=false'
    ]
    
    print(f"Executing: {' '.join(cmd)}")
    
    # Start Streamlit
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error starting Streamlit: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()