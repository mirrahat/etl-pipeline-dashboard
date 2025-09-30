#!/bin/bash
# Startup script for Azure Web App
# This runs your Streamlit dashboard in the cloud

echo "Starting Mir Hasibul Hasan Rahat's ETL Pipeline Dashboard..."

# Install dependencies
pip install -r requirements.txt

# Run Streamlit on the port Azure expects
streamlit run unified_dashboard.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true