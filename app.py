"""
Simple Flask wrapper for Azure deployment
Developed by: Mir Hasibul Hasan Rahat
"""
from flask import Flask, render_template_string
import subprocess
import os

app = Flask(__name__)

@app.route('/')
def home():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>ETL Pipeline Dashboard - Mir Hasibul Hasan Rahat</title>
        <style>
            body { font-family: Arial, sans-serif; text-align: center; padding: 50px; }
            .header { color: #4CAF50; margin-bottom: 30px; }
            .info { color: #666; margin: 20px 0; }
            .link { background: #4CAF50; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px; }
        </style>
    </head>
    <body>
        <h1 class="header">🚀 ETL Pipeline Dashboard</h1>
        <h2>Developed by: Mir Hasibul Hasan Rahat</h2>
        <p class="info">Data Engineer | Python Developer | Analytics Specialist</p>
        
        <div style="margin: 30px 0;">
            <h3>Project Features:</h3>
            <ul style="text-align: left; max-width: 600px; margin: 0 auto;">
                <li>✅ Bronze → Silver → Gold ETL Architecture</li>
                <li>✅ Interactive Data Processing Pipeline</li>
                <li>✅ Real-time Analytics Dashboard</li>
                <li>✅ Azure Cloud Integration</li>
                <li>✅ GitHub Repository with Clean Code</li>
            </ul>
        </div>
        
        <div style="margin: 30px 0;">
            <p><strong>GitHub Repository:</strong></p>
            <a href="https://github.com/mirrahat/etl-pipeline-dashboard" class="link" target="_blank">
                View Source Code
            </a>
        </div>
        
        <div style="margin: 30px 0;">
            <p><strong>Local Demo Available</strong></p>
            <p style="color: #999;">Contact for live demonstration of the interactive Streamlit dashboard</p>
        </div>
        
        <footer style="margin-top: 50px; color: #999; font-size: 0.9em;">
            <p>Built with Python, Streamlit, Azure, and deployed with professional DevOps practices</p>
        </footer>
    </body>
    </html>
    '''

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port)