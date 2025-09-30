# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements_azure.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY unified_dashboard.py .
COPY simple_local_pipeline.py .

# Expose port 8501 (Streamlit default)
EXPOSE 8501

# Health check
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# Set environment variables
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# Create a startup script
RUN echo '#!/bin/bash\n\
echo "=== Starting Streamlit ETL Dashboard ==="\n\
echo "Developed by: Mir Hasibul Hasan Rahat"\n\
echo "URL will be available at: http://localhost:8501"\n\
streamlit run unified_dashboard.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false\n\
' > /app/start.sh && chmod +x /app/start.sh

# Run the application
CMD ["/app/start.sh"]