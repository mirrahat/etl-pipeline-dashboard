@echo off
echo ========================================
echo 🚀 QUICK LOCAL PIPELINE SETUP & RUN
echo ========================================

echo Step 1: Checking Java...
java -version
if %errorlevel% neq 0 (
    echo ❌ Java not found!
    echo Please install Java from: https://adoptium.net/
    echo Then set JAVA_HOME environment variable
    pause
    exit /b 1
)
echo ✅ Java found!

echo.
echo Step 2: Installing Python packages...
pip install --quiet pyspark==3.5.0
pip install --quiet delta-spark==3.0.0  
pip install --quiet pandas==2.1.0
pip install --quiet matplotlib==3.7.2
echo ✅ Packages installed!

echo.
echo Step 3: Running ETL Pipeline...
echo 🎬 Processing Bronze → Silver → Gold layers...
python run_local_pipeline.py

echo.
echo ✅ Pipeline Complete! Check the results above.
pause