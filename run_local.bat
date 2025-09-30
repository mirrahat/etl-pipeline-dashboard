@echo off
echo ========================================
echo 🚀 RUNNING LOCAL ETL PIPELINE
echo ========================================
echo.

REM Navigate to project directory
cd /d "c:\Users\ASUS\Downloads\dataeng\Databricks-ETL-Pipeline-using-PySpark-and-Delta-Lake-Architecture-main"

echo Checking Java installation...
java -version
if %errorlevel% neq 0 (
    echo ❌ Java not found! Please install Java 8 or 11
    echo Download from: https://adoptium.net/
    pause
    exit /b 1
)

echo.
echo Installing required packages...
pip install pyspark==3.5.0 delta-spark==3.0.0 pandas matplotlib

echo.
echo 🎬 Running the ETL pipeline...
python run_local_pipeline.py

echo.
echo Pipeline execution completed!
pause