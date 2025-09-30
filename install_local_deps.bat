@echo off
echo Installing dependencies for local ETL pipeline...
echo.

REM Navigate to project directory
cd /d "c:\Users\ASUS\Downloads\dataeng\Databricks-ETL-Pipeline-using-PySpark-and-Delta-Lake-Architecture-main"

REM Install core dependencies
echo Installing PySpark and Delta Lake...
pip install pyspark==3.5.0 delta-spark==3.0.0

echo Installing data processing libraries...
pip install pandas==2.1.0 numpy==1.24.3 pyarrow==13.0.0

echo Installing visualization libraries...
pip install plotly==5.15.0 matplotlib==3.7.2 seaborn==0.12.2

echo Installing utilities...
pip install python-dotenv==1.0.0 jupyter==1.0.0

echo.
echo ✅ Dependencies installed successfully!
echo.
echo Next steps:
echo 1. Run: python run_local_pipeline.py
echo 2. Or run individual components as needed
echo.
pause