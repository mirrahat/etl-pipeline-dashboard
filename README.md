# ETL Pipeline Dashboard
### Complete ETL Pipeline + Data Lake + Analytics Dashboard

**Developed by: Mir Hasibul Hasan Rahat**

[![Streamlit](https://img.shields.io/badge/streamlit-1.28+-red.svg)](https://streamlit.io/)

This is my personal data engineering project demonstrating ETL pipeline implementation with modern visualization techniques. I built this platform to showcase my skills in data processing, pipeline architecture, and dashboard development.

## Key Features

### ETL Pipeline
- **Bronze to Silver to Gold** medallion architecture
- **Fast Processing**: Complete pipeline execution in approximately 0.04 seconds
- **JSON-Based Storage**: Lightweight and readable data format
- **Automated Data Cleaning**: Smart data validation and transformation

### Data Lake Architecture  
- **Multi-Format Support**: JSON, CSV, Parquet files
- **Partition-Based Optimization**: Decade-based data organization
- **Columnar Storage**: Efficient Parquet format for analytics
- **Enterprise Scalability**: Production-ready architecture patterns

### Unified Web Dashboard
- **Single URL Access**: Everything available at http://localhost:8505
- **Interactive Analytics**: Real-time charts and visualizations
- **Pipeline Management**: Run and monitor pipelines from the web interface
- **Data Explorer**: Browse and examine data at all processing layers
- **Architecture Diagrams**: Visual representation of the complete platform

## Architecture Overview

```
Data Sources (APIs, Files, Databases)
           ↓
Raw Zone (JSON, CSV, Parquet)
           ↓
Bronze Layer (Standardized + Metadata)
           ↓
Silver Layer (Cleaned + Enriched)
           ↓
Gold Layer (Analytics Ready)
           ↓
Interactive Dashboard (Streamlit)
```

## Getting Started

### Prerequisites
- Python 3.8 or higher
- Git (for cloning)

### Installation

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd etl-pipeline-dashboard
   ```

2. **Install dependencies**
   ```bash
   pip install pandas streamlit plotly pyarrow openpyxl
   ```

3. **Run the ETL Pipeline**
   ```bash
   # Simple ETL Pipeline
   python simple_local_pipeline.py
   
   # Enhanced Data Lake Pipeline
   python enhanced_data_lake.py
   ```

4. **Launch the Dashboard**
   ```bash
   streamlit run unified_dashboard.py --server.port 8505
   ```

5. **Open your browser**
   ```
   http://localhost:8505
   ```

## Usage Guide

### Running Pipelines

#### Option 1: Command Line
```bash
# Run simple ETL pipeline
python simple_local_pipeline.py

# Run enhanced data lake pipeline
python enhanced_data_lake.py
```

#### Option 2: Web Interface
1. Go to http://localhost:8505
2. Use the sidebar "Platform Control Center"
3. Click "Run Simple ETL Pipeline" or "Run Enhanced Data Lake"
4. Monitor execution in real-time

### Dashboard Features

#### Architecture Tab
- Visual diagram of complete platform architecture
- Component relationships and data flow
- Processing zones and storage layers
- Browse data at Bronze, Silver, and Gold layers
- Interactive data tables
- Search and filter capabilities
- Business intelligence visualizations

#### Data Lake Tab
## Sample Analytics


## Technical Details

   - Raw data ingestion
   - Basic data validation
   - Metadata preservation

2. **Silver Layer** (`data/silver/`)
   - Data cleaning and standardization
   - Quality checks and validation
   - Enhanced with business rules

3. **Gold Layer** (`data/gold/`)
   - Business-ready datasets
   - Optimized for reporting

### Data Lake Structure

│   ├── silver/        # Partitioned cleaned data
│   │   └── movies_enriched/
│   │       ├── decade=1970/
│   │       └── decade=2000/
│   └── gold/          # Analytics-ready datasets
└── [simple ETL structure]
```

- **Python**: Core processing language
- **Pandas**: Data manipulation and analysis
- **Streamlit**: Web dashboard framework
- **Plotly**: Interactive visualizations
- **PyArrow**: Efficient Parquet file handling

## Key Improvements Over Original

This enhanced version adds:

- **Local Execution** - No cloud dependencies required  
- **Web Dashboard** - Interactive visualization interface  
- **Dual Pipeline Support** - Simple ETL + Advanced Data Lake  
- **Real-time Monitoring** - Live pipeline execution tracking  
- **Business Analytics** - Ready-to-use business intelligence  
- **Multi-format Support** - JSON, CSV, Parquet compatibility  
- **Partition Strategy** - Optimized data organization  

## Performance

- **Pipeline Execution**: ~0.04 seconds for simple ETL
- **Data Processing**: Handles datasets up to several GB
- **Dashboard Response**: Near real-time updates
- **Memory Efficiency**: Optimized Pandas operations

## Future Enhancements

- **Azure Integration**: Deploy to Azure Data Factory
- **Advanced Analytics**: Machine learning integration
- **Data Quality Monitoring**: Automated quality checks
- **API Integration**: REST API for pipeline management


## Acknowledgments

- Original ETL Pipeline architecture concepts
- Streamlit community for the web framework
- Plotly for interactive visualization capabilities

## Support

If you encounter any issues:

1. Check the [Issues](../../issues) page
2. Ensure all dependencies are installed correctly
3. Verify Python version compatibility (3.8+)
4. Check that port 8505 is available

---

### Dashboard Preview

Once running, your dashboard will include:

- **Architecture Visualization**
- **Real-time Pipeline Status**  
- **Interactive Data Explorer**
- **Business Analytics Charts**
- **Data Lake Management**

**Start exploring your data engineering platform at http://localhost:8505!**

---

**Developed by: Mir Hasibul Hasan Rahat**  
*Built with care for the data engineering community*