"""
ETL Pipeline Anal    #else:
     #else:
    # Local configuration
    st.set_page_config(
        page_title="Data Engineering Platform",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded"
    )onfiguration
    st.set_page_config(
        page_title="Data Engineering Platform",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded"
    )cal configuration
    st.set_page_config(
        page_title="Data Engineering Platform",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded"
    )   page_title="Data Engineering Platform",al configuration
    st.set_page_config(
        page_title="Data Engineering Platform",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded"
    )ashboard
Author: # Custom CSSul Hasan Rahat
Date: September 2025

Interactive web dashboard for ETL pipeline monitoring and data analytics.
Built with Streamlit, Plotly, and Pandas for real-time data visualization.
Integrates Bronze-Silver-Gold data layers with business intelligence charts.
"""

import streamlit as st
import os

# Azure-specific configuration
if os.getenv('WEBSITE_HOSTNAME'):  # Running on Azure
    st.set_page_config(
        page_title="ETL Dashboard - Mir Hasibul Hasan Rahat",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded"
    )
else:
    # Local configuration
    st.set_page_config(
        page_title="Data Engineering Plat                            st.markdown("### Partitioning Strategy")orm",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded"
    )

# Initialize session state for tracking pipeline runs
if 'last_etl_run' not in st.session_state:
    st.session_state.last_etl_run = None
if 'last_lake_run' not in st.session_state:
    st.session_state.last_lake_run = None

import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import time
from datetime import datetime
import subprocess
import pyarrow.parquet as pq

# Page configuration
st.set_page_config(
    page_title="Data Engineering Platform",
    page_icon="�",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3.5rem;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 1rem;
    }
    .pipeline-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin: 1rem 0;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
    }
    .success-card {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        padding: 1rem;
        border-radius: 8px;
        color: white;
        margin: 0.5rem 0;
    }
    .zone-indicator {
        display: inline-block;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        color: white;
        margin: 0.2rem;
        font-weight: bold;
    }
    .bronze { background: #CD7F32; }
    .silver { background: #C0C0C0; }
    .gold { background: #FFD700; color: black; }
    .raw { background: #FF6B6B; }
    
    .metric-highlight {
        background: linear-gradient(45deg, #f093fb 0%, #f5576c 100%);
        padding: 1rem;
        border-radius: 8px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_all_data():
    """Load data from all sources - ETL pipeline and Data Lake"""
    all_data = {
        'etl_pipeline': {},
        'data_lake': {},
        'status': {'etl_exists': False, 'lake_exists': False}
    }
    
    # Load ETL Pipeline data (simple pipeline)
    try:
        if os.path.exists("data/bronze/movies.json"):
            with open("data/bronze/movies.json", 'r') as f:
                all_data['etl_pipeline']['bronze'] = json.load(f)
                all_data['status']['etl_exists'] = True
        
        if os.path.exists("data/silver/movies_clean.json"):
            with open("data/silver/movies_clean.json", 'r') as f:
                all_data['etl_pipeline']['silver'] = json.load(f)
        
        if os.path.exists("data/gold/genre_analysis.json"):
            with open("data/gold/genre_analysis.json", 'r') as f:
                all_data['etl_pipeline']['gold_genre'] = json.load(f)
                
        if os.path.exists("data/gold/era_analysis.json"):
            with open("data/gold/era_analysis.json", 'r') as f:
                all_data['etl_pipeline']['gold_era'] = json.load(f)
                
        if os.path.exists("data/gold/top_movies.json"):
            with open("data/gold/top_movies.json", 'r') as f:
                all_data['etl_pipeline']['gold_top'] = json.load(f)
    except Exception as e:
        st.sidebar.warning(f"ETL data loading issue: {e}")
    
    # Load Data Lake data (enhanced pipeline)
    try:
        base_path = "data/lake"
        if os.path.exists(base_path):
            all_data['status']['lake_exists'] = True
            
            # Bronze zone
            if os.path.exists(f"{base_path}/bronze"):
                bronze_data = {}
                for file in os.listdir(f"{base_path}/bronze"):
                    if file.endswith('.parquet'):
                        table_name = file.replace('.parquet', '')
                        bronze_data[table_name] = pd.read_parquet(f"{base_path}/bronze/{file}")
                all_data['data_lake']['bronze'] = bronze_data
            
            # Silver zone
            if os.path.exists(f"{base_path}/silver/movies_enriched"):
                silver_data = {}
                for decade_dir in os.listdir(f"{base_path}/silver/movies_enriched"):
                    if decade_dir.startswith('decade='):
                        decade = decade_dir.split('=')[1]
                        decade_data = pd.read_parquet(f"{base_path}/silver/movies_enriched/{decade_dir}/data.parquet")
                        if 'enriched_movies' not in silver_data:
                            silver_data['enriched_movies'] = decade_data
                        else:
                            silver_data['enriched_movies'] = pd.concat([silver_data['enriched_movies'], decade_data])
                all_data['data_lake']['silver'] = silver_data
            
            # Gold zone
            if os.path.exists(f"{base_path}/gold"):
                gold_data = {}
                for file in os.listdir(f"{base_path}/gold"):
                    if file.endswith('.parquet'):
                        analysis_name = file.replace('.parquet', '')
                        gold_data[analysis_name] = pd.read_parquet(f"{base_path}/gold/{file}")
                all_data['data_lake']['gold'] = gold_data
                
    except Exception as e:
        st.sidebar.warning(f"Data Lake loading issue: {e}")
    
    return all_data

def create_unified_architecture_diagram():
    """Create comprehensive architecture diagram"""
    
    fig = go.Figure()
    
    # Data sources
    sources = [
        {'name': 'APIs', 'x': 0.5, 'y': 5, 'color': '#34495E'},
        {'name': 'Files', 'x': 0.5, 'y': 4.5, 'color': '#34495E'},
        {'name': 'Databases', 'x': 0.5, 'y': 4, 'color': '#34495E'}
    ]
    
    # Processing zones
    zones = [
        {'name': 'Raw Zone', 'x': 2, 'y': 4.5, 'color': '#FF6B6B', 'desc': 'JSON, CSV\nParquet'},
        {'name': 'Bronze Layer', 'x': 3.5, 'y': 4.5, 'color': '#CD7F32', 'desc': 'Standardized\nMetadata'},
        {'name': 'Silver Layer', 'x': 5, 'y': 4.5, 'color': '#C0C0C0', 'desc': 'Cleaned\nEnriched'},
        {'name': 'Gold Layer', 'x': 6.5, 'y': 4.5, 'color': '#FFD700', 'desc': 'Analytics\nReady'},
    ]
    
    # Output destinations
    outputs = [
        {'name': 'Dashboards', 'x': 8, 'y': 5, 'color': '#2ECC71'},
        {'name': 'BI Tools', 'x': 8, 'y': 4.5, 'color': '#2ECC71'},
        {'name': 'Reports', 'x': 8, 'y': 4, 'color': '#2ECC71'}
    ]
    
    # Add all components
    all_components = sources + zones + outputs
    
    for comp in all_components:
        fig.add_shape(
            type="rect",
            x0=comp['x']-0.35, y0=comp['y']-0.25,
            x1=comp['x']+0.35, y1=comp['y']+0.25,
            fillcolor=comp['color'],
            opacity=0.8,
            line=dict(color=comp['color'], width=2)
        )
        
        fig.add_annotation(
            x=comp['x'], y=comp['y']+0.05,
            text=f"<b>{comp['name']}</b>",
            showarrow=False,
            font=dict(color="white" if comp['color'] != '#FFD700' else "black", size=10)
        )
        
        if 'desc' in comp:
            fig.add_annotation(
                x=comp['x'], y=comp['y']-0.1,
                text=comp['desc'],
                showarrow=False,
                font=dict(color="white" if comp['color'] != '#FFD700' else "black", size=8)
            )
    
    # Add arrows
    arrows = [
        (sources[1]['x']+0.4, sources[1]['y'], zones[0]['x']-0.4, zones[0]['y']),
        (zones[0]['x']+0.4, zones[0]['y'], zones[1]['x']-0.4, zones[1]['y']),
        (zones[1]['x']+0.4, zones[1]['y'], zones[2]['x']-0.4, zones[2]['y']),
        (zones[2]['x']+0.4, zones[2]['y'], zones[3]['x']-0.4, zones[3]['y']),
        (zones[3]['x']+0.4, zones[3]['y'], outputs[1]['x']-0.4, outputs[1]['y'])
    ]
    
    for x1, y1, x2, y2 in arrows:
        fig.add_annotation(
            x=x1, y=y1, ax=x2, ay=y2,
            arrowhead=2, arrowsize=1.5, arrowwidth=2,
            arrowcolor="#2C3E50"
        )
    
    fig.update_layout(
        title="Complete Data Engineering Platform Architecture",
        showlegend=False,
        xaxis=dict(range=[0, 9], showgrid=False, showticklabels=False),
        yaxis=dict(range=[3.5, 5.5], showgrid=False, showticklabels=False),
        plot_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    return fig

def create_pipeline_status_indicator(data):
    """Create pipeline status indicators based on current session"""
    
    # Determine active pipeline based on session state
    active_pipeline = None
    if st.session_state.last_etl_run and st.session_state.last_lake_run:
        active_pipeline = "Both Pipelines"
    elif st.session_state.last_etl_run:
        active_pipeline = "Simple ETL"
    elif st.session_state.last_lake_run:
        active_pipeline = "Data Lake"
    
    # Session Status Header
    if active_pipeline:
        st.success(f"**Active Pipeline**: {active_pipeline}")
        if st.session_state.last_etl_run:
            st.info(f"ETL executed at: {st.session_state.last_etl_run.strftime('%H:%M:%S')}")
        if st.session_state.last_lake_run:
            st.info(f"Data Lake executed at: {st.session_state.last_lake_run.strftime('%H:%M:%S')}")
    else:
        st.warning("**No Pipeline Executed in Current Session**")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Determine status based on session and file existence
    etl_session_active = st.session_state.last_etl_run is not None
    lake_session_active = st.session_state.last_lake_run is not None
    
    zone_status = {
        'raw': "Active" if (etl_session_active and data['status']['etl_exists']) or (lake_session_active and data['status']['lake_exists']) else "Inactive",
        'bronze': "Active" if (etl_session_active and data['status']['etl_exists']) or (lake_session_active and data['status']['lake_exists']) else "Inactive", 
        'silver': "Active" if (etl_session_active and data['status']['etl_exists']) or (lake_session_active and data['status']['lake_exists']) else "Inactive",
        'gold': "Active" if (etl_session_active and data['status']['etl_exists']) or (lake_session_active and data['status']['lake_exists']) else "Inactive"
    }
    
    with col1:
        st.markdown(f'<div class="zone-indicator raw">Raw Zone<br/>{zone_status["raw"]}</div>', 
                   unsafe_allow_html=True)
    
    with col2:
        st.markdown(f'<div class="zone-indicator bronze">Bronze Layer<br/>{zone_status["bronze"]}</div>', 
                   unsafe_allow_html=True)
    
    with col3:
        st.markdown(f'<div class="zone-indicator silver">Silver Layer<br/>{zone_status["silver"]}</div>', 
                   unsafe_allow_html=True)
    
    with col4:
        st.markdown(f'<div class="zone-indicator gold">Gold Layer<br/>{zone_status["gold"]}</div>', 
                   unsafe_allow_html=True)

def run_pipeline_selector():
    """Pipeline execution options"""
    
    st.sidebar.markdown("### Pipeline Execution")
    
    # Show pipeline status
    etl_status = "Active" if st.session_state.last_etl_run else "Inactive"
    lake_status = "Active" if st.session_state.last_lake_run else "Inactive"
    
    st.sidebar.markdown(f"""
    **Session Status:**
    - {etl_status} Simple ETL Pipeline
    - {lake_status} Enhanced Data Lake Pipeline
    """)
    
    if st.session_state.last_etl_run and st.session_state.last_lake_run:
        st.sidebar.success("Both pipelines ready for comparison!")
    elif st.session_state.last_etl_run or st.session_state.last_lake_run:
        st.sidebar.info("Run both pipelines to enable comparison.")
    
    pipeline_choice = st.sidebar.selectbox(
        "Choose Pipeline to Run:",
        ["Simple ETL Pipeline", "Enhanced Data Lake Pipeline", "Both Pipelines"]
    )
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        execute_btn = st.button("Execute Pipeline", type="primary")
    with col2:
        if st.button("Clear Session", help="Reset pipeline tracking"):
            st.session_state.last_etl_run = None
            st.session_state.last_lake_run = None
            st.success("Session cleared!")
            st.rerun()
    
    if execute_btn:
        with st.spinner(f"Running {pipeline_choice}..."):
            try:
                success = False
                
                if pipeline_choice == "Simple ETL Pipeline":
                    result = subprocess.run(
                        ["python", "simple_local_pipeline.py"],
                        capture_output=True, text=True, cwd="."
                    )
                    success = result.returncode == 0
                    if success:
                        st.session_state.last_etl_run = datetime.now()
                    
                elif pipeline_choice == "Enhanced Data Lake Pipeline":
                    result = subprocess.run(
                        ["python", "enhanced_data_lake.py"],
                        capture_output=True, text=True, cwd="."
                    )
                    success = result.returncode == 0
                    if success:
                        st.session_state.last_lake_run = datetime.now()
                    
                elif pipeline_choice == "Both Pipelines":
                    result1 = subprocess.run(
                        ["python", "simple_local_pipeline.py"],
                        capture_output=True, text=True, cwd="."
                    )
                    result2 = subprocess.run(
                        ["python", "enhanced_data_lake.py"],
                        capture_output=True, text=True, cwd="."
                    )
                    success = result1.returncode == 0 and result2.returncode == 0
                    result = result2  # Show last result
                    if success:
                        st.session_state.last_etl_run = datetime.now()
                        st.session_state.last_lake_run = datetime.now()
                
                if success:
                    st.success(f"{pipeline_choice} completed successfully!")
                    st.code(result.stdout, language="text")
                    st.cache_data.clear()  # Clear cache to reload data
                    st.rerun()
                else:
                    st.error(f"❌ {pipeline_choice} failed!")
                    st.code(result.stderr, language="text")
                    
            except Exception as e:
                st.error(f"Error running pipeline: {e}")

def create_unified_analytics(data):
    """Create unified analytics from both ETL and Data Lake"""
    
    analytics = {}
    
    # From ETL Pipeline
    if 'gold_genre' in data['etl_pipeline']:
        etl_genre_df = pd.DataFrame(data['etl_pipeline']['gold_genre'])
        # The DataFrame has genres as index, reset it to make them a column
        etl_genre_df = etl_genre_df.reset_index()
        etl_genre_df = etl_genre_df.rename(columns={'index': 'genre'})
        analytics['etl_genre'] = etl_genre_df
    
    # From Data Lake
    if 'gold' in data['data_lake'] and 'genre_performance' in data['data_lake']['gold']:
        lake_genre_df = data['data_lake']['gold']['genre_performance'].reset_index()
        lake_genre_df = lake_genre_df.rename(columns={'index': 'genre'})
        analytics['lake_genre'] = lake_genre_df
    
    # Add session tracking to ensure we only compare when both were run in current session
    analytics['has_fresh_etl'] = data['status'].get('etl_exists', False)
    analytics['has_fresh_lake'] = data['status'].get('lake_exists', False)
    
    return analytics

def main():
    """Main Unified Dashboard"""
    
    # Header
    st.markdown('<h1 class="main-header">Data Engineering Platform</h1>', unsafe_allow_html=True)
    st.markdown("""
    <div style='text-align: center; margin-bottom: 30px;'>
        <h3 style='color: #4CAF50; margin-bottom: 5px;'>Developed by: Mir Hasibul Hasan Rahat</h3>
        <p style='color: #666; font-size: 1.1em; margin-bottom: 15px;'><strong>Data Engineer | Python Developer | Analytics Specialist</strong></p>
        <p style='color: #333; font-size: 1.2em;'><strong>Complete ETL Pipeline + Data Lake + Analytics in One Dashboard</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    st.sidebar.title("Platform Control Center")
    run_pipeline_selector()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Platform Features")
    st.sidebar.info("""
    **ETL Pipeline:**
    - Simple Bronze→Silver→Gold processing
    - JSON-based data storage
    - Fast execution (~0.04 seconds)
    
    **Data Lake:**
    - Multi-format support (JSON, CSV, Parquet)
    - Partition-based optimization
    - Enterprise-grade architecture
    
    **Analytics:**
    - Business intelligence dashboards
    - Interactive visualizations
    - Real-time insights
    """)
    
    # Load all data
    data = load_all_data()
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Architecture", 
        "Pipeline Status", 
        "Data Explorer", 
        "Analytics", 
        "Data Lake"
    ])
    
    with tab1:
        st.markdown("## Complete Platform Architecture")
        
        # Architecture diagram
        arch_fig = create_unified_architecture_diagram()
        st.plotly_chart(arch_fig, use_container_width=True)
        
        # Platform overview
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            <div class="pipeline-card">
                <h3>ETL Pipeline</h3>
                <p>• Fast local processing</p>
                <p>• JSON-based storage</p>
                <p>• Bronze → Silver → Gold layers</p>
                <p>• Business analytics generation</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="pipeline-card">
                <h3>Data Lake</h3>
                <p>• Multi-format support</p>
                <p>• Columnar storage (Parquet)</p>
                <p>• Partition-based optimization</p>
                <p>• Enterprise scalability</p>
            </div>
            """, unsafe_allow_html=True)
    
    with tab2:
        st.markdown("## Pipeline Status & Metrics")
        
        # Status indicators
        create_pipeline_status_indicator(data)
        
        # Session-based Metrics
        st.markdown("### Current Session Metrics")
        
        etl_records = 0
        lake_records = 0
        
        # Only count records from pipelines run in current session
        if st.session_state.last_etl_run and data['status']['etl_exists']:
            etl_records = len(data['etl_pipeline'].get('bronze', []))
        
        if st.session_state.last_lake_run and data['status']['lake_exists']:
            if 'bronze' in data['data_lake']:
                for dataset in data['data_lake']['bronze'].values():
                    if isinstance(dataset, pd.DataFrame):
                        lake_records += len(dataset)
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("ETL Records", f"{etl_records:,}", 
                   help="Records from Simple ETL Pipeline (current session)")
        col2.metric("Lake Records", f"{lake_records:,}",
                   help="Records from Data Lake Pipeline (current session)")
        
        active_pipelines = 0
        if st.session_state.last_etl_run and data['status']['etl_exists']:
            active_pipelines += 1
        if st.session_state.last_lake_run and data['status']['lake_exists']:
            active_pipelines += 1
            
        col3.metric("Active Pipelines", active_pipelines,
                   help="Pipelines executed in current session")
        col4.metric("Zones Available", 4,
                   help="Bronze, Silver, Gold + Raw data zones")
        
        # Session execution summary
        if st.session_state.last_etl_run or st.session_state.last_lake_run:
            st.markdown("""
            <div class="success-card">
                <h4>Platform Status: OPERATIONAL</h4>
                <p>Pipeline executed successfully in current session. Data is ready for exploration and analysis.</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Execution timeline
            st.markdown("### Execution Timeline")
            timeline_data = []
            if st.session_state.last_etl_run:
                timeline_data.append({
                    'Pipeline': 'Simple ETL',
                    'Executed At': st.session_state.last_etl_run.strftime('%H:%M:%S'),
                    'Status': 'Success',
                    'Records': etl_records
                })
            if st.session_state.last_lake_run:
                timeline_data.append({
                    'Pipeline': 'Data Lake',
                    'Executed At': st.session_state.last_lake_run.strftime('%H:%M:%S'),
                    'Status': 'Success', 
                    'Records': lake_records
                })
            
            if timeline_data:
                st.dataframe(pd.DataFrame(timeline_data), use_container_width=True)
        else:
            st.markdown("""
            <div class="warning-card">
                <h4>Platform Status: READY</h4>
                <p>No pipelines executed in current session. Ready to run your first pipeline!</p>
            </div>
            """, unsafe_allow_html=True)
    
    with tab3:
        st.markdown("## Unified Data Explorer")
        
        # Data source selector based on current session
        available_sources = []
        
        # Only show sources for pipelines run in current session
        if st.session_state.last_etl_run is not None and data['status']['etl_exists']:
            available_sources.append("ETL Pipeline Data")
        if st.session_state.last_lake_run is not None and data['status']['lake_exists']:
            available_sources.append("Data Lake")
        
        # Show session status
        if st.session_state.last_etl_run or st.session_state.last_lake_run:
            st.info(f"**Current Session**: " + 
                   ("ETL Pipeline Active " if st.session_state.last_etl_run else "") +
                   ("Data Lake Active" if st.session_state.last_lake_run else ""))
        
        if available_sources:
            # Show pipeline comparison if both are available
            if len(available_sources) == 2:
                st.markdown("### Pipeline Comparison Available")
                comparison_col1, comparison_col2 = st.columns(2)
                
                with comparison_col1:
                    st.markdown("""
                    **Simple ETL Pipeline:**
                    - **Storage**: JSON files
                    - **Movies**: 10 records
                    - **Processing**: Sequential layers
                    - **Speed**: ~0.04 seconds
                    """)
                
                with comparison_col2:
                    st.markdown("""
                    **Data Lake Pipeline:**
                    - **Storage**: Parquet columnar
                    - **Data Sources**: Movies + Ratings
                    - **Features**: Partitioning, Multi-format
                    - **Architecture**: Enterprise-grade
                    """)
            
            selected_source = st.selectbox("Select Data Source:", available_sources)
            
            if selected_source == "ETL Pipeline Data" and data['etl_pipeline']:
                st.markdown("### Simple ETL Pipeline Data")
                st.success(f"Executed at: {st.session_state.last_etl_run.strftime('%H:%M:%S')}")
                
                # ETL Pipeline characteristics
                st.info("**Pipeline Type**: Standard ETL with JSON storage | **Dataset**: 10 movies | **Processing**: ~0.04s")
                
                total_records = 0
                layer_count = 0
                
                for layer, layer_data in data['etl_pipeline'].items():
                    if layer_data:
                        layer_count += 1
                        with st.expander(f"{layer.title()} Layer", expanded=True):
                            if isinstance(layer_data, list):
                                df = pd.DataFrame(layer_data)
                                total_records += len(df)
                                
                                col1, col2, col3, col4 = st.columns(4)
                                col1.metric("Records", len(df))
                                col2.metric("Columns", len(df.columns))
                                col3.metric("Format", "JSON")
                                col4.metric("Size", f"{len(str(layer_data))/1024:.1f} KB")
                                
                                # Show column info
                                if len(df.columns) > 0:
                                    st.write("**Columns:**", ", ".join(df.columns.tolist()))
                                
                                # Show sample data with more context
                                st.write("**Sample Data:**")
                                st.dataframe(df.head(5), use_container_width=True)
                                
                            elif isinstance(layer_data, dict):
                                df = pd.DataFrame(layer_data)
                                total_records += len(df)
                                
                                col1, col2, col3, col4 = st.columns(4)
                                col1.metric("Records", len(df))
                                col2.metric("Columns", len(df.columns))
                                col3.metric("Format", "JSON Aggregated")
                                col4.metric("Size", f"{len(str(layer_data))/1024:.1f} KB")
                                
                                if len(df.columns) > 0:
                                    st.write("**Columns:**", ", ".join(df.columns.tolist()))
                                st.write("**Analytics Data:**")
                                st.dataframe(df, use_container_width=True)
                
                # Pipeline summary
                st.markdown("### ETL Pipeline Summary")
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Records Processed", total_records)
                col2.metric("Data Layers", layer_count)
                col3.metric("Storage Method", "File-based JSON")
            
            elif selected_source == "Data Lake" and data['data_lake']:
                st.markdown("### Enhanced Data Lake Explorer")
                st.success(f"Executed at: {st.session_state.last_lake_run.strftime('%H:%M:%S')}")
                
                # Data Lake characteristics
                st.info("**Pipeline Type**: Enterprise Data Lake with Parquet storage | **Dataset**: Multi-source data | **Features**: Partitioning, Columnar")
                
                lake_zone = st.selectbox("Select Zone:", list(data['data_lake'].keys()))
                
                if lake_zone in data['data_lake']:
                    zone_data = data['data_lake'][lake_zone]
                    
                    st.markdown(f"### {lake_zone.title()} Zone Analysis")
                    
                    total_datasets = len(zone_data)
                    total_records = 0
                    total_memory = 0
                    
                    for dataset_name, dataset in zone_data.items():
                        with st.expander(f"{dataset_name.replace('_', ' ').title()}", expanded=True):
                            if isinstance(dataset, pd.DataFrame):
                                total_records += len(dataset)
                                dataset_memory = dataset.memory_usage(deep=True).sum()
                                total_memory += dataset_memory
                                
                                col1, col2, col3, col4 = st.columns(4)
                                col1.metric("Records", len(dataset))
                                col2.metric("Columns", len(dataset.columns))
                                col3.metric("Format", "Parquet")
                                col4.metric("Memory", f"{dataset_memory/1024:.1f} KB")
                                
                                # Show column types and info
                                if len(dataset.columns) > 0:
                                    st.write("**Column Types:**")
                                    col_info = pd.DataFrame({
                                        'Column': dataset.columns,
                                        'Type': [str(dtype) for dtype in dataset.dtypes],
                                        'Non-Null': [dataset[col].count() for col in dataset.columns]
                                    })
                                    st.dataframe(col_info, use_container_width=True)
                                
                                # Show sample data
                                st.write("**Sample Data:**")
                                st.dataframe(dataset.head(5), use_container_width=True)
                                
                                # Show unique values for categorical columns
                                categorical_cols = dataset.select_dtypes(include=['object']).columns
                                if len(categorical_cols) > 0:
                                    st.write("**Categorical Data Summary:**")
                                    for col in categorical_cols[:3]:  # Show first 3 categorical columns
                                        unique_vals = dataset[col].value_counts().head(5)
                                        st.write(f"**{col}:** {', '.join([f'{k}({v})' for k, v in unique_vals.items()])}")
                    
                    # Zone summary
                    st.markdown(f"### {lake_zone.title()} Zone Summary")
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Total Datasets", total_datasets)
                    col2.metric("Total Records", total_records)
                    col3.metric("Total Memory", f"{total_memory/1024:.1f} KB")
                    col4.metric("Storage Format", "Columnar Parquet")
                    
                    # Show partitioning info if silver zone
                    if lake_zone == 'silver' and os.path.exists("data/lake/silver/movies_enriched"):
                        partitions = [d for d in os.listdir("data/lake/silver/movies_enriched") if d.startswith('decade=')]
                        if partitions:
                            st.markdown("### 🗂️ Partitioning Strategy")
                            st.success(f"Data partitioned by decade: {len(partitions)} partitions ({', '.join(partitions)})")
                
                # Overall Data Lake summary
                st.markdown("### Data Lake Architecture Summary")
                lake_summary_col1, lake_summary_col2, lake_summary_col3 = st.columns(3)
                lake_summary_col1.metric("Available Zones", len(data['data_lake']))
                lake_summary_col2.metric("Storage Technology", "Apache Parquet")
                lake_summary_col3.metric("Architecture", "Medallion (Bronze→Silver→Gold)")
        else:
            # Show helpful message based on what pipelines are available
            if not st.session_state.last_etl_run and not st.session_state.last_lake_run:
                st.info("**No Pipeline Executed Yet**")
                st.markdown("""
                ### How to Explore Data:
                1. **Go to Sidebar** → Select a pipeline
                2. **Execute Pipeline** → Click the execute button  
                3. **Return Here** → Explore your data results
                
                **Available Options:**
                - **Simple ETL Pipeline** → Standard JSON-based processing
                - **Enhanced Data Lake Pipeline** → Advanced Parquet-based lake
                - **Both Pipelines** → Complete comparison view
                """)
            elif st.session_state.last_etl_run and not data['status']['etl_exists']:
                st.error("ETL Pipeline was executed but data files are missing!")
            elif st.session_state.last_lake_run and not data['status']['lake_exists']:
                st.error("Data Lake Pipeline was executed but data files are missing!")
    
    with tab4:
        st.markdown("## Unified Analytics Dashboard")
        
        analytics = create_unified_analytics(data)
        
        # Check what pipelines have been run in current session
        etl_run_in_session = st.session_state.last_etl_run is not None
        lake_run_in_session = st.session_state.last_lake_run is not None
        
        if analytics and (etl_run_in_session or lake_run_in_session):
            # Only show comparison if both pipelines have been run in current session
            both_pipelines_run = etl_run_in_session and lake_run_in_session
            
            # Compare ETL vs Data Lake results (only if both run in current session)
            if 'etl_genre' in analytics and 'lake_genre' in analytics and both_pipelines_run:
                st.markdown("### ETL vs Data Lake Comparison")
                st.info("Both pipelines have been executed in this session. Comparing results...")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### ETL Pipeline Results")
                    etl_fig = px.bar(
                        analytics['etl_genre'], 
                        x='genre', y='total_revenue',
                        title="Revenue by Genre (ETL)",
                        color='total_revenue'
                    )
                    st.plotly_chart(etl_fig, use_container_width=True)
                
                with col2:
                    st.markdown("#### Data Lake Results")
                    lake_fig = px.bar(
                        analytics['lake_genre'], 
                        x='genre', y='revenue',
                        title="Revenue by Genre (Data Lake)",
                        color='revenue'
                    )
                    st.plotly_chart(lake_fig, use_container_width=True)
            
            elif 'etl_genre' in analytics and etl_run_in_session and not both_pipelines_run:
                st.markdown("### ETL Pipeline Analytics")
                st.success(f"ETL Pipeline executed at: {st.session_state.last_etl_run.strftime('%H:%M:%S')}")
                st.info("Showing ETL Pipeline results. Run Enhanced Data Lake Pipeline to see comparison.")
                etl_fig = px.bar(
                    analytics['etl_genre'], 
                    x='genre', y='total_revenue',
                    title="Revenue by Genre (ETL Pipeline)",
                    color='total_revenue'
                )
                st.plotly_chart(etl_fig, use_container_width=True)
                st.dataframe(analytics['etl_genre'], use_container_width=True)
            
            elif 'lake_genre' in analytics and lake_run_in_session and not both_pipelines_run:
                st.markdown("### Data Lake Analytics")
                st.success(f"Data Lake Pipeline executed at: {st.session_state.last_lake_run.strftime('%H:%M:%S')}")
                st.info("Showing Data Lake results. Run Simple ETL Pipeline to see comparison.")
                lake_fig = px.bar(
                    analytics['lake_genre'], 
                    x='genre', y='revenue',
                    title="Revenue by Genre (Data Lake)",
                    color='revenue'
                )
                st.plotly_chart(lake_fig, use_container_width=True)
                st.dataframe(analytics['lake_genre'], use_container_width=True)
            else:
                # This case shouldn't happen, but just in case
                st.info("Analytics data exists but no matching pipeline was run in current session.")
        else:
            # No pipelines run in current session
            st.info("**No Analytics Available Yet**")
            st.markdown("""
            ### Generate Analytics:
            
            **To see analytics and insights:**
            1. **Go to Sidebar** → Choose a pipeline
            2. **Execute Pipeline** → Click 'Execute Pipeline' button
            3. **Return Here** → View your analytics results
            
            **Pipeline Options:**
            - **Simple ETL Pipeline** → Basic analytics with JSON data
            - **Enhanced Data Lake Pipeline** → Advanced analytics with Parquet data
            - **Both Pipelines** → Side-by-side comparison analytics
            
            **What You'll See:**
            - Revenue by genre charts
            - Business intelligence metrics  
            - Interactive data exploration
            - 📋 Detailed data tables
            """)
    
    with tab5:
        st.markdown("## Data Lake Deep Dive")
        
        # Check if Data Lake was run in current session
        if st.session_state.last_lake_run is not None:
            if data['status']['lake_exists']:
                # Data Lake specific features
                st.success(f"Data Lake Pipeline last executed: {st.session_state.last_lake_run.strftime('%H:%M:%S')}")
                st.markdown("### Lake Zones Overview")
                
                for zone_name, zone_data in data['data_lake'].items():
                    if zone_data:
                        with st.expander(f"{zone_name.title()} Zone Details", expanded=True):
                            for dataset_name, dataset in zone_data.items():
                                if isinstance(dataset, pd.DataFrame):
                                    col1, col2, col3 = st.columns(3)
                                    col1.metric("Records", len(dataset))
                                    col2.metric("Columns", len(dataset.columns))
                                    col3.metric("Data Size", f"{dataset.memory_usage(deep=True).sum() / 1024:.1f} KB")
                                    st.write(f"**Dataset: {dataset_name.replace('_', ' ').title()}**")
                                    st.dataframe(dataset.head(5), use_container_width=True)
                
                # Partitioning info
                if os.path.exists("data/lake/silver/movies_enriched"):
                    partitions = [d for d in os.listdir("data/lake/silver/movies_enriched") if d.startswith('decade=')]
                    st.markdown(f"### 🗂️ Partitioning Strategy")
                    st.info(f"Silver layer partitioned by decade: {', '.join(partitions)}")
                    
                    # Show partition sizes
                    if partitions:
                        partition_info = []
                        for partition in partitions:
                            partition_path = f"data/lake/silver/movies_enriched/{partition}/data.parquet"
                            if os.path.exists(partition_path):
                                df = pd.read_parquet(partition_path)
                                partition_info.append({
                                    'Decade': partition.replace('decade=', ''),
                                    'Records': len(df),
                                    'Size (KB)': f"{df.memory_usage(deep=True).sum() / 1024:.1f}"
                                })
                        
                        if partition_info:
                            st.markdown("### Partition Details")
                            st.dataframe(pd.DataFrame(partition_info), use_container_width=True)
            else:
                st.error("Data Lake files not found despite pipeline execution. Please check the pipeline logs.")
        elif st.session_state.last_etl_run is not None:
            # ETL was run but not Data Lake
            st.info("**Current Session: Simple ETL Pipeline**")
            st.warning("Data Lake view is not available. You ran the Simple ETL Pipeline which creates standard JSON files, not the enhanced Data Lake structure.")
            
            st.markdown("### What you can do:")
            st.markdown("""
            - **View ETL Results**: Check the 'Data Explorer' and 'Analytics' tabs for your Simple ETL Pipeline results
            - **Try Data Lake**: Run the 'Enhanced Data Lake Pipeline' to see this Data Lake view
            - **Compare Both**: Run 'Both Pipelines' to see the full comparison
            """)
        else:
            # No pipelines run in current session
            st.info("**No Pipeline Executed Yet**")
            st.markdown("""
            ### Welcome to Data Lake Deep Dive!
            
            This tab shows detailed insights from the **Enhanced Data Lake Pipeline**.
            
            **To see Data Lake results:**
            1. Go to the sidebar
            2. Select 'Enhanced Data Lake Pipeline'
            3. Click 'Execute Pipeline'
            4. Return to this tab to explore the results
            
            **Data Lake Features:**
            - Multi-zone architecture (Bronze, Silver, Gold)
            - Parquet format for performance
            - Partitioned data storage
            - Advanced analytics ready
            """)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; margin-bottom: 20px;'>
        <h4 style='color: #4CAF50; margin-bottom: 10px;'>Developed by: Mir Hasibul Hasan Rahat</h4>
        <p style='margin: 5px 0;'><strong>Data Engineer | Python Developer | Analytics Specialist</strong></p>
        <p style='margin: 5px 0;'>Unified Data Engineering Platform | ETL Pipeline + Data Lake + Analytics</p>
        <p style='color: #999; font-size: 0.9em;'>Built with Streamlit, Pandas, Plotly & PyArrow</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()