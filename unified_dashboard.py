"""
ETL Pipeline Analytics Dashboard
Author: Mir Hasibul Hasan Rahat
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
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
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
    """Create pipeline status indicators"""
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Check data availability
    etl_status = "Available" if data['status']['etl_exists'] else "Not Available"
    lake_status = "Available" if data['status']['lake_exists'] else "Not Available"
    
    with col1:
        st.markdown(f'<div class="zone-indicator raw">Raw Zone {etl_status if data["status"]["etl_exists"] else "Not Available"}</div>', 
                   unsafe_allow_html=True)
    
    with col2:
        st.markdown(f'<div class="zone-indicator bronze">Bronze Layer {etl_status}</div>', 
                   unsafe_allow_html=True)
    
    with col3:
        st.markdown(f'<div class="zone-indicator silver">Silver Layer {etl_status}</div>', 
                   unsafe_allow_html=True)
    
    with col4:
        st.markdown(f'<div class="zone-indicator gold">Gold Layer {etl_status}</div>', 
                   unsafe_allow_html=True)

def run_pipeline_selector():
    """Pipeline execution options"""
    
    st.sidebar.markdown("### Pipeline Execution")
    
    pipeline_choice = st.sidebar.selectbox(
        "Choose Pipeline to Run:",
        ["Simple ETL Pipeline", "Enhanced Data Lake Pipeline", "Both Pipelines"]
    )
    
    if st.sidebar.button("Execute Pipeline", type="primary"):
        with st.spinner(f"Running {pipeline_choice}..."):
            try:
                success = False
                
                if pipeline_choice == "Simple ETL Pipeline":
                    result = subprocess.run(
                        ["python", "simple_local_pipeline.py"],
                        capture_output=True, text=True, cwd="."
                    )
                    success = result.returncode == 0
                    
                elif pipeline_choice == "Enhanced Data Lake Pipeline":
                    result = subprocess.run(
                        ["python", "enhanced_data_lake.py"],
                        capture_output=True, text=True, cwd="."
                    )
                    success = result.returncode == 0
                    
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
    **🔄 ETL Pipeline:**
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
                <h3>🔄 ETL Pipeline</h3>
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
        
        # Metrics
        etl_records = len(data['etl_pipeline'].get('bronze', []))
        lake_records = 0
        if 'bronze' in data['data_lake']:
            for dataset in data['data_lake']['bronze'].values():
                if isinstance(dataset, pd.DataFrame):
                    lake_records += len(dataset)
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("🔄 ETL Records", f"{etl_records:,}")
        col2.metric("Lake Records", f"{lake_records:,}")
        col3.metric("Active Pipelines", 
                   int(data['status']['etl_exists']) + int(data['status']['lake_exists']))
        col4.metric("Zones Available", 4)
        
        # Recent execution info
        if data['status']['etl_exists'] or data['status']['lake_exists']:
            st.markdown("""
            <div class="success-card">
                <h4>Platform Status: OPERATIONAL</h4>
                <p>All pipelines are ready for execution and data analysis.</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.warning("⚠️ No pipeline data found. Please run a pipeline to get started!")
    
    with tab3:
        st.markdown("## Unified Data Explorer")
        
        # Data source selector
        available_sources = []
        if data['status']['etl_exists']:
            available_sources.append("ETL Pipeline Data")
        if data['status']['lake_exists']:
            available_sources.append("Data Lake")
        
        if available_sources:
            selected_source = st.selectbox("Select Data Source:", available_sources)
            
            if selected_source == "ETL Pipeline Data" and data['etl_pipeline']:
                st.markdown("### 🔄 ETL Pipeline Data")
                
                for layer, layer_data in data['etl_pipeline'].items():
                    if layer_data:
                        st.markdown(f"#### {layer.title()} Layer")
                        if isinstance(layer_data, list):
                            df = pd.DataFrame(layer_data)
                            st.dataframe(df.head(10), use_container_width=True)
                        elif isinstance(layer_data, dict):
                            df = pd.DataFrame(layer_data)
                            st.dataframe(df, use_container_width=True)
            
            elif selected_source == "Data Lake" and data['data_lake']:
                st.markdown("### Data Lake Explorer")
                
                lake_zone = st.selectbox("Select Zone:", list(data['data_lake'].keys()))
                
                if lake_zone in data['data_lake']:
                    zone_data = data['data_lake'][lake_zone]
                    for dataset_name, dataset in zone_data.items():
                        st.markdown(f"#### {dataset_name.replace('_', ' ').title()}")
                        if isinstance(dataset, pd.DataFrame):
                            col1, col2 = st.columns(2)
                            col1.metric("Records", len(dataset))
                            col2.metric("Columns", len(dataset.columns))
                            st.dataframe(dataset.head(10), use_container_width=True)
        else:
            st.info("No data sources available. Run a pipeline first!")
    
    with tab4:
        st.markdown("## Unified Analytics Dashboard")
        
        analytics = create_unified_analytics(data)
        
        if analytics:
            # Compare ETL vs Data Lake results
            if 'etl_genre' in analytics and 'lake_genre' in analytics:
                st.markdown("### ETL vs Data Lake Comparison")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### 🔄 ETL Pipeline Results")
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
            
            elif 'etl_genre' in analytics:
                st.markdown("### 🔄 ETL Pipeline Analytics")
                etl_fig = px.bar(
                    analytics['etl_genre'], 
                    x='genre', y='total_revenue',
                    title="Revenue by Genre",
                    color='total_revenue'
                )
                st.plotly_chart(etl_fig, use_container_width=True)
                st.dataframe(analytics['etl_genre'], use_container_width=True)
            
            elif 'lake_genre' in analytics:
                st.markdown("### Data Lake Analytics")
                lake_fig = px.bar(
                    analytics['lake_genre'], 
                    x='genre', y='revenue',
                    title="Revenue by Genre",
                    color='revenue'
                )
                st.plotly_chart(lake_fig, use_container_width=True)
                st.dataframe(analytics['lake_genre'], use_container_width=True)
        else:
            st.info("No analytics data available. Run a pipeline to generate insights!")
    
    with tab5:
        st.markdown("## Data Lake Deep Dive")
        
        if data['status']['lake_exists']:
            # Data Lake specific features
            st.markdown("### Lake Zones Overview")
            
            for zone_name, zone_data in data['data_lake'].items():
                if zone_data:
                    with st.expander(f"{zone_name.title()} Zone Details"):
                        for dataset_name, dataset in zone_data.items():
                            if isinstance(dataset, pd.DataFrame):
                                st.write(f"**{dataset_name}:** {len(dataset)} records, {len(dataset.columns)} columns")
                                st.dataframe(dataset.head(3), use_container_width=True)
            
            # Partitioning info
            if os.path.exists("data/lake/silver/movies_enriched"):
                partitions = [d for d in os.listdir("data/lake/silver/movies_enriched") if d.startswith('decade=')]
                st.markdown(f"### Partitioning Strategy")
                st.info(f"Silver layer partitioned by decade: {', '.join(partitions)}")
        else:
            st.info("Data Lake not available. Run the Enhanced Data Lake Pipeline!")
    
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