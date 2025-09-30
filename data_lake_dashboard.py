"""
Data Lake Visualization Dashboard with Streamlit
Interactive exploration of multi-zone data lake architecture
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import json
from datetime import datetime
import pyarrow.parquet as pq

# Page configuration
st.set_page_config(
    page_title="Data Lake Explorer",
    page_icon="🏔️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Data Lake theme
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #2E86AB;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #2E86AB, #A23B72, #F18F01);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .zone-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .data-flow {
        background: linear-gradient(90deg, #74b9ff, #0984e3);
        padding: 0.5rem;
        border-radius: 5px;
        color: white;
        text-align: center;
        margin: 0.2rem;
    }
    .metrics-container {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #2E86AB;
    }
</style>
""", unsafe_allow_html=True)

def load_data_lake_data():
    """Load data from all data lake zones"""
    data_lake = {}
    base_path = "data/lake"
    
    try:
        # Raw zone data
        if os.path.exists(f"{base_path}/raw"):
            raw_data = {}
            if os.path.exists(f"{base_path}/raw/movies_api.json"):
                with open(f"{base_path}/raw/movies_api.json", 'r') as f:
                    raw_data['movies'] = json.load(f)
            
            if os.path.exists(f"{base_path}/raw/user_ratings.csv"):
                raw_data['ratings'] = pd.read_csv(f"{base_path}/raw/user_ratings.csv").to_dict('records')
            
            if os.path.exists(f"{base_path}/raw/box_office.parquet"):
                raw_data['box_office'] = pd.read_parquet(f"{base_path}/raw/box_office.parquet").to_dict('records')
            
            data_lake['raw'] = raw_data
        
        # Bronze zone data
        if os.path.exists(f"{base_path}/bronze"):
            bronze_data = {}
            for file in os.listdir(f"{base_path}/bronze"):
                if file.endswith('.parquet'):
                    table_name = file.replace('.parquet', '')
                    bronze_data[table_name] = pd.read_parquet(f"{base_path}/bronze/{file}")
            data_lake['bronze'] = bronze_data
        
        # Silver zone data
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
            data_lake['silver'] = silver_data
        
        # Gold zone data
        if os.path.exists(f"{base_path}/gold"):
            gold_data = {}
            for file in os.listdir(f"{base_path}/gold"):
                if file.endswith('.parquet'):
                    analysis_name = file.replace('.parquet', '')
                    gold_data[analysis_name] = pd.read_parquet(f"{base_path}/gold/{file}")
            data_lake['gold'] = gold_data
        
    except Exception as e:
        st.error(f"Error loading data lake: {e}")
        return None
    
    return data_lake

def get_data_lake_metrics(data_lake):
    """Calculate data lake metrics"""
    metrics = {
        'zones': 0,
        'datasets': 0,
        'total_records': 0,
        'formats': set(),
        'partitions': 0
    }
    
    for zone, data in data_lake.items():
        if data:
            metrics['zones'] += 1
            if isinstance(data, dict):
                for dataset_name, dataset in data.items():
                    metrics['datasets'] += 1
                    if isinstance(dataset, pd.DataFrame):
                        metrics['total_records'] += len(dataset)
                        metrics['formats'].add('Parquet')
                    elif isinstance(dataset, list):
                        metrics['total_records'] += len(dataset)
                        metrics['formats'].add('JSON')
    
    # Check for partitions
    if os.path.exists("data/lake/silver/movies_enriched"):
        metrics['partitions'] = len([d for d in os.listdir("data/lake/silver/movies_enriched") if d.startswith('decade=')])
    
    return metrics

def create_data_lake_architecture_diagram():
    """Create data lake architecture visualization"""
    
    fig = go.Figure()
    
    # Define zones with positions
    zones = [
        {'name': 'Raw Zone', 'x': 1, 'y': 4, 'color': '#FF6B6B', 'desc': 'Landing Area\nJSON, CSV, Parquet'},
        {'name': 'Bronze Zone', 'x': 2, 'y': 4, 'color': '#4ECDC4', 'desc': 'Standardized\nMetadata Added'},
        {'name': 'Silver Zone', 'x': 3, 'y': 4, 'color': '#45B7D1', 'desc': 'Enriched\nCleaned & Joined'},
        {'name': 'Gold Zone', 'x': 4, 'y': 4, 'color': '#96CEB4', 'desc': 'Curated\nBusiness Analytics'},
    ]
    
    # Add zone boxes
    for zone in zones:
        fig.add_shape(
            type="rect",
            x0=zone['x']-0.4, y0=zone['y']-0.3,
            x1=zone['x']+0.4, y1=zone['y']+0.3,
            fillcolor=zone['color'],
            opacity=0.7,
            line=dict(color=zone['color'], width=2)
        )
        
        fig.add_annotation(
            x=zone['x'], y=zone['y']+0.1,
            text=f"<b>{zone['name']}</b>",
            showarrow=False,
            font=dict(color="white", size=12)
        )
        
        fig.add_annotation(
            x=zone['x'], y=zone['y']-0.1,
            text=zone['desc'],
            showarrow=False,
            font=dict(color="white", size=9)
        )
    
    # Add flow arrows
    for i in range(len(zones)-1):
        fig.add_annotation(
            x=zones[i]['x']+0.5, y=zones[i]['y'],
            ax=zones[i+1]['x']-0.5, ay=zones[i+1]['y'],
            arrowhead=2, arrowsize=1, arrowwidth=2,
            arrowcolor="#2C3E50"
        )
    
    # Add data sources
    sources = [
        {'name': 'API Data\n(JSON)', 'x': 0.2, 'y': 4.5},
        {'name': 'CSV Files', 'x': 0.2, 'y': 4},
        {'name': 'Parquet\nFiles', 'x': 0.2, 'y': 3.5}
    ]
    
    for source in sources:
        fig.add_shape(
            type="rect",
            x0=source['x']-0.15, y0=source['y']-0.15,
            x1=source['x']+0.15, y1=source['y']+0.15,
            fillcolor="#34495E",
            opacity=0.8
        )
        
        fig.add_annotation(
            x=source['x'], y=source['y'],
            text=source['name'],
            showarrow=False,
            font=dict(color="white", size=8)
        )
        
        # Arrow to Raw zone
        fig.add_annotation(
            x=source['x']+0.2, y=source['y'],
            ax=0.6, ay=4,
            arrowhead=2, arrowsize=1, arrowwidth=1,
            arrowcolor="#7F8C8D"
        )
    
    fig.update_layout(
        title="Data Lake Architecture Flow",
        showlegend=False,
        xaxis=dict(range=[0, 5], showgrid=False, showticklabels=False),
        yaxis=dict(range=[3, 5], showgrid=False, showticklabels=False),
        plot_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    return fig

def create_zone_comparison_chart(data_lake):
    """Create comparison chart of data across zones"""
    
    zone_stats = []
    
    for zone_name, zone_data in data_lake.items():
        if zone_data and isinstance(zone_data, dict):
            for dataset_name, dataset in zone_data.items():
                if isinstance(dataset, pd.DataFrame):
                    zone_stats.append({
                        'Zone': zone_name.title(),
                        'Dataset': dataset_name,
                        'Records': len(dataset),
                        'Columns': len(dataset.columns)
                    })
                elif isinstance(dataset, list):
                    zone_stats.append({
                        'Zone': zone_name.title(),
                        'Dataset': dataset_name, 
                        'Records': len(dataset),
                        'Columns': len(dataset[0].keys()) if dataset else 0
                    })
    
    if not zone_stats:
        return None
    
    df_stats = pd.DataFrame(zone_stats)
    
    fig = px.bar(
        df_stats, 
        x='Zone', 
        y='Records',
        color='Dataset',
        title="Data Volume Across Lake Zones",
        labels={'Records': 'Number of Records'},
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_layout(height=400)
    return fig

def create_data_lineage_flow():
    """Create data lineage visualization"""
    
    fig = go.Figure()
    
    # Define the flow
    flow_data = [
        {'source': 'Movies API', 'target': 'Raw JSON', 'value': 5},
        {'source': 'User Systems', 'target': 'Raw CSV', 'value': 5},
        {'source': 'Box Office', 'target': 'Raw Parquet', 'value': 5},
        {'source': 'Raw JSON', 'target': 'Bronze Movies', 'value': 5},
        {'source': 'Raw CSV', 'target': 'Bronze Ratings', 'value': 5},
        {'source': 'Raw Parquet', 'target': 'Bronze Box Office', 'value': 5},
        {'source': 'Bronze Movies', 'target': 'Silver Enriched', 'value': 5},
        {'source': 'Bronze Ratings', 'target': 'Silver Enriched', 'value': 5},
        {'source': 'Bronze Box Office', 'target': 'Silver Enriched', 'value': 5},
        {'source': 'Silver Enriched', 'target': 'Gold Analytics', 'value': 15},
    ]
    
    # Create nodes
    all_nodes = list(set([item['source'] for item in flow_data] + [item['target'] for item in flow_data]))
    
    # Create sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node = dict(
            pad = 15,
            thickness = 20,
            line = dict(color = "black", width = 0.5),
            label = all_nodes,
            color = ["#FF6B6B", "#FF6B6B", "#FF6B6B", "#4ECDC4", "#4ECDC4", "#4ECDC4", "#45B7D1", "#96CEB4"]
        ),
        link = dict(
            source = [all_nodes.index(item['source']) for item in flow_data],
            target = [all_nodes.index(item['target']) for item in flow_data], 
            value = [item['value'] for item in flow_data]
        )
    )])
    
    fig.update_layout(
        title_text="Data Lineage Flow Through Lake Zones", 
        font_size=10,
        height=500
    )
    
    return fig

def run_data_lake_pipeline():
    """Button to run the data lake pipeline"""
    if st.button("🏔️ Run Data Lake Pipeline", type="primary"):
        with st.spinner("Processing Data Lake Pipeline..."):
            try:
                import subprocess
                result = subprocess.run(
                    ["python", "enhanced_data_lake.py"],
                    capture_output=True,
                    text=True,
                    cwd="."
                )
                
                if result.returncode == 0:
                    st.success("✅ Data Lake Pipeline completed successfully!")
                    st.code(result.stdout, language="text")
                    st.rerun()  # Refresh the data
                else:
                    st.error("❌ Pipeline failed!")
                    st.code(result.stderr, language="text")
                    
            except Exception as e:
                st.error(f"Error running pipeline: {e}")

def main():
    """Main Data Lake Dashboard"""
    
    # Header
    st.markdown('<h1 class="main-header">🏔️ Data Lake Explorer</h1>', unsafe_allow_html=True)
    st.markdown("**Multi-Zone Data Lake Architecture Visualization**")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.title("🎛️ Data Lake Controls")
    st.sidebar.markdown("### Pipeline Management")
    run_data_lake_pipeline()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Data Lake Info")
    st.sidebar.info("""
    **Architecture:** Raw → Bronze → Silver → Gold
    
    **Raw Zone:** Landing area for multiple formats  
    **Bronze Zone:** Standardized with metadata  
    **Silver Zone:** Enriched and partitioned  
    **Gold Zone:** Business-ready analytics  
    
    **Features:**
    - Multi-format support (JSON, CSV, Parquet)
    - Partition-based optimization  
    - Schema evolution
    - Data lineage tracking
    """)
    
    # Load data
    data_lake = load_data_lake_data()
    
    if not data_lake or not any(data_lake.values()):
        st.warning("⚠️ No data lake found. Please run the Data Lake Pipeline first!")
        st.markdown("### How to Create Data Lake:")
        st.code("python enhanced_data_lake.py", language="bash")
        return
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🏗️ Architecture", "📊 Zone Explorer", "🔄 Data Flow", "📈 Analytics", "🗂️ Metadata"])
    
    with tab1:
        st.markdown("## 🏗️ Data Lake Architecture")
        
        # Key metrics
        metrics = get_data_lake_metrics(data_lake)
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("🏔️ Active Zones", metrics['zones'])
        col2.metric("📊 Datasets", metrics['datasets'])
        col3.metric("📝 Total Records", f"{metrics['total_records']:,}")
        col4.metric("🗂️ Partitions", metrics['partitions'])
        
        # Architecture diagram
        arch_fig = create_data_lake_architecture_diagram()
        st.plotly_chart(arch_fig, use_container_width=True)
        
        # Zone comparison
        zone_fig = create_zone_comparison_chart(data_lake)
        if zone_fig:
            st.plotly_chart(zone_fig, use_container_width=True)
    
    with tab2:
        st.markdown("## 📊 Zone Explorer")
        
        # Zone selector
        available_zones = [zone for zone in data_lake.keys() if data_lake[zone]]
        selected_zone = st.selectbox("Select Data Lake Zone:", available_zones)
        
        if selected_zone and data_lake[selected_zone]:
            zone_data = data_lake[selected_zone]
            
            st.markdown(f'<div class="zone-card"><h3>📁 {selected_zone.title()} Zone</h3></div>', 
                       unsafe_allow_html=True)
            
            # Show datasets in zone
            for dataset_name, dataset in zone_data.items():
                st.markdown(f"### 📋 {dataset_name.replace('_', ' ').title()}")
                
                if isinstance(dataset, pd.DataFrame):
                    col1, col2 = st.columns(2)
                    col1.metric("Records", len(dataset))
                    col2.metric("Columns", len(dataset.columns))
                    
                    # Show data preview
                    st.markdown("**Data Preview:**")
                    st.dataframe(dataset.head(10), use_container_width=True)
                    
                    # Show column info
                    if st.expander(f"Column Information - {dataset_name}"):
                        col_info = pd.DataFrame({
                            'Column': dataset.columns,
                            'Type': [str(dataset[col].dtype) for col in dataset.columns],
                            'Non-Null': [dataset[col].count() for col in dataset.columns],
                            'Null %': [f"{(dataset[col].isnull().sum()/len(dataset)*100):.1f}%" for col in dataset.columns]
                        })
                        st.dataframe(col_info, use_container_width=True)
                
                elif isinstance(dataset, list) and dataset:
                    st.metric("Records", len(dataset))
                    st.json(dataset[0])  # Show first record
    
    with tab3:
        st.markdown("## 🔄 Data Lineage & Flow")
        
        # Data lineage diagram
        lineage_fig = create_data_lineage_flow() 
        st.plotly_chart(lineage_fig, use_container_width=True)
        
        # Flow summary
        st.markdown("### 📋 Processing Flow Summary")
        
        flow_steps = [
            "🔄 **Ingestion:** Multiple data sources → Raw Zone (JSON, CSV, Parquet)",
            "🔄 **Standardization:** Raw Zone → Bronze Zone (Add metadata, standardize)",
            "🔄 **Enrichment:** Bronze Zone → Silver Zone (Join datasets, add business logic)",
            "🔄 **Analytics:** Silver Zone → Gold Zone (Create business aggregations)"
        ]
        
        for step in flow_steps:
            st.markdown(f'<div class="data-flow">{step}</div>', unsafe_allow_html=True)
    
    with tab4:
        st.markdown("## 📈 Business Analytics from Data Lake")
        
        if 'gold' in data_lake and data_lake['gold']:
            gold_data = data_lake['gold']
            
            # Genre performance
            if 'genre_performance' in gold_data:
                st.markdown("### 🎭 Genre Performance Analysis")
                genre_df = gold_data['genre_performance'].reset_index()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_revenue = px.bar(
                        genre_df, x='genre', y='revenue',
                        title="Revenue by Genre",
                        color='revenue',
                        color_continuous_scale='viridis'
                    )
                    st.plotly_chart(fig_revenue, use_container_width=True)
                
                with col2:
                    fig_roi = px.bar(
                        genre_df, x='genre', y='roi',
                        title="ROI by Genre", 
                        color='roi',
                        color_continuous_scale='plasma'
                    )
                    st.plotly_chart(fig_roi, use_container_width=True)
                
                st.dataframe(genre_df, use_container_width=True)
            
            # Decade trends  
            if 'decade_trends' in gold_data:
                st.markdown("### 📅 Decade Trends Analysis")
                decade_df = gold_data['decade_trends'].reset_index()
                
                fig_trends = px.line(
                    decade_df, x='decade', y=['revenue', 'budget'],
                    title="Revenue vs Budget Trends by Decade",
                    labels={'value': 'Amount ($)', 'variable': 'Metric'}
                )
                st.plotly_chart(fig_trends, use_container_width=True)
                
                st.dataframe(decade_df, use_container_width=True)
            
            # Top performers
            if 'top_performers' in gold_data:
                st.markdown("### 🏆 Top Performing Movies")
                top_df = gold_data['top_performers'].reset_index(drop=True)
                st.dataframe(top_df, use_container_width=True)
        
        else:
            st.warning("No Gold zone analytics available. Run the pipeline to generate business insights.")
    
    with tab5:
        st.markdown("## 🗂️ Data Lake Metadata")
        
        # File system overview
        st.markdown("### 📁 File System Structure")
        
        def show_directory_tree(path, prefix=""):
            items = []
            if os.path.exists(path):
                for item in sorted(os.listdir(path)):
                    item_path = os.path.join(path, item)
                    if os.path.isdir(item_path):
                        items.append(f"{prefix}📁 {item}/")
                        items.extend(show_directory_tree(item_path, prefix + "  "))
                    else:
                        size = os.path.getsize(item_path)
                        items.append(f"{prefix}📄 {item} ({size:,} bytes)")
            return items
        
        if os.path.exists("data/lake"):
            tree_items = show_directory_tree("data/lake")
            for item in tree_items[:20]:  # Limit display
                st.text(item)
        
        # Data formats summary
        st.markdown("### 📊 Data Formats Summary")
        
        format_info = pd.DataFrame([
            {"Zone": "Raw", "Format": "JSON", "Purpose": "API responses", "Compression": "None"},
            {"Zone": "Raw", "Format": "CSV", "Purpose": "User data", "Compression": "None"},
            {"Zone": "Raw", "Format": "Parquet", "Purpose": "Analytics data", "Compression": "Snappy"},
            {"Zone": "Bronze", "Format": "Parquet", "Purpose": "Standardized storage", "Compression": "Snappy"},
            {"Zone": "Silver", "Format": "Parquet", "Purpose": "Enriched data", "Compression": "Snappy"},
            {"Zone": "Gold", "Format": "Parquet", "Purpose": "Analytics tables", "Compression": "Snappy"},
        ])
        
        st.dataframe(format_info, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        🏔️ Data Lake Explorer | Multi-Zone Architecture Visualization | 
        Built with Streamlit & Plotly
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()