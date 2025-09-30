"""
Streamlit Dashboard for ETL Pipeline Results
Interactive web visualization of Bronze, Silver, and Gold layer data
"""

import streamlit as st
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# Page configuration
st.set_page_config(
    page_title="ETL Pipeline Dashboard",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .layer-header {
        color: #ff7f0e;
        border-bottom: 2px solid #ff7f0e;
        padding-bottom: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

def load_pipeline_data():
    """Load data from all pipeline layers"""
    data = {}
    
    try:
        # Load Bronze data
        if os.path.exists("data/bronze/movies.json"):
            with open("data/bronze/movies.json", 'r') as f:
                data['bronze'] = json.load(f)
        
        # Load Silver data
        if os.path.exists("data/silver/movies_clean.json"):
            with open("data/silver/movies_clean.json", 'r') as f:
                data['silver'] = json.load(f)
        
        # Load Gold analytics
        if os.path.exists("data/gold/genre_analysis.json"):
            with open("data/gold/genre_analysis.json", 'r') as f:
                data['genre_analysis'] = json.load(f)
        
        if os.path.exists("data/gold/era_analysis.json"):
            with open("data/gold/era_analysis.json", 'r') as f:
                data['era_analysis'] = json.load(f)
                
        if os.path.exists("data/gold/top_movies.json"):
            with open("data/gold/top_movies.json", 'r') as f:
                data['top_movies'] = json.load(f)
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None
    
    return data

def create_genre_chart(genre_data):
    """Create genre performance chart"""
    genres = list(genre_data['movie_count'].keys())
    revenues = list(genre_data['total_revenue'].values())
    
    fig = px.bar(
        x=genres,
        y=revenues,
        title="📊 Total Revenue by Genre",
        labels={'x': 'Genre', 'y': 'Total Revenue ($)'},
        color=revenues,
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(
        title_font_size=20,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14,
        showlegend=False
    )
    
    return fig

def create_era_chart(era_data):
    """Create era analysis chart"""
    eras = list(era_data['movie_count'].keys())
    ratings = list(era_data['avg_rating'].values())
    revenues = list(era_data['avg_revenue'].values())
    
    fig = go.Figure()
    
    # Add rating line
    fig.add_trace(go.Scatter(
        x=eras,
        y=ratings,
        mode='lines+markers',
        name='Average Rating',
        yaxis='y',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=10)
    ))
    
    # Add revenue bars
    fig.add_trace(go.Bar(
        x=eras,
        y=revenues,
        name='Average Revenue',
        yaxis='y2',
        opacity=0.7,
        marker_color='#ff7f0e'
    ))
    
    fig.update_layout(
        title="📈 Movie Performance by Era",
        xaxis_title="Era",
        yaxis=dict(title="Average Rating", side="left"),
        yaxis2=dict(title="Average Revenue ($)", side="right", overlaying="y"),
        legend=dict(x=0.01, y=0.99),
        title_font_size=20
    )
    
    return fig

def create_top_movies_chart(top_movies_data):
    """Create top movies chart"""
    titles = [f"{title} ({year})" for title, year in zip(top_movies_data['title'].values(), top_movies_data['year'].values())]
    revenues = list(top_movies_data['revenue'].values())
    
    fig = px.bar(
        x=revenues,
        y=titles,
        title="🏆 Top Movies by Revenue",
        labels={'x': 'Revenue ($)', 'y': 'Movie'},
        orientation='h',
        color=revenues,
        color_continuous_scale='plasma'
    )
    
    fig.update_layout(
        title_font_size=20,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14,
        height=400,
        showlegend=False
    )
    
    return fig

def run_pipeline_button():
    """Button to run the ETL pipeline"""
    if st.button("🚀 Run ETL Pipeline", type="primary"):
        with st.spinner("Running ETL Pipeline..."):
            try:
                # Import and run the pipeline
                import subprocess
                result = subprocess.run(
                    ["python", "simple_local_pipeline.py"],
                    capture_output=True,
                    text=True,
                    cwd="."
                )
                
                if result.returncode == 0:
                    st.success("✅ ETL Pipeline completed successfully!")
                    st.code(result.stdout, language="text")
                    st.rerun()  # Refresh the data
                else:
                    st.error("❌ Pipeline failed!")
                    st.code(result.stderr, language="text")
                    
            except Exception as e:
                st.error(f"Error running pipeline: {e}")

def main():
    """Main dashboard function"""
    
    # Header
    st.markdown('<h1 class="main-header">🎬 ETL Pipeline Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Sidebar
    st.sidebar.title("🎛️ Pipeline Controls")
    st.sidebar.markdown("### Run Pipeline")
    run_pipeline_button()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Pipeline Info")
    st.sidebar.info("""
    **Architecture:** Bronze → Silver → Gold
    
    **Bronze:** Raw data ingestion  
    **Silver:** Data cleaning & validation  
    **Gold:** Business analytics  
    """)
    
    # Load data
    data = load_pipeline_data()
    
    if not data:
        st.warning("⚠️ No pipeline data found. Please run the ETL pipeline first!")
        st.markdown("### How to Run:")
        st.code("python simple_local_pipeline.py", language="bash")
        return
    
    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🥉 Bronze", "🥈 Silver", "🥇 Gold"])
    
    with tab1:
        st.markdown("## 📈 Pipeline Overview")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        if 'bronze' in data:
            col1.metric("🥉 Bronze Records", len(data['bronze']))
        
        if 'silver' in data:
            col2.metric("🥈 Silver Records", len(data['silver']))
        
        if 'genre_analysis' in data:
            total_revenue = sum(data['genre_analysis']['total_revenue'].values())
            col3.metric("💰 Total Revenue", f"${total_revenue:,.0f}")
        
        if 'top_movies' in data:
            col4.metric("🏆 Top Movies", len(data['top_movies']['title']))
        
        st.markdown("---")
        
        # Overview charts
        if 'genre_analysis' in data and 'era_analysis' in data:
            col1, col2 = st.columns(2)
            
            with col1:
                genre_fig = create_genre_chart(data['genre_analysis'])
                st.plotly_chart(genre_fig, use_container_width=True)
            
            with col2:
                era_fig = create_era_chart(data['era_analysis'])
                st.plotly_chart(era_fig, use_container_width=True)
        
        if 'top_movies' in data:
            top_movies_fig = create_top_movies_chart(data['top_movies'])
            st.plotly_chart(top_movies_fig, use_container_width=True)
    
    with tab2:
        st.markdown('<h2 class="layer-header">🥉 Bronze Layer - Raw Data</h2>', unsafe_allow_html=True)
        
        if 'bronze' in data:
            st.markdown(f"**Records:** {len(data['bronze'])}")
            
            # Show sample data
            bronze_df = pd.DataFrame(data['bronze'])
            st.markdown("### Sample Records")
            st.dataframe(bronze_df.head(10), use_container_width=True)
            
            # Data info
            st.markdown("### Data Schema")
            schema_info = {
                'Column': bronze_df.columns.tolist(),
                'Type': [str(bronze_df[col].dtype) for col in bronze_df.columns],
                'Sample': [str(bronze_df[col].iloc[0]) if len(bronze_df) > 0 else 'N/A' for col in bronze_df.columns]
            }
            st.dataframe(pd.DataFrame(schema_info), use_container_width=True)
        else:
            st.warning("No Bronze data available")
    
    with tab3:
        st.markdown('<h2 class="layer-header">🥈 Silver Layer - Cleaned Data</h2>', unsafe_allow_html=True)
        
        if 'silver' in data:
            silver_df = pd.DataFrame(data['silver'])
            st.markdown(f"**Records:** {len(silver_df)}")
            
            # Show cleaned data
            st.markdown("### Cleaned Records")
            st.dataframe(silver_df, use_container_width=True)
            
            # Data quality metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("✅ Valid Records", len(silver_df))
            col2.metric("📊 Eras Identified", silver_df['era'].nunique())
            col3.metric("🎭 Genres", silver_df['genre'].nunique())
            
            # Distribution charts
            col1, col2 = st.columns(2)
            
            with col1:
                era_dist = silver_df['era'].value_counts()
                fig_era = px.pie(values=era_dist.values, names=era_dist.index, title="Distribution by Era")
                st.plotly_chart(fig_era, use_container_width=True)
            
            with col2:
                genre_dist = silver_df['genre'].value_counts()
                fig_genre = px.pie(values=genre_dist.values, names=genre_dist.index, title="Distribution by Genre")
                st.plotly_chart(fig_genre, use_container_width=True)
        else:
            st.warning("No Silver data available")
    
    with tab4:
        st.markdown('<h2 class="layer-header">🥇 Gold Layer - Business Analytics</h2>', unsafe_allow_html=True)
        
        if 'genre_analysis' in data:
            st.markdown("### 🎭 Genre Performance Analysis")
            genre_df = pd.DataFrame(data['genre_analysis'])
            st.dataframe(genre_df, use_container_width=True)
            
            st.markdown("---")
        
        if 'era_analysis' in data:
            st.markdown("### 🕰️ Era Analysis")
            era_df = pd.DataFrame(data['era_analysis'])
            st.dataframe(era_df, use_container_width=True)
            
            st.markdown("---")
        
        if 'top_movies' in data:
            st.markdown("### 🏆 Top Performing Movies")
            top_movies_df = pd.DataFrame(data['top_movies'])
            st.dataframe(top_movies_df, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        🚀 ETL Pipeline Dashboard | Built with Streamlit | 
        Data Engineering Portfolio Project
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()