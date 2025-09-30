# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Movie Analytics Dashboard
# MAGIC 
# MAGIC **Interactive Visualizations & Business Intelligence**
# MAGIC 
# MAGIC This notebook creates comprehensive visualizations of the movie analytics data from our Gold layer, providing executive-level insights and detailed performance analysis.
# MAGIC 
# MAGIC **Contents:**
# MAGIC - Executive KPI Dashboard
# MAGIC - Genre Performance Analysis  
# MAGIC - Financial Performance Trends
# MAGIC - Era-based Historical Analysis
# MAGIC - Interactive Movie Explorer
# MAGIC - Predictive Analytics Visualizations

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

# Import visualization libraries
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.figure_factory as ff
import pandas as pd
import numpy as np
from datetime import datetime

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure visualization settings
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MovieAnalytics-Visualization") \
    .getOrCreate()

print("📊 Visualization environment initialized!")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Load Gold Layer Data

# COMMAND ----------

# Configuration
STORAGE_ACCOUNT = "your_storage_account"  
CONTAINER_NAME = "datalake"
GOLD_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/"

# Load all Gold layer tables
try:
    # Main movie performance data
    movies_df = spark.read.format("delta").load(f"{GOLD_PATH}movie_performance_summary")
    
    # Analytics tables
    genre_analytics_df = spark.read.format("delta").load(f"{GOLD_PATH}genre_analytics")
    era_analytics_df = spark.read.format("delta").load(f"{GOLD_PATH}era_analytics") 
    financial_kpis_df = spark.read.format("delta").load(f"{GOLD_PATH}financial_kpis")
    
    # Convert to Pandas for visualization
    movies_pd = movies_df.toPandas()
    genre_analytics_pd = genre_analytics_df.toPandas()
    era_analytics_pd = era_analytics_df.toPandas()
    financial_kpis_pd = financial_kpis_df.toPandas()
    
    print("✅ Gold layer data loaded successfully!")
    print(f"📈 Movies dataset: {len(movies_pd)} records")
    print(f"📊 Genre analytics: {len(genre_analytics_pd)} categories")
    print(f"📅 Era analytics: {len(era_analytics_pd)} time periods")
    
except Exception as e:
    print(f"⚠️ Using sample data for demonstration: {str(e)}")
    
    # Create sample data for visualization demo
    np.random.seed(42)
    movies_pd = pd.DataFrame({
        'title': [f'Movie {i}' for i in range(50)],
        'year': np.random.randint(1970, 2023, 50),
        'genre': np.random.choice(['Action', 'Drama', 'Comedy', 'Sci-Fi', 'Horror'], 50),
        'rating': np.random.uniform(5.0, 9.5, 50),
        'budget': np.random.randint(1000000, 300000000, 50),
        'box_office': np.random.randint(5000000, 2000000000, 50),
        'profit': np.random.randint(-50000000, 1500000000, 50),
        'roi': np.random.uniform(-50, 1000, 50),
        'performance_score': np.random.uniform(1, 10, 50),
        'movie_era': np.random.choice(['Classic Era', 'Modern Era', 'Digital Era', 'Contemporary Era'], 50),
        'popularity_tier': np.random.choice(['Most Popular', 'Popular', 'Moderate', 'Low', 'Niche'], 50),
        'is_blockbuster': np.random.choice([True, False], 50, p=[0.2, 0.8]),
        'is_profitable': np.random.choice([True, False], 50, p=[0.7, 0.3])
    })
    
    # Create analytics data
    genre_analytics_pd = movies_pd.groupby('genre').agg({
        'title': 'count',
        'rating': 'mean',
        'budget': 'mean',
        'box_office': 'mean',
        'roi': 'mean'
    }).reset_index()
    genre_analytics_pd.columns = ['genre', 'total_movies', 'avg_rating', 'avg_budget', 'avg_box_office', 'avg_roi']

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Executive KPI Dashboard

# COMMAND ----------

# Create executive dashboard with key metrics
def create_executive_dashboard():
    """Create high-level KPI dashboard"""
    
    # Calculate key metrics
    total_movies = len(movies_pd)
    total_revenue = movies_pd['box_office'].sum()
    total_budget = movies_pd['budget'].sum()
    total_profit = movies_pd['profit'].sum()
    avg_roi = movies_pd['roi'].mean()
    blockbuster_rate = (movies_pd['is_blockbuster'].sum() / total_movies) * 100
    profitability_rate = (movies_pd['is_profitable'].sum() / total_movies) * 100
    avg_rating = movies_pd['rating'].mean()
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=4,
        subplot_titles=('Total Revenue', 'Profitability Rate', 'Average ROI', 'Blockbuster Rate',
                       'Movies by Genre', 'Rating Distribution', 'Era Performance', 'Budget vs Revenue'),
        specs=[[{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}],
               [{"type": "bar"}, {"type": "histogram"}, {"type": "bar"}, {"type": "scatter"}]]
    )
    
    # KPI Indicators
    fig.add_trace(go.Indicator(
        mode = "number+delta+gauge",
        value = total_revenue/1e9,
        title = {"text": "Total Revenue (B$)"},
        number = {"suffix": "B", "font": {"size": 40}},
        gauge = {"axis": {"range": [0, 100]}, "bar": {"color": "darkgreen"}},
        domain = {'row': 0, 'column': 0}
    ), row=1, col=1)
    
    fig.add_trace(go.Indicator(
        mode = "number+gauge",
        value = profitability_rate,
        title = {"text": "Profitability Rate (%)"},
        number = {"suffix": "%", "font": {"size": 40}},
        gauge = {"axis": {"range": [0, 100]}, "bar": {"color": "blue"}},
    ), row=1, col=2)
    
    fig.add_trace(go.Indicator(
        mode = "number+gauge",
        value = avg_roi,
        title = {"text": "Average ROI (%)"},
        number = {"suffix": "%", "font": {"size": 40}},
        gauge = {"axis": {"range": [0, 500]}, "bar": {"color": "orange"}},
    ), row=1, col=3)
    
    fig.add_trace(go.Indicator(
        mode = "number+gauge",
        value = blockbuster_rate,
        title = {"text": "Blockbuster Rate (%)"},
        number = {"suffix": "%", "font": {"size": 40}},
        gauge = {"axis": {"range": [0, 50]}, "bar": {"color": "red"}},
    ), row=1, col=4)
    
    # Genre distribution
    genre_counts = movies_pd['genre'].value_counts()
    fig.add_trace(go.Bar(
        x=genre_counts.index,
        y=genre_counts.values,
        name="Movies by Genre",
        marker_color='lightblue'
    ), row=2, col=1)
    
    # Rating distribution
    fig.add_trace(go.Histogram(
        x=movies_pd['rating'],
        nbinsx=20,
        name="Rating Distribution",
        marker_color='lightgreen'
    ), row=2, col=2)
    
    # Era performance
    era_profit = movies_pd.groupby('movie_era')['profit'].mean()
    fig.add_trace(go.Bar(
        x=era_profit.index,
        y=era_profit.values,
        name="Average Profit by Era",
        marker_color='gold'
    ), row=2, col=3)
    
    # Budget vs Revenue scatter
    fig.add_trace(go.Scatter(
        x=movies_pd['budget'],
        y=movies_pd['box_office'],
        mode='markers',
        text=movies_pd['title'],
        name="Budget vs Revenue",
        marker=dict(
            size=movies_pd['rating']*2,
            color=movies_pd['performance_score'],
            colorscale='Viridis',
            showscale=True
        )
    ), row=2, col=4)
    
    fig.update_layout(
        height=800,
        title_text="🎬 Movie Industry Executive Dashboard",
        title_x=0.5,
        showlegend=False
    )
    
    return fig

# Display executive dashboard
executive_fig = create_executive_dashboard()
executive_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎭 Genre Performance Analysis

# COMMAND ----------

# Create comprehensive genre analysis
fig_genre = make_subplots(
    rows=2, cols=2,
    subplot_titles=('Average Rating by Genre', 'ROI Performance', 
                   'Budget Efficiency', 'Genre Market Share'),
    specs=[[{"secondary_y": True}, {"type": "box"}],
           [{"type": "scatter"}, {"type": "pie"}]]
)

# Average rating and movie count by genre
genre_stats = movies_pd.groupby('genre').agg({
    'rating': 'mean',
    'title': 'count',
    'roi': 'mean',
    'budget': 'mean',
    'box_office': 'mean'
}).reset_index()

# Rating by genre with movie count
fig_genre.add_trace(go.Bar(
    x=genre_stats['genre'],
    y=genre_stats['rating'],
    name="Average Rating",
    marker_color='lightcoral',
    yaxis='y'
), row=1, col=1)

fig_genre.add_trace(go.Scatter(
    x=genre_stats['genre'],
    y=genre_stats['title'],
    mode='markers+lines',
    name="Movie Count",
    marker=dict(size=10, color='darkblue'),
    yaxis='y2'
), row=1, col=1, secondary_y=True)

# ROI distribution by genre (box plot)
for genre in movies_pd['genre'].unique():
    genre_data = movies_pd[movies_pd['genre'] == genre]
    fig_genre.add_trace(go.Box(
        y=genre_data['roi'],
        name=genre,
        boxpoints='outliers'
    ), row=1, col=2)

# Budget efficiency (Revenue per dollar spent)
genre_stats['efficiency'] = genre_stats['box_office'] / genre_stats['budget']
fig_genre.add_trace(go.Scatter(
    x=genre_stats['budget'],
    y=genre_stats['box_office'],
    mode='markers+text',
    text=genre_stats['genre'],
    textposition="top center",
    marker=dict(
        size=genre_stats['efficiency']*5,
        color=genre_stats['rating'],
        colorscale='Plasma',
        showscale=True,
        colorbar=dict(title="Avg Rating")
    ),
    name="Budget vs Revenue"
), row=2, col=1)

# Market share pie chart
genre_revenue = movies_pd.groupby('genre')['box_office'].sum()
fig_genre.add_trace(go.Pie(
    labels=genre_revenue.index,
    values=genre_revenue.values,
    name="Market Share"
), row=2, col=2)

fig_genre.update_layout(height=800, title_text="🎭 Genre Performance Deep Dive")
fig_genre.update_xaxes(title_text="Genre", row=1, col=1)
fig_genre.update_yaxes(title_text="Average Rating", row=1, col=1)
fig_genre.update_yaxes(title_text="Movie Count", secondary_y=True, row=1, col=1)
fig_genre.update_xaxes(title_text="Budget ($)", row=2, col=1)
fig_genre.update_yaxes(title_text="Box Office ($)", row=2, col=1)

fig_genre.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Financial Performance Trends

# COMMAND ----------

# Create financial performance analysis
def create_financial_dashboard():
    """Create detailed financial performance visualizations"""
    
    fig_financial = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Profit vs Investment', 'ROI Distribution', 
                       'Performance Score Analysis', 'Risk-Return Matrix'),
        specs=[[{"type": "scatter"}, {"type": "histogram"}],
               [{"type": "box"}, {"type": "scatter"}]]
    )
    
    # Profit vs Investment scatter
    fig_financial.add_trace(go.Scatter(
        x=movies_pd['budget'],
        y=movies_pd['profit'],
        mode='markers',
        text=movies_pd['title'],
        marker=dict(
            size=8,
            color=movies_pd['rating'],
            colorscale='RdYlBu',
            showscale=True,
            colorbar=dict(title="Rating", x=0.47)
        ),
        name="Movies"
    ), row=1, col=1)
    
    # Add break-even line
    max_budget = movies_pd['budget'].max()
    fig_financial.add_trace(go.Scatter(
        x=[0, max_budget],
        y=[0, 0],
        mode='lines',
        line=dict(color='red', dash='dash'),
        name="Break-even"
    ), row=1, col=1)
    
    # ROI distribution
    fig_financial.add_trace(go.Histogram(
        x=movies_pd['roi'],
        nbinsx=30,
        name="ROI Distribution",
        marker_color='lightgreen',
        opacity=0.7
    ), row=1, col=2)
    
    # Performance score by genre
    for genre in movies_pd['genre'].unique():
        genre_data = movies_pd[movies_pd['genre'] == genre]
        fig_financial.add_trace(go.Box(
            y=genre_data['performance_score'],
            name=genre,
            boxpoints='outliers'
        ), row=2, col=1)
    
    # Risk-Return matrix (Budget as risk proxy)
    fig_financial.add_trace(go.Scatter(
        x=movies_pd['budget'],  # Risk proxy
        y=movies_pd['roi'],     # Return
        mode='markers',
        text=movies_pd['title'],
        marker=dict(
            size=movies_pd['performance_score']*3,
            color=movies_pd['genre'].astype('category').cat.codes,
            colorscale='Set1',
            showscale=False
        ),
        name="Risk-Return"
    ), row=2, col=2)
    
    # Update layout
    fig_financial.update_xaxes(title_text="Budget ($)", row=1, col=1)
    fig_financial.update_yaxes(title_text="Profit ($)", row=1, col=1)
    fig_financial.update_xaxes(title_text="ROI (%)", row=1, col=2)
    fig_financial.update_yaxes(title_text="Frequency", row=1, col=2)
    fig_financial.update_yaxes(title_text="Performance Score", row=2, col=1)
    fig_financial.update_xaxes(title_text="Budget (Risk Proxy) ($)", row=2, col=2)
    fig_financial.update_yaxes(title_text="ROI (Return) (%)", row=2, col=2)
    
    fig_financial.update_layout(
        height=800,
        title_text="💰 Financial Performance Analysis",
        showlegend=False
    )
    
    return fig_financial

financial_fig = create_financial_dashboard()
financial_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🕰️ Historical Era Analysis

# COMMAND ----------

# Create era-based analysis
def create_era_analysis():
    """Analyze performance trends across different movie eras"""
    
    # Calculate era statistics
    era_stats = movies_pd.groupby('movie_era').agg({
        'title': 'count',
        'rating': 'mean',
        'budget': 'mean',
        'box_office': 'mean',
        'profit': 'mean',
        'roi': 'mean',
        'performance_score': 'mean'
    }).reset_index()
    
    fig_era = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Evolution of Movie Budgets', 'Rating Trends by Era',
                       'Profitability Over Time', 'Era Performance Comparison'),
        specs=[[{"secondary_y": True}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "radar"}]]
    )
    
    # Budget and box office evolution
    fig_era.add_trace(go.Scatter(
        x=era_stats['movie_era'],
        y=era_stats['budget'],
        mode='lines+markers',
        name='Average Budget',
        line=dict(color='blue', width=3),
        yaxis='y'
    ), row=1, col=1)
    
    fig_era.add_trace(go.Scatter(
        x=era_stats['movie_era'],
        y=era_stats['box_office'],
        mode='lines+markers',
        name='Average Box Office',
        line=dict(color='green', width=3),
        yaxis='y2'
    ), row=1, col=1, secondary_y=True)
    
    # Rating trends
    fig_era.add_trace(go.Bar(
        x=era_stats['movie_era'],
        y=era_stats['rating'],
        name='Average Rating',
        marker_color='gold',
        text=era_stats['rating'].round(2),
        textposition='auto'
    ), row=1, col=2)
    
    # Profitability by era
    fig_era.add_trace(go.Bar(
        x=era_stats['movie_era'],
        y=era_stats['profit'],
        name='Average Profit',
        marker_color='lightcoral',
        text=era_stats['profit'].apply(lambda x: f'${x/1e6:.1f}M'),
        textposition='auto'
    ), row=2, col=1)
    
    # Radar chart for comprehensive era comparison
    categories = ['Rating', 'ROI', 'Performance Score']
    
    for i, era in enumerate(era_stats['movie_era']):
        era_row = era_stats[era_stats['movie_era'] == era].iloc[0]
        values = [
            era_row['rating']/10*100,  # Normalize to 0-100
            min(era_row['roi'], 500)/500*100,  # Cap and normalize ROI
            era_row['performance_score']/10*100  # Normalize performance score
        ]
        
        fig_era.add_trace(go.Scatterpolar(
            r=values + [values[0]],  # Close the polygon
            theta=categories + [categories[0]],
            fill='toself',
            name=era,
            opacity=0.6
        ), row=2, col=2)
    
    # Update layout
    fig_era.update_layout(
        height=800,
        title_text="🕰️ Historical Era Performance Analysis"
    )
    
    fig_era.update_xaxes(title_text="Movie Era", row=1, col=1)
    fig_era.update_yaxes(title_text="Budget ($)", row=1, col=1)
    fig_era.update_yaxes(title_text="Box Office ($)", secondary_y=True, row=1, col=1)
    
    return fig_era

era_fig = create_era_analysis()
era_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎬 Interactive Movie Explorer

# COMMAND ----------

# Create interactive movie explorer
def create_movie_explorer():
    """Create an interactive movie exploration tool"""
    
    fig_explorer = px.scatter(
        movies_pd,
        x='budget',
        y='box_office',
        size='rating',
        color='genre',
        hover_name='title',
        hover_data={
            'year': True,
            'rating': ':.1f',
            'roi': ':.1f',
            'performance_score': ':.1f',
            'budget': ':$,.0f',
            'box_office': ':$,.0f'
        },
        title='🎬 Interactive Movie Explorer - Budget vs Box Office',
        labels={
            'budget': 'Budget ($)',
            'box_office': 'Box Office Revenue ($)',
            'rating': 'Rating',
            'genre': 'Genre'
        },
        size_max=20
    )
    
    # Add break-even line
    max_val = max(movies_pd['budget'].max(), movies_pd['box_office'].max())
    fig_explorer.add_trace(go.Scatter(
        x=[0, max_val],
        y=[0, max_val],
        mode='lines',
        line=dict(color='red', dash='dash', width=2),
        name='Break-even Line',
        showlegend=True
    ))
    
    fig_explorer.update_layout(
        height=600,
        xaxis_title="Budget ($)",
        yaxis_title="Box Office Revenue ($)"
    )
    
    return fig_explorer

explorer_fig = create_movie_explorer()
explorer_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Performance Score Deep Dive

# COMMAND ----------

# Analyze the performance scoring algorithm
def create_performance_analysis():
    """Deep dive into performance score components"""
    
    fig_perf = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Performance Score Distribution', 'Score vs Rating Correlation',
                       'Top Performers Analysis', 'Score Components Heatmap')
    )
    
    # Performance score distribution
    fig_perf.add_trace(go.Histogram(
        x=movies_pd['performance_score'],
        nbinsx=25,
        name="Performance Score",
        marker_color='lightblue'
    ), row=1, col=1)
    
    # Score vs Rating correlation
    fig_perf.add_trace(go.Scatter(
        x=movies_pd['rating'],
        y=movies_pd['performance_score'],
        mode='markers',
        text=movies_pd['title'],
        marker=dict(
            size=8,
            color=movies_pd['roi'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="ROI (%)", x=1.1)
        ),
        name="Score vs Rating"
    ), row=1, col=2)
    
    # Top performers
    top_performers = movies_pd.nlargest(10, 'performance_score')
    fig_perf.add_trace(go.Bar(
        x=top_performers['performance_score'],
        y=top_performers['title'],
        orientation='h',
        name="Top Performers",
        marker_color='gold'
    ), row=2, col=1)
    
    # Performance components heatmap
    perf_components = movies_pd[['rating', 'roi', 'performance_score', 'budget', 'box_office']].corr()
    
    fig_perf.add_trace(go.Heatmap(
        z=perf_components.values,
        x=perf_components.columns,
        y=perf_components.columns,
        colorscale='RdBu',
        zmid=0,
        text=perf_components.values.round(2),
        texttemplate="%{text}",
        textfont={"size": 10},
        showscale=True
    ), row=2, col=2)
    
    fig_perf.update_layout(
        height=800,
        title_text="🎯 Performance Score Analysis"
    )
    
    return fig_perf

perf_fig = create_performance_analysis()
perf_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Business Intelligence Summary

# COMMAND ----------

# Create executive summary with key insights
def create_executive_summary():
    """Generate executive summary with actionable insights"""
    
    # Calculate key insights
    best_genre = genre_analytics_pd.loc[genre_analytics_pd['avg_rating'].idxmax(), 'genre']
    most_profitable_genre = genre_analytics_pd.loc[genre_analytics_pd['avg_roi'].idxmax(), 'genre']
    
    highest_rated = movies_pd.loc[movies_pd['rating'].idxmax()]
    most_profitable = movies_pd.loc[movies_pd['roi'].idxmax()]
    biggest_loss = movies_pd.loc[movies_pd['profit'].idxmin()]
    
    total_investment = movies_pd['budget'].sum()
    total_revenue = movies_pd['box_office'].sum()
    overall_roi = ((total_revenue - total_investment) / total_investment) * 100
    
    print("=" * 80)
    print("🎬 MOVIE INDUSTRY EXECUTIVE SUMMARY")
    print("=" * 80)
    print()
    
    print("📊 KEY PERFORMANCE INDICATORS:")
    print(f"   Total Movies Analyzed: {len(movies_pd):,}")
    print(f"   Total Industry Investment: ${total_investment:,.0f}")
    print(f"   Total Industry Revenue: ${total_revenue:,.0f}")
    print(f"   Overall Industry ROI: {overall_roi:.1f}%")
    print(f"   Average Movie Rating: {movies_pd['rating'].mean():.1f}/10")
    print(f"   Blockbuster Success Rate: {(movies_pd['is_blockbuster'].sum()/len(movies_pd)*100):.1f}%")
    print()
    
    print("🏆 TOP PERFORMERS:")
    print(f"   Highest Rated Movie: '{highest_rated['title']}' ({highest_rated['rating']:.1f}/10)")
    print(f"   Most Profitable Movie: '{most_profitable['title']}' (ROI: {most_profitable['roi']:.1f}%)")
    print(f"   Best Performing Genre: {best_genre}")
    print(f"   Most Profitable Genre: {most_profitable_genre}")
    print()
    
    print("⚠️ RISK ANALYSIS:")
    print(f"   Biggest Financial Loss: '{biggest_loss['title']}' (${biggest_loss['profit']:,.0f})")
    print(f"   Unprofitable Movies: {len(movies_pd[movies_pd['profit'] < 0])} ({len(movies_pd[movies_pd['profit'] < 0])/len(movies_pd)*100:.1f}%)")
    print(f"   High-Risk Investments (>$150M): {len(movies_pd[movies_pd['budget'] > 150000000])}")
    print()
    
    print("💡 STRATEGIC INSIGHTS:")
    era_performance = movies_pd.groupby('movie_era')['roi'].mean().sort_values(ascending=False)
    print(f"   Most Profitable Era: {era_performance.index[0]} (Avg ROI: {era_performance.iloc[0]:.1f}%)")
    
    high_performers = movies_pd[movies_pd['performance_score'] > movies_pd['performance_score'].quantile(0.8)]
    common_traits = high_performers['genre'].value_counts()
    print(f"   Success Pattern: {common_traits.index[0]} genre dominates top 20% performers")
    
    budget_sweet_spot = movies_pd[(movies_pd['budget'] > 20000000) & (movies_pd['budget'] < 100000000)]
    sweet_spot_roi = budget_sweet_spot['roi'].mean()
    print(f"   Optimal Budget Range: $20M-$100M (Avg ROI: {sweet_spot_roi:.1f}%)")
    print()
    
    print("🎯 RECOMMENDATIONS:")
    print("   1. Focus investment in", most_profitable_genre, "genre for highest ROI")
    print("   2. Consider mid-budget films ($20M-$100M) for optimal risk-adjusted returns")
    print("   3. Leverage", era_performance.index[0], "characteristics in new productions")
    print("   4. Implement risk management for high-budget productions (>$150M)")
    print("   5. Develop rating improvement strategies to boost performance scores")
    print()
    print("=" * 80)

# Generate executive summary
create_executive_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Export Visualizations

# COMMAND ----------

# Export charts for external use
def export_visualizations():
    """Export visualizations in various formats"""
    
    print("📁 Exporting visualizations...")
    
    # Create exports directory
    import os
    export_dir = "/tmp/movie_analytics_exports"
    os.makedirs(export_dir, exist_ok=True)
    
    # Export static images
    try:
        executive_fig.write_image(f"{export_dir}/executive_dashboard.png", width=1200, height=800)
        financial_fig.write_image(f"{export_dir}/financial_analysis.png", width=1200, height=800)
        era_fig.write_image(f"{export_dir}/era_analysis.png", width=1200, height=800)
        perf_fig.write_image(f"{export_dir}/performance_analysis.png", width=1200, height=800)
        print("✅ PNG exports completed")
    except Exception as e:
        print(f"⚠️ PNG export failed: {e}")
    
    # Export interactive HTML
    try:
        executive_fig.write_html(f"{export_dir}/executive_dashboard.html")
        explorer_fig.write_html(f"{export_dir}/movie_explorer.html")
        print("✅ HTML exports completed")
    except Exception as e:
        print(f"⚠️ HTML export failed: {e}")
    
    # Export data for external BI tools
    try:
        movies_pd.to_csv(f"{export_dir}/movies_data.csv", index=False)
        genre_analytics_pd.to_csv(f"{export_dir}/genre_analytics.csv", index=False)
        print("✅ CSV exports completed")
    except Exception as e:
        print(f"⚠️ CSV export failed: {e}")
    
    print(f"📂 Exports saved to: {export_dir}")
    return export_dir

export_path = export_visualizations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Next Steps
# MAGIC 
# MAGIC ### 📊 Visualization Outputs Created:
# MAGIC - **Executive KPI Dashboard** - High-level business metrics and indicators
# MAGIC - **Genre Performance Analysis** - Deep dive into genre-specific performance
# MAGIC - **Financial Performance Trends** - ROI, profit analysis, and risk-return matrices  
# MAGIC - **Historical Era Analysis** - Evolution of movie industry over time
# MAGIC - **Interactive Movie Explorer** - Explore individual movies and their performance
# MAGIC - **Performance Score Analysis** - Understanding the scoring algorithm
# MAGIC 
# MAGIC ### 🔗 Integration Options:
# MAGIC 1. **Power BI**: Connect to Gold layer Delta tables for real-time dashboards
# MAGIC 2. **Tableau**: Use exported CSV/Parquet files for advanced visualizations
# MAGIC 3. **Excel**: Import CSV exports for pivot tables and charts
# MAGIC 4. **Web Dashboards**: Use exported HTML files for web integration
# MAGIC 
# MAGIC ### 🚀 Advanced Features:
# MAGIC - Real-time streaming dashboards with Event Hubs
# MAGIC - Machine learning predictions integrated into visualizations
# MAGIC - Automated report generation and email distribution
# MAGIC - Mobile-responsive dashboard deployment
# MAGIC 
# MAGIC **Your movie analytics pipeline now includes comprehensive visualization capabilities!** 📈🎬