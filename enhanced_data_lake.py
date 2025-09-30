"""
Enhanced Data Lake Simulation
This version adds proper data lake characteristics to the local pipeline
"""

import pandas as pd
import json
import os
import pyarrow.parquet as pq
from datetime import datetime
import time

def create_data_lake_structure():
    """Create proper data lake directory structure"""
    
    lake_structure = {
        'raw': 'data/lake/raw',           # Landing zone
        'bronze': 'data/lake/bronze',     # Raw ingestion
        'silver': 'data/lake/silver',     # Refined data
        'gold': 'data/lake/gold',         # Curated analytics
        'archive': 'data/lake/archive',   # Historical data
        'metadata': 'data/lake/metadata'  # Schema registry
    }
    
    for zone, path in lake_structure.items():
        os.makedirs(path, exist_ok=True)
        print(f"Created data lake zone: {zone} -> {path}")
    
    return lake_structure

def simulate_multiple_data_sources():
    """Simulate data from multiple sources with different formats"""
    
    # Movie data (JSON)
    movies = [
        {"id": 1, "title": "The Shawshank Redemption", "year": 1994, "genre": "Drama", "rating": 9.3, "budget": 25000000, "revenue": 16000000},
        {"id": 2, "title": "The Godfather", "year": 1972, "genre": "Crime", "rating": 9.2, "budget": 6000000, "revenue": 245000000},
        {"id": 3, "title": "The Dark Knight", "year": 2008, "genre": "Action", "rating": 9.0, "budget": 185000000, "revenue": 1004000000},
        {"id": 4, "title": "Pulp Fiction", "year": 1994, "genre": "Crime", "rating": 8.9, "budget": 8000000, "revenue": 214000000},
        {"id": 5, "title": "Forrest Gump", "year": 1994, "genre": "Drama", "rating": 8.8, "budget": 55000000, "revenue": 677000000}
    ]
    
    # User ratings (CSV)
    ratings = [
        {"user_id": 1, "movie_id": 1, "rating": 5, "timestamp": "2024-01-15"},
        {"user_id": 2, "movie_id": 1, "rating": 4, "timestamp": "2024-01-16"},
        {"user_id": 1, "movie_id": 2, "rating": 5, "timestamp": "2024-01-17"},
        {"user_id": 3, "movie_id": 3, "rating": 5, "timestamp": "2024-01-18"},
        {"user_id": 2, "movie_id": 4, "rating": 4, "timestamp": "2024-01-19"}
    ]
    
    # Box office data (Parquet)
    box_office = [
        {"movie_id": 1, "week": 1, "weekend_gross": 2500000, "theaters": 2500},
        {"movie_id": 2, "week": 1, "weekend_gross": 15000000, "theaters": 500},
        {"movie_id": 3, "week": 1, "weekend_gross": 158000000, "theaters": 4366},
        {"movie_id": 4, "week": 1, "weekend_gross": 9500000, "theaters": 1338},
        {"movie_id": 5, "week": 1, "weekend_gross": 24500000, "theaters": 1595}
    ]
    
    return {
        'movies': movies,
        'ratings': ratings,  
        'box_office': box_office
    }

def data_lake_ingestion(data_sources, lake_structure):
    """Ingest data into data lake with proper partitioning"""
    
    print("\n[DATA LAKE INGESTION] - Multi-format Data Landing")
    print("=" * 60)
    
    # Raw zone - Store as received
    with open(f"{lake_structure['raw']}/movies_api.json", 'w') as f:
        json.dump(data_sources['movies'], f, indent=2)
    
    ratings_df = pd.DataFrame(data_sources['ratings'])
    ratings_df.to_csv(f"{lake_structure['raw']}/user_ratings.csv", index=False)
    
    box_office_df = pd.DataFrame(data_sources['box_office'])
    box_office_df.to_parquet(f"{lake_structure['raw']}/box_office.parquet")
    
    # Bronze zone - Standardized formats with metadata
    for source_name, data in data_sources.items():
        df = pd.DataFrame(data)
        
        # Add data lake metadata
        df['_ingestion_timestamp'] = datetime.now()
        df['_source_system'] = f"{source_name}_api"
        df['_partition_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Store as parquet (data lake standard)
        parquet_path = f"{lake_structure['bronze']}/{source_name}.parquet"
        df.to_parquet(parquet_path)
        
        print(f"SUCCESS: Ingested {len(df)} {source_name} records to Bronze")
    
    return True

def data_lake_processing(lake_structure):
    """Process data through Silver and Gold layers with proper schemas"""
    
    print("\n[DATA LAKE PROCESSING] - Silver & Gold Layer Creation")
    print("=" * 60)
    
    # Silver Layer - Join and enrich data
    movies_df = pd.read_parquet(f"{lake_structure['bronze']}/movies.parquet")
    ratings_df = pd.read_parquet(f"{lake_structure['bronze']}/ratings.parquet")
    box_office_df = pd.read_parquet(f"{lake_structure['bronze']}/box_office.parquet")
    
    # Create enriched movie dataset
    enriched_movies = movies_df.merge(
        ratings_df.groupby('movie_id')['rating'].agg(['mean', 'count']).reset_index(),
        left_on='id', right_on='movie_id', how='left'
    ).merge(
        box_office_df.groupby('movie_id')['weekend_gross'].sum().reset_index(),
        left_on='id', right_on='movie_id', how='left'
    )
    
    # Clean column names
    enriched_movies.columns = [col.replace('mean', 'avg_user_rating') if 'mean' in col else col for col in enriched_movies.columns]
    enriched_movies.columns = [col.replace('count', 'user_rating_count') if 'count' in col else col for col in enriched_movies.columns]
    
    # Calculate business metrics
    enriched_movies['roi'] = ((enriched_movies['revenue'] - enriched_movies['budget']) / enriched_movies['budget'] * 100).round(2)
    enriched_movies['profit'] = enriched_movies['revenue'] - enriched_movies['budget']
    
    # Partition by decade for data lake optimization
    enriched_movies['decade'] = (enriched_movies['year'] // 10) * 10
    
    # Save to Silver layer with partitioning
    for decade in enriched_movies['decade'].unique():
        decade_data = enriched_movies[enriched_movies['decade'] == decade]
        partition_path = f"{lake_structure['silver']}/movies_enriched/decade={decade}"
        os.makedirs(partition_path, exist_ok=True)
        decade_data.to_parquet(f"{partition_path}/data.parquet")
    
    print(f"SUCCESS: Created Silver layer with {len(enriched_movies)} enriched records")
    print(f"INFO: Partitioned by decade: {sorted(enriched_movies['decade'].unique())}")
    
    # Gold Layer - Business aggregations
    gold_analytics = {
        'genre_performance': enriched_movies.groupby('genre').agg({
            'revenue': 'sum',
            'profit': 'sum', 
            'roi': 'mean',
            'rating': 'mean',
            'id': 'count'
        }).round(2),
        
        'decade_trends': enriched_movies.groupby('decade').agg({
            'revenue': 'mean',
            'budget': 'mean',
            'rating': 'mean',
            'roi': 'mean',
            'id': 'count'
        }).round(2),
        
        'top_performers': enriched_movies.nlargest(5, 'revenue')[
            ['title', 'year', 'genre', 'revenue', 'profit', 'roi']
        ]
    }
    
    # Save Gold layer analytics
    for analysis_name, data in gold_analytics.items():
        gold_path = f"{lake_structure['gold']}/{analysis_name}.parquet"
        data.to_parquet(gold_path)
        print(f"SUCCESS: Created Gold layer analysis: {analysis_name}")
    
    return gold_analytics

def show_data_lake_summary(lake_structure, analytics):
    """Display data lake summary and insights"""
    
    print("\n[DATA LAKE SUMMARY] - Architecture & Business Insights")
    print("=" * 60)
    
    # Show data lake structure
    print("\nData Lake Architecture:")
    for zone in ['raw', 'bronze', 'silver', 'gold']:
        zone_path = lake_structure[zone]
        file_count = len([f for f in os.listdir(zone_path) if os.path.isfile(os.path.join(zone_path, f))])
        dir_count = len([d for d in os.listdir(zone_path) if os.path.isdir(os.path.join(zone_path, d))])
        print(f"  {zone.upper():>8} -> {file_count} files, {dir_count} partitions")
    
    print("\nBusiness Insights from Data Lake:")
    print("\nGenre Performance:")
    print(analytics['genre_performance'])
    
    print("\nDecade Trends:")  
    print(analytics['decade_trends'])
    
    print("\nTop Performers:")
    print(analytics['top_performers'])

def main():
    """Enhanced Data Lake Pipeline"""
    start_time = time.time()
    
    print(">>> ENHANCED DATA LAKE PIPELINE <<<")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Create data lake structure
        lake_structure = create_data_lake_structure()
        
        # Simulate multiple data sources
        data_sources = simulate_multiple_data_sources()
        
        # Ingest data into data lake
        data_lake_ingestion(data_sources, lake_structure)
        
        # Process through Silver and Gold layers
        analytics = data_lake_processing(lake_structure)
        
        # Show summary
        show_data_lake_summary(lake_structure, analytics)
        
        # Summary
        execution_time = time.time() - start_time
        print(f"\n>>> DATA LAKE PIPELINE COMPLETED! <<<")
        print("=" * 60)
        print(f"Total execution time: {execution_time:.2f} seconds")
        print(f"Data lake location: data/lake/")
        print(f"Formats used: JSON, CSV, Parquet")
        print(f"Partitioning: By decade for optimization")
        print(f"Ready for analytics and BI tools!")
        
    except Exception as e:
        print(f"\nERROR: Data lake pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()