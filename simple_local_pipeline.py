"""
ETL Pipeline Implementation
Author: Mir Hasibul Hasan Rahat
Date: September 2025

This ETL pipeline implements a medallion architecture (Bronze-Silver-Gold)
for movie data processing with JSON storage and Pandas transformations.
Performance optimized for local execution with ~0.04s processing time.
"""

import pandas as pd
import json
import os
from datetime import datetime
import time

def create_sample_data():
    """Generate movie dataset for processing"""
    print("Initializing movie dataset...")
    
    movies = [
        {"title": "The Shawshank Redemption", "year": 1994, "genre": "Drama", "rating": 9.3, "budget": 25000000, "revenue": 16000000},
        {"title": "The Godfather", "year": 1972, "genre": "Crime", "rating": 9.2, "budget": 6000000, "revenue": 245000000},
        {"title": "The Dark Knight", "year": 2008, "genre": "Action", "rating": 9.0, "budget": 185000000, "revenue": 1004000000},
        {"title": "Pulp Fiction", "year": 1994, "genre": "Crime", "rating": 8.9, "budget": 8000000, "revenue": 214000000},
        {"title": "Forrest Gump", "year": 1994, "genre": "Drama", "rating": 8.8, "budget": 55000000, "revenue": 677000000},
        {"title": "Inception", "year": 2010, "genre": "Sci-Fi", "rating": 8.8, "budget": 160000000, "revenue": 829000000},
        {"title": "The Matrix", "year": 1999, "genre": "Sci-Fi", "rating": 8.7, "budget": 63000000, "revenue": 463000000},
        {"title": "Goodfellas", "year": 1990, "genre": "Crime", "rating": 8.7, "budget": 25000000, "revenue": 46800000},
        {"title": "Titanic", "year": 1997, "genre": "Romance", "rating": 7.8, "budget": 200000000, "revenue": 2187000000},
        {"title": "Avatar", "year": 2009, "genre": "Sci-Fi", "rating": 7.8, "budget": 237000000, "revenue": 2847000000}
    ]
    
    # Create data directory
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/bronze", exist_ok=True)
    os.makedirs("data/silver", exist_ok=True)
    os.makedirs("data/gold", exist_ok=True)
    
    return movies

def bronze_layer(movies):
    """Bronze Layer - Raw data ingestion"""
    print("\n[BRONZE LAYER] - Raw Data Ingestion")
    print("=" * 50)
    
    # Add ingestion metadata
    for movie in movies:
        movie['ingestion_timestamp'] = datetime.now().isoformat()
        movie['source_system'] = 'movie_api'
    
    # Save as JSON (simulating Delta table)
    with open("data/bronze/movies.json", 'w') as f:
        json.dump(movies, f, indent=2)
    
    print(f"SUCCESS: Loaded {len(movies)} records into Bronze layer")
    print("Bronze Data Preview:")
    for i, movie in enumerate(movies[:3]):
        print(f"   {i+1}. {movie['title']} ({movie['year']}) - {movie['genre']}")
    
    return movies

def silver_layer(bronze_data):
    """Silver Layer - Data cleaning and transformation"""
    print("\n[SILVER LAYER] - Data Cleaning & Validation")
    print("=" * 50)
    
    cleaned_movies = []
    
    for movie in bronze_data:
        # Data cleaning
        cleaned_movie = {
            'title': movie['title'],
            'year': int(movie['year']),
            'genre': movie['genre'].upper(),
            'rating': float(movie['rating']),
            'budget': int(movie.get('budget', 0)),
            'revenue': int(movie.get('revenue', 0)),
            'ingestion_timestamp': movie['ingestion_timestamp']
        }
        
        # Add calculated fields
        if cleaned_movie['budget'] > 0:
            cleaned_movie['profit_margin'] = ((cleaned_movie['revenue'] - cleaned_movie['budget']) / cleaned_movie['budget']) * 100
        else:
            cleaned_movie['profit_margin'] = 0
            
        # Add era classification
        if cleaned_movie['year'] < 1980:
            cleaned_movie['era'] = 'Classic'
        elif cleaned_movie['year'] < 2000:
            cleaned_movie['era'] = 'Modern'
        elif cleaned_movie['year'] < 2010:
            cleaned_movie['era'] = 'Digital'
        else:
            cleaned_movie['era'] = 'Contemporary'
        
        # Data quality filter
        if 1900 <= cleaned_movie['year'] <= 2025 and 0 <= cleaned_movie['rating'] <= 10:
            cleaned_movies.append(cleaned_movie)
    
    # Save cleaned data
    with open("data/silver/movies_clean.json", 'w') as f:
        json.dump(cleaned_movies, f, indent=2)
    
    print(f"SUCCESS: Cleaned {len(cleaned_movies)} records in Silver layer")
    print("Silver Data Preview:")
    for i, movie in enumerate(cleaned_movies[:3]):
        print(f"   {i+1}. {movie['title']} - Era: {movie['era']}, Profit: {movie['profit_margin']:.1f}%")
    
    return cleaned_movies

def gold_layer(silver_data):
    """Gold Layer - Business analytics"""
    print("\n[GOLD LAYER] - Business Analytics")
    print("=" * 50)
    
    # Convert to DataFrame for analysis
    df = pd.DataFrame(silver_data)
    
    # Genre analysis
    genre_analysis = df.groupby('genre').agg({
        'title': 'count',
        'rating': 'mean',
        'revenue': 'sum',
        'profit_margin': 'mean'
    }).round(2)
    genre_analysis.columns = ['movie_count', 'avg_rating', 'total_revenue', 'avg_profit_margin']
    
    # Era analysis  
    era_analysis = df.groupby('era').agg({
        'title': 'count',
        'rating': 'mean',
        'budget': 'mean',
        'revenue': 'mean'
    }).round(2)
    era_analysis.columns = ['movie_count', 'avg_rating', 'avg_budget', 'avg_revenue']
    
    # Top movies by revenue
    top_movies = df.nlargest(5, 'revenue')[['title', 'year', 'genre', 'revenue', 'rating']]
    
    # Save analytics
    genre_analysis.to_json("data/gold/genre_analysis.json", indent=2)
    era_analysis.to_json("data/gold/era_analysis.json", indent=2)
    top_movies.to_json("data/gold/top_movies.json", indent=2)
    
    print("SUCCESS: Gold layer analytics created:")
    print("   > Genre Analysis")
    print("   > Era Analysis") 
    print("   > Top Movies")
    
    print("\nBUSINESS INSIGHTS:")
    print("\nGenre Performance:")
    print(genre_analysis)
    
    print("\nEra Analysis:")
    print(era_analysis)
    
    print("\nTop 5 Movies by Revenue:")
    print(top_movies)
    
    return {
        'genre_analysis': genre_analysis,
        'era_analysis': era_analysis,
        'top_movies': top_movies
    }

def main():
    """Main execution"""
    start_time = time.time()
    
    print(">>> SIMPLE ETL PIPELINE - LOCAL EXECUTION <<<")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Execute pipeline
        movies = create_sample_data()
        bronze_data = bronze_layer(movies)
        silver_data = silver_layer(bronze_data)
        gold_analytics = gold_layer(silver_data)
        
        # Summary
        execution_time = time.time() - start_time
        print(f"\n>>> PIPELINE COMPLETED SUCCESSFULLY! <<<")
        print("=" * 60)
        print(f"Total execution time: {execution_time:.2f} seconds")
        print(f"Data stored in: data/ directory")
        print(f"Layers processed: Bronze -> Silver -> Gold")
        print(f"Ready for analysis!")
        
    except Exception as e:
        print(f"\nERROR: Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()