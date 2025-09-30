"""
Movie Data Generator
Creates movie dataset for ETL pipeline processing
Built by Mir Hasibul Hasan Rahat
"""

import pandas as pd
import json
import os
from datetime import datetime, timedelta
import random
from config.local_config import local_config

def create_movie_dataset():
    """Create movie dataset for ETL processing"""
    
    # Movie titles and data for processing
    movies_data = [
        {"title": "The Shawshank Redemption", "year": 1994, "genre": "Drama", "rating": 9.3, "budget": 25000000, "revenue": 16000000},
        {"title": "The Godfather", "year": 1972, "genre": "Crime", "rating": 9.2, "budget": 6000000, "revenue": 245000000},
        {"title": "The Dark Knight", "year": 2008, "genre": "Action", "rating": 9.0, "budget": 185000000, "revenue": 1004000000},
        {"title": "Pulp Fiction", "year": 1994, "genre": "Crime", "rating": 8.9, "budget": 8000000, "revenue": 214000000},
        {"title": "Forrest Gump", "year": 1994, "genre": "Drama", "rating": 8.8, "budget": 55000000, "revenue": 677000000},
        {"title": "Inception", "year": 2010, "genre": "Sci-Fi", "rating": 8.8, "budget": 160000000, "revenue": 829000000},
        {"title": "The Matrix", "year": 1999, "genre": "Sci-Fi", "rating": 8.7, "budget": 63000000, "revenue": 463000000},
        {"title": "Goodfellas", "year": 1990, "genre": "Crime", "rating": 8.7, "budget": 25000000, "revenue": 46800000},
        {"title": "Titanic", "year": 1997, "genre": "Romance", "rating": 7.8, "budget": 200000000, "revenue": 2187000000},
        {"title": "Avatar", "year": 2009, "genre": "Sci-Fi", "rating": 7.8, "budget": 237000000, "revenue": 2847000000},
        {"title": "Avengers: Endgame", "year": 2019, "genre": "Action", "rating": 8.4, "budget": 356000000, "revenue": 2797000000},
        {"title": "Jurassic Park", "year": 1993, "genre": "Adventure", "rating": 8.1, "budget": 63000000, "revenue": 1029000000},
        {"title": "The Lion King", "year": 1994, "genre": "Animation", "rating": 8.5, "budget": 45000000, "revenue": 968000000},
        {"title": "Star Wars", "year": 1977, "genre": "Sci-Fi", "rating": 8.6, "budget": 11000000, "revenue": 775000000},
        {"title": "E.T.", "year": 1982, "genre": "Family", "rating": 7.8, "budget": 10500000, "revenue": 792000000},
        {"title": "The Wizard of Oz", "year": 1939, "genre": "Family", "rating": 8.0, "budget": 2800000, "revenue": 3000000},
        {"title": "Casablanca", "year": 1942, "genre": "Romance", "rating": 8.5, "budget": 1000000, "revenue": 4200000},
        {"title": "Citizen Kane", "year": 1941, "genre": "Drama", "rating": 8.3, "budget": 839000, "revenue": 1600000},
        {"title": "Gone with the Wind", "year": 1939, "genre": "Romance", "rating": 8.1, "budget": 3900000, "revenue": 200000000},
        {"title": "Lawrence of Arabia", "year": 1962, "genre": "Adventure", "rating": 8.3, "budget": 15000000, "revenue": 70000000}
    ]
    
    # Add more fields and randomize some data
    enhanced_movies = []
    directors = ["Christopher Nolan", "Steven Spielberg", "Martin Scorsese", "Quentin Tarantino", 
                "Francis Ford Coppola", "Stanley Kubrick", "Alfred Hitchcock", "George Lucas"]
    
    studios = ["Warner Bros", "Universal", "Paramount", "Disney", "Sony", "20th Century Fox", "MGM", "Columbia"]
    
    for movie in movies_data:
        enhanced_movie = movie.copy()
        
        # Add additional fields
        enhanced_movie["director"] = random.choice(directors)
        enhanced_movie["studio"] = random.choice(studios)
        enhanced_movie["runtime"] = random.randint(90, 180)  # minutes
        enhanced_movie["votes"] = random.randint(100000, 2000000)
        enhanced_movie["language"] = "English"
        enhanced_movie["country"] = "USA"
        
        # Add some data quality issues for testing (nulls, inconsistencies)
        if random.random() < 0.1:  # 10% chance of missing budget
            enhanced_movie["budget"] = None
        
        if random.random() < 0.05:  # 5% chance of inconsistent genre
            enhanced_movie["genre"] = enhanced_movie["genre"].lower()
        
        # Add timestamp
        enhanced_movie["ingestion_timestamp"] = datetime.now().isoformat()
        
        enhanced_movies.append(enhanced_movie)
    
    return enhanced_movies

def create_movie_datasets():
    """Create movie datasets for ETL pipeline"""
    
    print("Creating movie dataset...")
    
    # Generate movie data
    movies = create_movie_dataset()
    
    # Create raw data directory
    raw_data_path = os.path.join(local_config.data_path, "raw")
    os.makedirs(raw_data_path, exist_ok=True)
    
    # Save as JSON (simulating API response)
    movies_json_path = os.path.join(raw_data_path, "movies.json")
    with open(movies_json_path, 'w') as f:
        json.dump(movies, f, indent=2)
    
    # Save as CSV (simulating file upload)
    movies_df = pd.DataFrame(movies)
    movies_csv_path = os.path.join(raw_data_path, "movies.csv")
    movies_df.to_csv(movies_csv_path, index=False)
    
    print(f"Movie dataset created:")
    print(f"   📁 JSON: {movies_json_path}")
    print(f"   📁 CSV: {movies_csv_path}")
    print(f"   📊 Records: {len(movies)}")
    
    return {
        "movies_json": movies_json_path,
        "movies_csv": movies_csv_path,
        "record_count": len(movies)
    }

if __name__ == "__main__":
    create_movie_datasets()