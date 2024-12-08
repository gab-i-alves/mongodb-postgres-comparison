import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
import os
from typing import List, Dict
from tqdm import tqdm
import psutil
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='./log/postgres_import.log'
)

class PostgresImporter:
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        self.connection_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.conn = None
        self.cursor = None

    def connect(self, database: str = "postgres"):
        """Establish database connection with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(**{**self.connection_params, "dbname": database})
                self.conn.autocommit = True
                self.cursor = self.conn.cursor()
                logging.info(f"Successfully connected to {database}")
                return
            except Exception as e:
                logging.error(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)

    def create_database(self):
        """Create fresh database for sentiment analysis."""
        try:
            # Connect to default database first
            self.connect()
            
            # Drop database if exists
            self.cursor.execute("""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = %s
                AND pid <> pg_backend_pid();
            """, (self.connection_params["dbname"],))
            
            self.cursor.execute(f"DROP DATABASE IF EXISTS {self.connection_params['dbname']}")
            self.cursor.execute(f"CREATE DATABASE {self.connection_params['dbname']}")
            logging.info(f"Database {self.connection_params['dbname']} created successfully")
            
            # Close connection to default database
            self.close()
            
            # Connect to new database
            self.connect(self.connection_params["dbname"])
        except Exception as e:
            logging.error(f"Database creation failed: {e}")
            raise

    def create_tables(self):
        """Create optimized table schema with proper constraints."""
        try:
            # Create users table
            self.cursor.execute("""
            CREATE TABLE users (
                user_id SERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                flag VARCHAR(50),
                UNIQUE(username)
            );
            """)

            # Create tweets table
            self.cursor.execute("""
            CREATE TABLE tweets (
                tweet_id BIGINT PRIMARY KEY,
                user_id INTEGER REFERENCES users(user_id),
                date TIMESTAMP NOT NULL,
                original_text TEXT NOT NULL,
                cleaned_text TEXT,
                original_sentiment VARCHAR(20)
            );
            """)

            # Create sentiment_analysis table
            self.cursor.execute("""
            CREATE TABLE sentiment_analysis (
                sentiment_id SERIAL PRIMARY KEY,
                tweet_id BIGINT REFERENCES tweets(tweet_id),
                target INTEGER NOT NULL,
                textblob_sentiment VARCHAR(20),
                vader_sentiment VARCHAR(20),
                textblob_polarity FLOAT,
                vader_compound FLOAT,
                comparison_textblob BOOLEAN,
                comparison_vader BOOLEAN
            );
            """)

            logging.info("Tables created successfully")
            self.create_indexes()
            
        except Exception as e:
            logging.error(f"Table creation failed: {e}")
            raise

    def create_indexes(self):
        """Create optimized indexes for better query performance."""
        try:
            # Create indexes for tweets table
            self.cursor.execute("""
                CREATE INDEX idx_tweet_date ON tweets(date);
                CREATE INDEX idx_tweet_user ON tweets(user_id);
                CREATE INDEX idx_tweet_cleaned_text ON tweets USING gin(to_tsvector('english', cleaned_text));
            """)

            # Create indexes for sentiment_analysis table
            self.cursor.execute("""
                CREATE INDEX idx_sentiment_tweet ON sentiment_analysis(tweet_id);
                CREATE INDEX idx_sentiment_target ON sentiment_analysis(target);
            """)

            logging.info("Indexes created successfully")
        except Exception as e:
            logging.error(f"Index creation failed: {e}")
            raise

    def bulk_insert_users(self, users_df: pd.DataFrame, batch_size: int = 1000):
        """Efficiently insert user data in batches."""
        try:
            # Map 'user' column to 'username'
            users_df = users_df.rename(columns={'user': 'username'})
            
            # Log column names for debugging
            logging.info(f"Available columns: {users_df.columns.tolist()}")
            
            users_data = users_df[["username", "flag"]].drop_duplicates().values.tolist()
            
            logging.info(f"Preparing to insert {len(users_data)} unique users")
            
            execute_values(
                self.cursor,
                "INSERT INTO users (username, flag) VALUES %s ON CONFLICT (username) DO NOTHING",
                users_data,
                page_size=batch_size
            )
            self.conn.commit()
            logging.info(f"Inserted {len(users_data)} users")
        except Exception as e:
            logging.error(f"User insertion failed: {e}")
            logging.error(f"Error details: {str(e)}")
            raise

    def bulk_insert_tweets(self, tweets_df: pd.DataFrame, batch_size: int = 1000):
        """Efficiently insert tweet data in batches."""
        try:
            # Get user_id mapping
            self.cursor.execute("SELECT username, user_id FROM users")
            user_map = dict(self.cursor.fetchall())
            
            tweets_data = []
            for _, row in tweets_df.iterrows():
                tweets_data.append((
                    row["ids"],
                    user_map.get(row["user"]),
                    row["date"],
                    row["text"],
                    row["cleaned_text"],
                    row["original_sentiment"]
                ))
            
            execute_values(
                self.cursor,
                """
                INSERT INTO tweets (tweet_id, user_id, date, original_text, 
                                  cleaned_text, original_sentiment)
                VALUES %s
                """,
                tweets_data,
                page_size=batch_size
            )
            self.conn.commit()
            logging.info(f"Inserted {len(tweets_data)} tweets")
        except Exception as e:
            logging.error(f"Tweet insertion failed: {e}")
            raise

    def bulk_insert_sentiment(self, sentiment_df: pd.DataFrame, batch_size: int = 1000):
        """Efficiently insert sentiment analysis data in batches."""
        try:
            sentiment_data = []
            for _, row in sentiment_df.iterrows():
                sentiment_data.append((
                    row["ids"],
                    row["target"],
                    row["textblob_sentiment"],
                    row["vader_sentiment"],
                    row["textblob_polarity"],
                    row["vader_compound"],
                    row["comparison_textblob"],
                    row["comparison_vader"]
                ))
            
            execute_values(
                self.cursor,
                """
                INSERT INTO sentiment_analysis (tweet_id, target, textblob_sentiment,
                    vader_sentiment, textblob_polarity, vader_compound, 
                    comparison_textblob, comparison_vader)
                VALUES %s
                """,
                sentiment_data,
                page_size=batch_size
            )
            self.conn.commit()
            logging.info(f"Inserted {len(sentiment_data)} sentiment records")
        except Exception as e:
            logging.error(f"Sentiment insertion failed: {e}")
            raise

    def close(self):
        """Safely close database connections."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

def monitor_resources():
    """Monitor system resource usage."""
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / 1024 / 1024  # MB
    cpu_percent = process.cpu_percent()
    logging.info(f"Memory usage: {memory_usage:.2f} MB, CPU usage: {cpu_percent}%")

def main():
    start_time = datetime.now()
    logging.info(f"Starting PostgreSQL import at {start_time}")
    
    # Configuration
    DB_CONFIG = {
        "dbname": "sentiment_analysis",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": "5432"
    }
    BATCH_SIZE = 5000
    
    try:
        # Initialize importer
        importer = PostgresImporter(**DB_CONFIG)
        importer.create_database()
        importer.create_tables()
        
        # Load data
        df = pd.read_csv(
            "./data/sentiment_analysis_results_improved.csv",
            low_memory=False,
            dtype={
                'ids': str,
                'target': str,
                'textblob_polarity': float,
                'vader_compound': float,
                'text': str,
                'cleaned_text': str,
                'user': str,
                'flag': str,
                'comparison_textblob': bool,
                'comparison_vader': bool
            }
        )
        
        # Validate data
        logging.info(f"Loaded {len(df)} rows from CSV")
        logging.info(f"Columns in dataset: {df.columns.tolist()}")
        
        # Check for required columns
        required_columns = ['ids', 'target', 'text', 'cleaned_text', 'user', 'flag', 
                          'textblob_sentiment', 'vader_sentiment', 'textblob_polarity', 
                          'vader_compound', 'comparison_textblob', 'comparison_vader']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        # Check for null values in critical columns
        null_counts = df[['ids', 'user', 'flag']].isnull().sum()
        if null_counts.any():
            logging.warning(f"Null values found in critical columns:{null_counts[null_counts > 0]}")
            
        # Display sample of data
        logging.info("First few rows of user and flag columns:")
        logging.info(df[['user', 'flag']].head().to_string())
        
        # Process and import data
        with tqdm(total=3, desc="Importing data") as pbar:
            importer.bulk_insert_users(df, BATCH_SIZE)
            monitor_resources()
            pbar.update(1)
            
            importer.bulk_insert_tweets(df, BATCH_SIZE)
            monitor_resources()
            pbar.update(1)
            
            importer.bulk_insert_sentiment(df, BATCH_SIZE)
            monitor_resources()
            pbar.update(1)
        
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        logging.info("Import Summary:")
        logging.info(f"Total time: {processing_time:.2f} seconds")
        logging.info(f"Data imported successfully!")
        
    except Exception as e:
        logging.error(f"Import process failed: {e}")
        raise
    finally:
        if 'importer' in locals():
            importer.close()

if __name__ == "__main__":
    main()