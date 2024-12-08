import pandas as pd
import json
import time
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
import logging
from datetime import datetime
import psutil
import os
from typing import List, Dict
import numpy as np
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='./log/mongodb_import.log'
)

class MongoDBImporter:
    def __init__(self, uri: str, db_name: str, collection_name: str):
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        self.progress_bar = None

    def connect(self):
        """Establish MongoDB connection with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
                self.client.server_info()  # Test connection
                self.db = self.client[self.db_name]
                self.collection = self.db[self.collection_name]
                logging.info("Successfully connected to MongoDB")
                return True
            except Exception as e:
                logging.error(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)

    def delete_database(self):
        """Safely delete existing database."""
        try:
            if self.db_name in self.client.list_database_names():
                self.client.drop_database(self.db_name)
                logging.info(f"Database '{self.db_name}' dropped successfully")
        except Exception as e:
            logging.error(f"Failed to drop database: {e}")
            raise

    @staticmethod
    def prepare_document(row: pd.Series) -> Dict:
        """Transform a row into a MongoDB document with proper type conversion."""
        try:
            # Skip header row
            if row["ids"] == "ids":
                return None
            
            # Convert numeric values safely
            tweet_id = int(float(row["ids"])) if pd.notna(row["ids"]) else None
            target = int(float(row["target"])) if pd.notna(row["target"]) else None
            
            # Handle potential NaN values in float columns
            textblob_polarity = float(row["textblob_polarity"]) if pd.notna(row["textblob_polarity"]) else 0.0
            vader_compound = float(row["vader_compound"]) if pd.notna(row["vader_compound"]) else 0.0
            
            document = {
                "tweet_id": tweet_id,
                "date": str(row["date"]),
                "user": {
                    "username": str(row["user"]),
                    "flag": str(row["flag"])
                },
                "content": {
                    "original_text": str(row["text"]),
                    "cleaned_text": str(row["cleaned_text"]),
                    "original_sentiment": str(row["original_sentiment"]) if pd.notna(row["original_sentiment"]) else None
                },
                "sentiment_analysis": {
                    "target": target,
                    "textblob_sentiment": str(row["textblob_sentiment"]),
                    "vader_sentiment": str(row["vader_sentiment"]),
                    "textblob_polarity": textblob_polarity,
                    "vader_compound": vader_compound
                }
            }
            
            return document if document["tweet_id"] is not None else None
            
        except Exception as e:
            logging.error(f"Error preparing document: {e}")
            logging.error(f"Problematic row: {row.to_dict()}")
            return None

    def create_indexes(self):
        """Create optimized indexes for common queries."""
        indexes = [
            ("tweet_id", 1, True),
            ("date", 1, False),
            ("user.username", 1, False),
            ("sentiment_analysis.target", 1, False),
            ("content.cleaned_text", "text", False)
        ]
        
        with tqdm(total=len(indexes), desc="Creating indexes") as pbar:
            for field, direction, unique in indexes:
                try:
                    if direction == "text":
                        self.collection.create_index([(field, direction)])
                    else:
                        self.collection.create_index([(field, direction)], unique=unique)
                    pbar.update(1)
                except Exception as e:
                    logging.error(f"Failed to create index for {field}: {e}")
                    raise

    def bulk_insert_documents(self, documents: List[Dict], batch_size: int = 1000):
        """Perform bulk insert operations with error handling and progress tracking."""
        try:
            # Filter out None documents
            valid_documents = [doc for doc in documents if doc is not None]
            total_documents = len(valid_documents)
            logging.info(f"Attempting to insert {total_documents} valid documents")
            
            # Create insert operations
            operations = [InsertOne(doc) for doc in valid_documents]
            
            # Calculate total batches for progress bar
            total_batches = (total_documents + batch_size - 1) // batch_size
            
            total_inserted = 0
            failed_inserts = 0
            
            with tqdm(total=total_batches, desc="Inserting documents") as pbar:
                for i in range(0, len(operations), batch_size):
                    batch = operations[i:i + batch_size]
                    try:
                        result = self.collection.bulk_write(batch, ordered=False)
                        total_inserted += result.inserted_count
                    except BulkWriteError as bwe:
                        failed_inserts += len(batch) - (bwe.details.get('nInserted', 0))
                        logging.warning(f"Batch {i//batch_size + 1} partial failure: {bwe.details}")
                    
                    pbar.update(1)
                    
                    # Monitor memory usage periodically
                    if i % (batch_size * 10) == 0:
                        process = psutil.Process(os.getpid())
                        memory_mb = process.memory_info().rss / 1024 / 1024
                        logging.info(f"Memory usage: {memory_mb:.2f} MB")
            
            logging.info(f"Import completed: {total_inserted} documents inserted, {failed_inserts} failed")
            return total_inserted, failed_inserts
                
        except Exception as e:
            logging.error(f"Bulk insert failed: {e}")
            raise

def main():
    start_time = datetime.now()
    logging.info(f"Starting MongoDB import at {start_time}")
    
    # Configuration
    MONGO_URI = "mongodb://localhost:27017/"
    DB_NAME = "sentiment_analysis"
    COLLECTION_NAME = "tweets"
    BATCH_SIZE = 5000  # Increased batch size for better performance
    
    try:
        # Initialize importer
        importer = MongoDBImporter(MONGO_URI, DB_NAME, COLLECTION_NAME)
        importer.connect()
        importer.delete_database()
        
        # Load data with explicit dtypes
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
                'flag': str
            }
        )
        
        logging.info(f"Loaded {len(df)} rows from CSV")
        
        # Process documents with progress bar
        documents = []
        with tqdm(total=len(df), desc="Preparing documents") as pbar:
            for _, row in df.iterrows():
                doc = MongoDBImporter.prepare_document(row)
                if doc is not None:
                    documents.append(doc)
                pbar.update(1)
        
        logging.info(f"Prepared {len(documents)} valid documents for import")
        
        # Import documents
        total_inserted, failed_inserts = importer.bulk_insert_documents(documents, BATCH_SIZE)
        
        # Create indexes after successful import
        if total_inserted > 0:
            importer.create_indexes()
        
        # Final statistics
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        doc_count = importer.collection.count_documents({})
        
        logging.info("Import Summary:")
        logging.info(f"Total time: {processing_time:.2f} seconds")
        logging.info(f"Documents processed: {len(df)}")
        logging.info(f"Documents inserted: {total_inserted}")
        logging.info(f"Failed inserts: {failed_inserts}")
        logging.info(f"Final collection count: {doc_count}")
        logging.info(f"Average insertion rate: {total_inserted/processing_time:.2f} documents/second")
        
    except Exception as e:
        logging.error(f"Import process failed: {e}")
        raise
    finally:
        if 'importer' in locals():
            importer.client.close()

if __name__ == "__main__":
    main()