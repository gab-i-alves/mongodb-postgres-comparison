import pandas as pd
import re
from textblob import TextBlob
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
from datetime import datetime
import logging
import multiprocessing as mp
from tqdm import tqdm
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='./log/pre-processing.log'
)

class SentimentAnalyzer:
    def __init__(self):
        try:
            self.sia = SentimentIntensityAnalyzer()
        except Exception as e:
            logging.error(f"Failed to initialize VADER: {e}")
            raise

    @staticmethod
    def preprocess_text(text):
        """Enhanced text preprocessing with error handling."""
        try:
            if not isinstance(text, str):
                return ""
            
            url_pattern = r'https?://\S+|www\.\S+'
            mention_pattern = r'@\w+'
            hashtag_pattern = r'#\w+'
            
            text = re.sub(url_pattern, '', text)
            text = re.sub(mention_pattern, '', text)
            text = re.sub(hashtag_pattern, '', text)
            text = re.sub(r'[^\w\s]', '', text)
            
            return text.lower().strip()
        except Exception as e:
            logging.error(f"Text preprocessing failed: {e}")
            return ""

    def analyze_sentiment(self, text):
        """Combined sentiment analysis with error handling."""
        try:
            # TextBlob analysis
            blob_polarity = TextBlob(text).sentiment.polarity
            textblob_result = 'positive' if blob_polarity > 0 else 'negative' if blob_polarity < 0 else 'neutral'
            
            # VADER analysis
            vader_scores = self.sia.polarity_scores(text)
            vader_result = 'positive' if vader_scores['compound'] >= 0.05 else 'negative' if vader_scores['compound'] <= -0.05 else 'neutral'
            
            return {
                'textblob_sentiment': textblob_result,
                'vader_sentiment': vader_result,
                'textblob_polarity': blob_polarity,
                'vader_compound': vader_scores['compound']
            }
        except Exception as e:
            logging.error(f"Sentiment analysis failed: {e}")
            return {
                'textblob_sentiment': 'neutral',
                'vader_sentiment': 'neutral',
                'textblob_polarity': 0.0,
                'vader_compound': 0.0
            }

def parse_date(date_str):
    """Parse date string and handle timezones properly."""
    try:
        # Remove timezone abbreviations (PDT, UTC, etc.)
        cleaned_date = re.sub(r'\s+(?:PDT|PST|UTC)', '', date_str)
        # Parse the cleaned date string
        return pd.to_datetime(cleaned_date, format='%a %b %d %H:%M:%S %Y', errors='coerce')
    except Exception as e:
        logging.error(f"Date parsing failed for: {date_str}, Error: {e}")
        return None

def handle_duplicates(df):
    """Handle duplicate tweet IDs by keeping the most recent version."""
    logging.info(f"Original number of rows: {len(df)}")
    
    # Convert dates using the custom parser
    logging.info("Converting dates...")
    df['date'] = df['date'].apply(parse_date)
    
    # Remove rows with invalid dates
    invalid_dates = df['date'].isna().sum()
    if invalid_dates > 0:
        logging.warning(f"Found {invalid_dates} invalid dates")
        df = df.dropna(subset=['date'])
        logging.info(f"Rows remaining after removing invalid dates: {len(df)}")
    
    # Sort by date (newest first) and drop duplicates based on tweet ID
    df = df.sort_values('date', ascending=False).drop_duplicates(subset='ids', keep='first')
    
    logging.info(f"Number of rows after removing duplicates: {len(df)}")
    return df

def process_chunk(chunk, analyzer):
    """Process a chunk of data."""
    try:
        chunk = chunk.copy()
        chunk['cleaned_text'] = chunk['text'].apply(analyzer.preprocess_text)
        
        # Apply sentiment analysis
        sentiments = chunk['cleaned_text'].apply(analyzer.analyze_sentiment)
        
        # Extract sentiment results
        chunk['textblob_sentiment'] = sentiments.apply(lambda x: x['textblob_sentiment'])
        chunk['vader_sentiment'] = sentiments.apply(lambda x: x['vader_sentiment'])
        chunk['textblob_polarity'] = sentiments.apply(lambda x: x['textblob_polarity'])
        chunk['vader_compound'] = sentiments.apply(lambda x: x['vader_compound'])
        
        return chunk
    except Exception as e:
        logging.error(f"Chunk processing failed: {e}")
        return None

def main():
    start_time = datetime.now()
    logging.info(f"Starting sentiment analysis at {start_time}")
    
    # Configuration
    CHUNK_SIZE = 10000
    N_PROCESSES = max(1, mp.cpu_count() - 1)
    INPUT_FILE = "./data/training.1600000.processed.noemoticon.csv"
    OUTPUT_FILE = "./data/sentiment_analysis_results_improved.csv"
    
    try:
        # Initialize analyzer
        analyzer = SentimentAnalyzer()
        
        # Load dataset
        df = pd.read_csv(
            INPUT_FILE,
            low_memory=False,
            encoding="ISO-8859-1",
            names=['target', 'ids', 'date', 'flag', 'user', 'text']
        )
        
        # Remove duplicates before processing
        df = handle_duplicates(df)
        
        # Process chunks in parallel
        chunks = np.array_split(df, max(1, len(df) // CHUNK_SIZE))
        
        processed_chunks = []
        with mp.Pool(N_PROCESSES) as pool:
            with tqdm(total=len(chunks), desc="Processing chunks") as pbar:
                for chunk in chunks:
                    result = pool.apply_async(process_chunk, args=(chunk, analyzer))
                    processed_chunks.append(result)
                    pbar.update(1)
                
                # Get results
                results = [r.get() for r in processed_chunks if r.get() is not None]
        
        # Combine results
        df = pd.concat(results, ignore_index=True)
        
        # Post-processing
        df['original_sentiment'] = df['target'].map({0: 'negative', 4: 'positive'})
        df['comparison_textblob'] = df['original_sentiment'] == df['textblob_sentiment']
        df['comparison_vader'] = df['original_sentiment'] == df['vader_sentiment']
        
        # Verify no duplicates in final dataset
        final_duplicate_count = df['ids'].duplicated().sum()
        if final_duplicate_count > 0:
            logging.warning(f"Found {final_duplicate_count} remaining duplicates. Removing them...")
            df = df.drop_duplicates(subset='ids', keep='first')
        
        # Format dates for output
        df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Save results
        df.to_csv(OUTPUT_FILE, index=False, date_format='%Y-%m-%d %H:%M:%S')
        
        # Log statistics
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        logging.info("\nProcessing Statistics:")
        logging.info(f"Total processing time: {processing_time:.2f} seconds")
        logging.info(f"Final number of processed tweets: {len(df)}")
        logging.info("\nSentiment Distribution:")
        logging.info(df['original_sentiment'].value_counts().to_string())
        logging.info("\nAccuracy Metrics:")
        logging.info(f"TextBlob accuracy: {(df['comparison_textblob'].mean() * 100):.2f}%")
        logging.info(f"VADER accuracy: {(df['comparison_vader'].mean() * 100):.2f}%")
        
    except Exception as e:
        logging.error(f"Main process failed: {e}")
        raise

if __name__ == "__main__":
    main()