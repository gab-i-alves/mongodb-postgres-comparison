# MongoDB vs PostgreSQL Performance Comparison

A comprehensive analysis and benchmarking project comparing MongoDB and PostgreSQL performance for sentiment analysis data storage and retrieval.

## Project Overview

This project implements and compares the performance characteristics of MongoDB and PostgreSQL for handling large-scale sentiment analysis data. It includes data preprocessing, database implementations, and detailed performance benchmarking.

## Project Structure

```
├── data/
│   ├── sentiment_analysis_results_improved.csv
│   └── training.1600000.processed.noemoticon.csv
├── docs/
│   └── roteiro.md
├── image/
│   ├── execution_distribution.png
│   ├── high_scale_distribution.png
│   ├── iteration_comparison.png
│   ├── low_scale_distribution.png
│   ├── mean_execution_times.png
│   └── medium_scale_distribution.png
├── json/
│   └── benchmark_report.json
├── log/
│   ├── comparisons_results.log
│   ├── mongodb_import.log
│   ├── postgres_import.log
│   └── pre-processing.log
├── model/
│   ├── comparison-diagram.mermaid
│   ├── data-models.mermaid
│   ├── mongodb-diagram.mermaid
│   ├── postgres-diagram.mermaid
│   └── pre-processing-diagram.mermaid
└── scripts/
    ├── comparisons.py
    ├── mongodb-script.py
    ├── postgres-script.py
    └── pre-processing-script.py
```

## Features

- **Data Preprocessing**: Robust text preprocessing and sentiment analysis using TextBlob and VADER
- **Database Implementations**:
  - MongoDB implementation with optimized document structure
  - PostgreSQL implementation with properly normalized schema
- **Performance Benchmarking**:
  - Simple queries
  - Text search operations
  - Aggregation operations
  - Join/lookup operations
- **Comprehensive Analysis**:
  - Detailed performance metrics
  - Visual comparisons
  - Statistical analysis

## Key Findings

Based on the benchmark results:

1. **Text Search**: MongoDB performed 91.4% faster than PostgreSQL
2. **Aggregations**: PostgreSQL performed 1602% faster than MongoDB
3. **Joins**: PostgreSQL performed 135.1% faster than MongoDB
4. **Simple Queries**: Both databases showed comparable performance

## Technical Details

### Data Processing Pipeline

The project implements a three-stage pipeline:

1. **Pre-processing** (`pre-processing-script.py`):
   - Text cleaning and normalization
   - Sentiment analysis using multiple algorithms
   - Error handling and logging

2. **Database Import** (`mongodb-script.py` and `postgres-script.py`):
   - Optimized batch operations
   - Progress tracking
   - Resource monitoring

3. **Performance Analysis** (`comparisons.py`):
   - Automated benchmarking
   - Statistical analysis
   - Visualization generation

### Database Schemas

#### PostgreSQL Schema
- Normalized structure with three main tables:
  - Users
  - Tweets
  - SentimentAnalysis
- Proper indexing for optimal query performance

#### MongoDB Schema
- Document-based structure with embedded objects:
  - User information
  - Tweet content
  - Sentiment analysis results
- Text and compound indexes for search optimization

## Performance Metrics

### Import Performance
- MongoDB: 11,855.53 documents/second
- PostgreSQL: 7,933.12 records/second

### Query Performance (Average Response Times)
- Simple Queries:
  - MongoDB: 0.0029s
  - PostgreSQL: 0.0027s
- Text Search:
  - MongoDB: 0.0038s
  - PostgreSQL: 0.0439s
- Aggregations:
  - MongoDB: 0.9298s
  - PostgreSQL: 0.0546s
- Joins:
  - MongoDB: 0.0056s
  - PostgreSQL: 0.0024s

## Resource Usage

### Memory Consumption
- MongoDB: Peak at 2,313.89 MB during import
- PostgreSQL: Peak at 691.38 MB during import

## Installation and Usage

1. Install dependencies:
```bash
pip install pandas textblob nltk pymongo psycopg2-binary tqdm numpy matplotlib seaborn
```

2. Set up databases:
- MongoDB (local instance on default port 27017)
- PostgreSQL (local instance with credentials in scripts)

3. Run the pipeline:
```bash
python3 scripts/pre-processing-script.py
python3 scripts/postgres-script.py
python3 scripts/mongodb-script.py
python3 scripts/comparisons.py
```

## Conclusions

1. **Choose MongoDB for**:
   - Text search operations
   - Simple document retrieval
   - Cases where schema flexibility is important

2. **Choose PostgreSQL for**:
   - Complex aggregations
   - Join-heavy operations
   - Cases requiring strict data consistency
   - Memory-constrained environments

## About
This project was developed as a partial requirement for the Banco de Dados III course at the fourth semester of Análise e Desenvolvimento de Sistemas at UFPR. The goal was to implement and analyze different database approaches for handling sentiment analysis data, comparing the performance characteristics of relational (PostgreSQL) and NoSQL (MongoDB) database systems.