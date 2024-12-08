import time
import logging
import psycopg2
from pymongo import MongoClient
from typing import Dict, List, Callable, Tuple
import statistics
import json
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dataclasses import dataclass
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='./log/comparisons_results.log'
)

@dataclass
class QueryResult:
    execution_times: List[float]
    mean: float
    median: float
    std_dev: float
    min_time: float
    max_time: float

class DatabaseBenchmark:
    def __init__(self, iterations: int = 3):
        self.iterations = iterations
        # MongoDB setup
        self.mongo_client = MongoClient("mongodb://localhost:27017/")
        self.mongo_db = self.mongo_client["sentiment_analysis"]
        self.mongo_collection = self.mongo_db["tweets"]

        # PostgreSQL setup
        self.pg_conn = psycopg2.connect(
            dbname="sentiment_analysis",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        self.pg_cursor = self.pg_conn.cursor()

    def run_benchmark(self) -> Dict:
        """Run all benchmarks multiple times and collect results."""
        results = {
            "simple_queries": self.benchmark_simple_queries(),
            "text_search": self.benchmark_text_search(),
            "aggregations": self.benchmark_aggregations(),
            "joins": self.benchmark_joins()
        }
        return results

    def execute_query_with_stats(self, name: str, query_func: Callable) -> QueryResult:
        """Execute a query multiple times and collect statistics."""
        execution_times = []
        
        for i in range(self.iterations):
            logging.info(f"Running {name} - Iteration {i+1}/{self.iterations}")
            start_time = time.time()
            query_func()
            execution_time = time.time() - start_time
            execution_times.append(execution_time)
            
            # Add delay between iterations to prevent overloading
            time.sleep(1)
        
        return QueryResult(
            execution_times=execution_times,
            mean=statistics.mean(execution_times),
            median=statistics.median(execution_times),
            std_dev=statistics.stdev(execution_times) if len(execution_times) > 1 else 0,
            min_time=min(execution_times),
            max_time=max(execution_times)
        )

    def benchmark_simple_queries(self) -> Dict:
        """Benchmark basic CRUD operations."""
        queries = {
            "mongodb_find": (
                lambda: list(self.mongo_collection.find({
                    "sentiment_analysis.target": 4
                }).limit(100))
            ),
            "postgres_select": (
                lambda: self.pg_cursor.execute("""
                    SELECT * FROM tweets t
                    JOIN sentiment_analysis s ON t.tweet_id = s.tweet_id
                    WHERE s.target = 4
                    LIMIT 100
                """)
            )
        }
        return {name: self.execute_query_with_stats(name, func) 
                for name, func in queries.items()}

    def benchmark_text_search(self) -> Dict:
        """Benchmark text search capabilities."""
        queries = {
            "mongodb_text_search": (
                lambda: list(self.mongo_collection.find({
                    "$text": {"$search": "love"}
                }).limit(100))
            ),
            "postgres_text_search": (
                lambda: self.pg_cursor.execute("""
                    SELECT * FROM tweets
                    WHERE to_tsvector('english', cleaned_text) @@ to_tsquery('english', 'love')
                    LIMIT 100
                """)
            )
        }
        return {name: self.execute_query_with_stats(name, func) 
                for name, func in queries.items()}

    def benchmark_aggregations(self) -> Dict:
        """Benchmark aggregation operations."""
        queries = {
            "mongodb_aggregation": (
                lambda: list(self.mongo_collection.aggregate([
                    {
                        "$group": {
                            "_id": "$sentiment_analysis.target",
                            "count": {"$sum": 1},
                            "avg_vader_compound": {"$avg": "$sentiment_analysis.vader_compound"}
                        }
                    }
                ]))
            ),
            "postgres_aggregation": (
                lambda: self.pg_cursor.execute("""
                    SELECT 
                        target,
                        COUNT(*),
                        AVG(vader_compound)
                    FROM sentiment_analysis
                    GROUP BY target
                """)
            )
        }
        return {name: self.execute_query_with_stats(name, func) 
                for name, func in queries.items()}

    def benchmark_joins(self) -> Dict:
        """Benchmark join operations."""
        queries = {
            "mongodb_lookup": (
                lambda: list(self.mongo_collection.aggregate([
                    {
                        "$lookup": {
                            "from": "users",
                            "localField": "user.username",
                            "foreignField": "username",
                            "as": "user_details"
                        }
                    },
                    {"$limit": 100}
                ]))
            ),
            "postgres_join": (
                lambda: self.pg_cursor.execute("""
                    SELECT t.*, u.*, s.*
                    FROM tweets t
                    JOIN users u ON t.user_id = u.user_id
                    JOIN sentiment_analysis s ON t.tweet_id = s.tweet_id
                    LIMIT 100
                """)
            )
        }
        return {name: self.execute_query_with_stats(name, func) 
                for name, func in queries.items()}

    def generate_visualizations(self, results: Dict) -> None:
        # Set style and figure parameters
        plt.style.use('seaborn-v0_8')
        plt.rcParams.update({
            'figure.figsize': (15, 10),
            'font.size': 12,
            'axes.titlesize': 14,
            'axes.labelsize': 12
        })
        """Generate comprehensive performance comparison visualizations."""
        # Prepare data for plotting
        plot_data = []
        for category, tests in results.items():
            for test_name, metrics in tests.items():
                for i, time_value in enumerate(metrics.execution_times):
                    plot_data.append({
                        'Category': category,
                        'Test': test_name,
                        'Iteration': i + 1,
                        'Time (s)': time_value,
                        'Database': 'MongoDB' if 'mongo' in test_name.lower() else 'PostgreSQL'
                    })
        
        df = pd.DataFrame(plot_data)
        
        # Create visualizations
        
        # 1. Box plots with adjusted scales
        # Create three separate plots for different scale ranges
        
        # High scale (aggregations)
        plt.figure(figsize=(15, 6))
        agg_data = df[df['Category'] == 'aggregations']
        sns.boxplot(
            data=agg_data,
            x='Category',
            y='Time (s)',
            hue='Database',
            palette='Set3'
        )
        plt.title('High-Scale Operations (Aggregations)', pad=20)
        plt.ylabel('Time (s)')
        plt.tight_layout()
        plt.savefig('./image/high_scale_distribution.png')
        plt.close()

        # Medium scale (joins)
        plt.figure(figsize=(15, 6))
        joins_data = df[df['Category'] == 'joins']
        sns.boxplot(
            data=joins_data,
            x='Category',
            y='Time (s)',
            hue='Database',
            palette='Set3'
        )
        plt.title('Medium-Scale Operations (Joins)', pad=20)
        plt.ylabel('Time (s)')
        plt.tight_layout()
        plt.savefig('./image/medium_scale_distribution.png')
        plt.close()

        # Low scale (simple queries and text search)
        plt.figure(figsize=(15, 6))
        low_scale_data = df[df['Category'].isin(['simple_queries', 'text_search'])]
        sns.boxplot(
            data=low_scale_data,
            x='Category',
            y='Time (s)',
            hue='Database',
            palette='Set3'
        )
        plt.title('Low-Scale Operations (Simple Queries and Text Search)', pad=20)
        plt.ylabel('Time (s)')
        # Format y-axis to show milliseconds
        plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x*1000:.2f}ms'))
        plt.tight_layout()
        plt.savefig('./image/low_scale_distribution.png')
        plt.close()

        # Combined visualization with subplots and adjusted scales
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 18), height_ratios=[1, 1, 1])

        # High scale
        sns.boxplot(
            data=agg_data,
            x='Category',
            y='Time (s)',
            hue='Database',
            palette='Set3',
            ax=ax1
        )
        ax1.set_title('High-Scale Operations (Aggregations)', pad=20)
        ax1.set_ylabel('Time (s)')

        # Medium scale
        sns.boxplot(
            data=joins_data,
            x='Category',
            y='Time (s)',
            hue='Database',
            palette='Set3',
            ax=ax2
        )
        ax2.set_title('Medium-Scale Operations (Joins)', pad=20)
        ax2.set_ylabel('Time (s)')

        # Low scale with millisecond formatting
        sns.boxplot(
            data=low_scale_data,
            x='Category',
            y='Time (s)',
            hue='Database',
            palette='Set3',
            ax=ax3
        )
        ax3.set_title('Low-Scale Operations (Simple Queries and Text Search)', pad=20)
        ax3.set_ylabel('Time (ms)')
        ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x*1000:.2f}'))

        plt.tight_layout(pad=3.0)
        plt.savefig('./image/execution_distribution.png')
        plt.title('Query Execution Time Distribution by Category')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('./image/execution_distribution.png')
        plt.close()
        
        # 2. Bar plot showing mean execution times with error bars - split by scale
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12), height_ratios=[1, 1])
        summary_df = df.groupby(['Category', 'Database'])['Time (s)'].agg(['mean', 'std']).reset_index()
        
        # High-scale operations
        high_scale_summary = summary_df[summary_df['Category'].isin(['aggregations'])]
        sns.barplot(
            data=high_scale_summary,
            x='Category',
            y='mean',
            hue='Database',
            palette='Set3',
            errorbar='sd',
            ax=ax1
        )
        ax1.set_title('High-Scale Operations (Aggregations)')
        ax1.set_ylabel('Mean Execution Time (s)')
        
        # Low-scale operations
        low_scale_summary = summary_df[~summary_df['Category'].isin(['aggregations'])]
        sns.barplot(
            data=low_scale_summary,
            x='Category',
            y='mean',
            hue='Database',
            palette='Set3',
            errorbar='sd',
            ax=ax2
        )
        ax2.set_title('Low-Scale Operations')
        ax2.set_ylabel('Mean Execution Time (s)')
        plt.title('Average Query Execution Time by Category')
        plt.ylabel('Mean Execution Time (s)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('./image/mean_execution_times.png')
        plt.close()
        
        # 3. Line plot showing execution times across iterations
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        axes = axes.ravel()
        
        for idx, category in enumerate(sorted(df['Category'].unique())):
            category_data = df[df['Category'] == category]
            
            sns.lineplot(
                data=category_data,
                x='Iteration',
                y='Time (s)',
                hue='Database',
                marker='o',
                ax=axes[idx]
            )
            
            # Set appropriate scale for y-axis
            if category == 'aggregations':
                axes[idx].set_ylabel('Time (s) - Log Scale')
                axes[idx].set_yscale('log')
            else:
                axes[idx].set_ylabel('Time (s)')
            
            axes[idx].set_title(f'{category} - Execution Times')
            axes[idx].grid(True)
            plt.title(f'{category} - Execution Times Across Iterations')
            plt.xlabel('Iteration')
            plt.ylabel('Execution Time (s)')
        
        plt.tight_layout()
        plt.savefig('./image/iteration_comparison.png')
        plt.close()

    def generate_report(self, results: Dict) -> None:
        """Generate comprehensive performance report."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "test_categories": list(results.keys()),
                "total_queries_executed": sum(len(cat) for cat in results.values()),
                "iterations_per_query": self.iterations,
                "overall_findings": self._generate_findings(results)
            },
            "detailed_results": {
                category: {
                    test_name: {
                        "mean": metrics.mean,
                        "median": metrics.median,
                        "std_dev": metrics.std_dev,
                        "min": metrics.min_time,
                        "max": metrics.max_time,
                        "all_iterations": metrics.execution_times
                    }
                    for test_name, metrics in tests.items()
                }
                for category, tests in results.items()
            }
        }

        # Save report as JSON
        with open('./json/benchmark_report.json', 'w') as f:
            json.dump(report, f, indent=2)

        # Generate visualizations
        self.generate_visualizations(results)

        # Log summary
        logging.info("Benchmark Summary:")
        for category, tests in results.items():
            logging.info(f"{category.upper()}:")
            for test_name, metrics in tests.items():
                logging.info(f"  {test_name}:")
                logging.info(f"    Mean: {metrics.mean:.4f}s")
                logging.info(f"    Median: {metrics.median:.4f}s")
                logging.info(f"    Std Dev: {metrics.std_dev:.4f}s")

    def _generate_findings(self, results: Dict) -> List[str]:
        """Generate insights from benchmark results."""
        findings = []
        
        for category, tests in results.items():
            mongo_tests = {k: v for k, v in tests.items() if 'mongo' in k.lower()}
            pg_tests = {k: v for k, v in tests.items() if 'postgres' in k.lower()}
            
            for mongo_name, mongo_metrics in mongo_tests.items():
                for pg_name, pg_metrics in pg_tests.items():
                    diff_percent = ((mongo_metrics.mean - pg_metrics.mean) 
                                  / pg_metrics.mean * 100)
                    
                    if abs(diff_percent) > 10:  # Significant difference threshold
                        faster_db = "MongoDB" if diff_percent < 0 else "PostgreSQL"
                        findings.append(
                            f"In {category}, {faster_db} was {abs(diff_percent):.1f}% "
                            f"faster on average ({mongo_name} vs {pg_name})"
                        )

        return findings

    def close(self):
        """Clean up database connections."""
        self.mongo_client.close()
        self.pg_cursor.close()
        self.pg_conn.close()

def main():
    """Run the complete benchmark suite."""
    try:
        benchmark = DatabaseBenchmark(iterations=3)
        
        # Run benchmarks
        results = benchmark.run_benchmark()
        
        # Generate comprehensive report and visualizations
        benchmark.generate_report(results)
        
        logging.info("Benchmark completed successfully. Check comparison_report.json and visualization files.")
        
    except Exception as e:
        logging.error(f"Benchmark failed: {e}")
        raise
    finally:
        if 'benchmark' in locals():
            benchmark.close()

if __name__ == "__main__":
    main()