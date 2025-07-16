import os
import sys
import logging
from typing import List, Optional, Tuple
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, date_format, row_number, when, concat, lit
from pyspark.sql.window import Window
import pandas as pd


class ETLLogSearch:
    
    def __init__(self, log_level: str = "INFO", log_file: Optional[str] = None):
        """
        Initialize the ETL Log Search.
        
        Args:
            log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file (str, optional): Path to log file. If None, logs to console only
        """
        self.logger = self._setup_logging(log_level, log_file)
        self.spark = None
        self.logger.info("ETL Search Analyzer initialized")
        
    def _setup_logging(self, log_level: str, log_file: Optional[str]) -> logging.Logger:
        """
        Set up logging configuration.
        
        Args:
            log_level (str): Logging level
            log_file (str, optional): Path to log file
            
        Returns:
            logging.Logger: Configured logger instance
        """
        logger = logging.getLogger(__name__)
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler (if specified)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
        return logger
    
    def create_spark_session(self, app_name: str = "ETL_Log_Seach") -> SparkSession:
        """
        Create and configure Spark session.
        
        Args:
            app_name (str): Name of the Spark application
            
        Returns:
            SparkSession: Configured Spark session
        """
        try:
            self.logger.info(f"Creating Spark session with app name: {app_name}")
            self.spark = SparkSession.builder.appName(app_name).getOrCreate()
            self.logger.info("Spark session created successfully")
            return self.spark
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            raise
    
    def load_multiple_datasets(self, parent_path: str) -> DataFrame:
        """
        Load and combine multiple datasets from different folders.
        
        Args:
            parent_path (str): Parent directory containing dataset folders
            
        Returns:
            DataFrame: Combined DataFrame from all datasets
        """
        try:
            self.logger.info(f"Loading multiple datasets from: {parent_path}")
            
            if not os.path.exists(parent_path):
                raise FileNotFoundError(f"Parent path does not exist: {parent_path}")
            
            folder_names = sorted(os.listdir(parent_path))
            all_dfs = []
            
            for folder in folder_names:
                full_path = os.path.join(parent_path, folder)
                if os.path.isdir(full_path):
                    self.logger.info(f"Processing folder: {folder}")
                    parquet_files = [f for f in os.listdir(full_path) if f.endswith(".parquet")]
                    
                    for parquet_file in parquet_files:
                        file_path = os.path.join(full_path, parquet_file)
                        self.logger.debug(f"Loading file: {file_path}")
                        df_temp = self.spark.read.parquet(file_path)
                        df_temp = df_temp.withColumn("Month", date_format(col("datetime"), "MM"))
                        all_dfs.append(df_temp)
            
            if not all_dfs:
                raise ValueError("No parquet files found in the specified path")
            
            # Combine all DataFrames
            df = all_dfs[0]
            for df_next in all_dfs[1:]:
                df = df.unionByName(df_next)
            
            # Remove the rows with null values and users' action is not searching
            df = df.filter(
                (col('action') == 'search') & 
                (col('user_id').isNotNull()) & 
                (col('keyword').isNotNull())
            )
            
            self.logger.info(f"Successfully combined {len(all_dfs)} datasets with {df.count()} filtered rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load multiple datasets: {str(e)}")
            raise
    
    def most_search(self, df: DataFrame, month: int) -> DataFrame:
        """
        Find the most searched keyword for each user in a specific month.
        
        Args:
            df (DataFrame): Input DataFrame with search data
            month (int): Month to analyze
            
        Returns:
            DataFrame: DataFrame with most searched keywords per user
        """
        try:
            self.logger.info(f"Analyzing most searched keywords for month: {month}")
            
            # Filter by month
            df_filtered = df.filter(col('Month') == str(month).zfill(2))
            
            # Select relevant columns and group by user and keyword
            df_grouped = df_filtered.select('user_id', 'keyword', 'Month')
            df_grouped = df_grouped.groupBy('user_id', 'keyword', 'Month').count()
            df_grouped = df_grouped.withColumnRenamed('count', 'Total_search')
            
            # Create window function to rank searches per user
            window = Window.partitionBy('user_id').orderBy(col('Total_search').desc())
            df_ranked = df_grouped.withColumn('Rank', row_number().over(window))
            
            # Get the most searched keyword for each user
            df_result = df_ranked.filter(col('Rank') == 1)
            df_result = df_result.withColumnRenamed('keyword', 'Most_Search')
            df_result = df_result.select('user_id', 'Most_Search', 'Month')
            
            self.logger.info(f"Found most searched keywords for {df_result.count()} users in month {month}")
            return df_result
            
        except Exception as e:
            self.logger.error(f"Failed to analyze most searched keywords for month {month}: {str(e)}")
            raise
    
    def load_keyword_categories(self, excel_path: str) -> DataFrame:
        """
        Load keyword categories from Excel file.
        
        Args:
            excel_path (str): Path to Excel file with keyword categories
            
        Returns:
            DataFrame: DataFrame with keyword categories
        """
        try:
            self.logger.info(f"Loading keyword categories from: {excel_path}")
            
            if not os.path.exists(excel_path):
                raise FileNotFoundError(f"Excel file not found: {excel_path}")
            
            key_category_pd = pd.read_excel(excel_path)
            key_category = self.spark.createDataFrame(key_category_pd)
            
            self.logger.info(f"Successfully loaded {key_category.count()} keyword categories")
            return key_category
            
        except Exception as e:
            self.logger.error(f"Failed to load keyword categories: {str(e)}")
            raise
    
    def join_with_categories(self, df: DataFrame, key_category: DataFrame, 
                           month: str) -> DataFrame:
        """
        Join search data with keyword categories.
        
        Args:
            df (DataFrame): Search data DataFrame
            key_category (DataFrame): Keyword categories DataFrame
            month (str): Specific month
            
        Returns:
            DataFrame: DataFrame with categories joined
        """
        try:
            self.logger.info(f"Joining search data with categories for {month}")
            
            df_joined = df.join(key_category, 'Most_Search', 'inner')
            df_joined = df_joined.select('user_id', 'Most_Search', 'Category')
            
            # Rename columns with time period suffix
            df_joined = df_joined.withColumnRenamed('Most_Search', f'Most_Search_{month}')
            df_joined = df_joined.withColumnRenamed('Category', f'Category_{month}')
            
            self.logger.info(f"Successfully joined categories for {df_joined.count()} records")
            return df_joined
            
        except Exception as e:
            self.logger.error(f"Failed to join with categories for {month}: {str(e)}")
            raise
    
    def merge_data_frame(self, df_t6: DataFrame, df_t7: DataFrame) -> DataFrame:
        """
        Merge data from two months
        
        Args:
            df_t6 (DataFrame): Data from June (06/01 - 06/14)
            df_t7 (DataFrame): Data from July (07/01 - 07/14)
            
        Returns:
            DataFrame: Merged DataFrame
        """
        try:
            self.logger.info("Merging data from two months")
            
            merge_df = df_t6.join(df_t7, 'user_id', 'inner')
            
            self.logger.info(f"Successfully merged data with {merge_df.count()} records")
            return merge_df
            
        except Exception as e:
            self.logger.error(f"Failed to merge: {str(e)}")
            raise
    
    def analyze_category_changes(self, merge_df: DataFrame) -> DataFrame:
        """
        Analyze category changes between two months.
        
        Args:
            merge_df (DataFrame): Merged DataFrame from two months
            
        Returns:
            DataFrame: DataFrame with category change analysis
        """
        try:
            self.logger.info("Analyzing category changes between two months")
            
            condition = (col('Category_T6') == col('Category_T7'))
            result_df = merge_df.withColumn(
                'Category_change', 
                when(condition, 'No Change').otherwise(
                    concat(col('Category_T6'), lit(' - '), col('Category_T7'))
                )
            )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Failed to analyze category changes: {str(e)}")
            raise
    
    def save_results(self, df: DataFrame, output_path: str) -> None:
        """
        Save results to CSV file.
        
        Args:
            df (DataFrame): DataFrame to save
            output_path (str): Output file path or directory
        """
        try:
            self.logger.info(f"Saving results to: {output_path} in csv format")
            df.write.mode("overwrite").csv(output_path, header=True)
            self.logger.info(f"Results saved successfully to {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to save results: {str(e)}")
            raise
    
    def run_elt_process(self, data_path: str, keyword_categories_path: str, 
                    output_path: str = "./output_most_search") -> DataFrame:
        """
        Run the complete ETL pipeline.
        
        Args:
            data_path (str): Path to data directory
            keyword_categories_path (str): Path to keyword categories Excel file
            output_path (str): Output directory for results
            
        Returns:
            DataFrame: Final results
        """
        try:
            self.logger.info("Starting ETL pipeline")
            
            # Step 1: Load data
            df = self.load_multiple_datasets(data_path)
            
            # Step 2: Analyze most searched keywords for each month
            df_t6 = self.most_search(df, month=6)
            df_t7 = self.most_search(df, month=7)
            
            # Step 3: Load keyword categories
            key_category = self.load_keyword_categories(keyword_categories_path)
            
            # Step 4: Join with categories
            df_t6 = self.join_with_categories(df_t6, key_category, 'T6')
            df_t7 = self.join_with_categories(df_t7, key_category, 'T7')
            
            # Step 5: Merge time periods
            merge_df = self.merge_data_frame(df_t6, df_t7)
            
            # Step 6: Analyze category changes
            final_df = self.analyze_category_changes(merge_df)
            
            # Step 7: Save results
            self.save_results(final_df, output_path)
            
            self.logger.info("ETL pipeline completed successfully")
            return final_df
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {str(e)}")
            raise

    def cleanup(self) -> None:
        """
        Clean up resources.
        """
        try:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")


def main():
    """
    Main function to run the ETL analysis.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize analyzer
    analyzer = ETLLogSearch(log_level="INFO", log_file="/content/drive/MyDrive/Colab Notebooks/output/etl-process.log")
    
    try:
        # Create Spark session
        analyzer.create_spark_session()
        
        # Define paths (adjust these according to your environment)
        data_path = "/content/drive/MyDrive/Colab Notebooks/data"  # Directory containing 20220601, 20220714 folders
        keyword_categories_path = "/content/drive/MyDrive/Colab Notebooks/mapping-keywords-with-category.xlsx"
        output_path = "/content/drive/MyDrive/Colab Notebooks/output"
        
        # Run analysis
        results = analyzer.run_elt_process(data_path, keyword_categories_path, output_path)
        
        # Show sample results
        print("\n=== Sample Results ===")
        results.show(10)
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        logging.error(f"Main execution failed: {str(e)}")
        
    finally:
        # Cleanup
        analyzer.cleanup()


if __name__ == "__main__":
    main() 