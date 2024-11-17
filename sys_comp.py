import sqlite3
import pandas as pd
import numpy as np
from newsapi import newsapi_client
import os
import yfinance as yf
from datetime import datetime
import sqlite3

class DataLake:
    def __init__(self,db_name="datalake.db"):
        self.raw_data = {} # raw json type
        self.processed_data = {}
        self.db_name = db_name
        
    def access_control(func):
        def wrapper(*args, **kwargs):
            password = "1234"
            input_pw = input("Type in your password")
            if input_pw == password:
                result = func(*args,**kwargs)
            else:
                result = "You have no access to this DataLake!"
            return result
        return wrapper
        
    @access_control
    def store_data(self, dataset_name, data, processed=False): # adding data
        if processed:
            with sqlite3.connect(self.db_name) as conn:
                if dataset_name not in self.processed_data:
                    self.processed_data[dataset_name] = data
                    data.to_sql(dataset_name, conn, index=False, if_exists='replace')
                else:  # Append new data if table already exists
                    self.processed_data[dataset_name] = pd.concat([self.processed_data[dataset_name], data])
                    data.to_sql(dataset_name, conn, index=False, if_exists='append')

        else:
            if dataset_name not in self.raw_data:
                self.raw_data[dataset_name] = data
            else: # preventing overrridinng
                self.raw_data[dataset_name] += data
    
    @access_control
    def retrieve_data(self, dataset_name, processed=False, sql_query = None): # for data filtering and extraction
        if processed: # assuming value for each processed key is a dataframe
            with sqlite3.connect(self.db_name) as conn:
                if sql_query is None:
                    sql_query = f"SELECT * FROM {dataset_name}"
                try:
                    query_data = pd.read_sql_query(sql_query, conn)
                    return query_data
                except Exception as e:
                    print(f"Error: {e}")
                    return None
            
        else: # if raw data just return its raw data
            return self.raw_data[dataset_name]
        
    
class DataCategory:

    def __init__(self, name):
        self.name = name
        self.datasets = {}  # Dictionary to store datasets with their metadata 

    def add_dataset(self, dataset_name, metadata=None):
        """
        param metadata: Metadata dictionary (e.g., description, parameters, etc.).
        """
        if dataset_name not in self.datasets:
            self.datasets[dataset_name] = metadata or {}

    def search(self, keyword):
        """
        Search for datasets within the category by keyword.
        :param keyword: Keyword to search in dataset names or metadata.
        :return: List of matching datasets.
        """
        results = []
        for dataset_name, metadata in self.datasets.items():
            if keyword.lower() in dataset_name.lower() or any(
                keyword.lower() in str(value).lower() for value in metadata.values()
            ):
                results.append((dataset_name, metadata))
        return results


class DataCatalog:
    """
    Organizes datasets into categories and provides metadata for easy discovery.
    """
    def __init__(self, data_lake):
        """
        Initialize the catalog with a reference to the DataLake.
        :param data_lake: An instance of the DataLake class.
        """
        self.categories = {}
        self.data_lake = data_lake  # Link to the DataLake instance

    def add_category(self, category_name):
        """
        Add a new category to the catalog.
        :param category_name: Name of the category.
        """
        if category_name not in self.categories:
            self.categories[category_name] = DataCategory(category_name)

    def add_dataset(self, category_name, dataset_name, data, metadata=None, processed=False):

        if category_name not in self.categories:
            self.add_category(category_name)

        # Add dataset to the category
        self.categories[category_name].add_dataset(dataset_name, metadata)

        # Store the dataset in the DataLake
        self.data_lake.store_data(dataset_name, data, processed=processed)

    def list_datasets(self, category_name): # 카테고리 내 모든 table

        if category_name in self.categories:
            return [
                {"dataset_name": name, "metadata": metadata}
                for name, metadata in self.categories[category_name].datasets.items()
            ]
        return f"Category '{category_name}' not found."

    def search_data(self, keyword):

        results = []
        for category_name, category in self.categories.items():
            matches = category.search(keyword) # table명 match
            for dataset_name, metadata in matches:
                results.append({
                    "category": category_name,
                    "dataset_name": dataset_name,
                    "metadata": metadata
                })
        return results

    def retrieve_dataset(self, dataset_name, processed=False, sql_query=None):
        
        return self.data_lake.retrieve_data(dataset_name, processed=processed, sql_query=sql_query)


# BaseDataModel for shared attributes and methods
class BaseDataModel:
    """
    BaseDataModel serves as the foundational class for data models,
    providing shared attributes and utility methods that can be
    inherited by more specialized data models.
    """

    def __init__(self, timestamp, symbol=None):
        """
        Initializes the BaseDataModel with a timestamp and an optional symbol.

        :param timestamp: The timestamp associated with the data model.
                          If not a datetime object, defaults to the current datetime.
        :param symbol: (Optional) A symbol identifier (e.g., stock ticker).
        """
        # Check if 'timestamp' is a datetime object; if not, assign current datetime
        self.timestamp = timestamp if isinstance(timestamp, datetime) else datetime.now()
        # Assign the symbol if provided; else, default to None
        self.symbol = symbol

    def is_recent(self, days=7):
        """
        Determines if the timestamp is within a recent range specified by 'days'.

        :param days: The number of days to consider as the "recent" threshold.
        :return: Boolean indicating if the timestamp is within the recent range.
        """
        # Calculate the time difference between now and the timestamp
        delta = datetime.now() - self.timestamp
        # Return True if the difference is less than or equal to 'days'
        return delta.days <= days

    def is_above_threshold(self, value, threshold):
        """
        Checks if a given value exceeds a specified threshold.

        :param value: The value to be compared against the threshold.
        :param threshold: The threshold value to compare with.
        :return: Boolean indicating if 'value' is greater than 'threshold'.
        """
        return value > threshold

# Optimized DataWorkbench
class DataWorkbench:
    """
    DataWorkbench acts as a centralized storage and processing hub for various datasets.
    It allows storing, retrieving, transforming, and aggregating data, as well as accessing metadata.
    """

    def __init__(self):
        """
        Initializes the DataWorkbench with empty dictionaries for data and metadata storage.
        """
        # Dictionary to store datasets by their names
        self.data_storage = {}
        # Dictionary to store metadata associated with each dataset
        self.metadata_storage = {}

    def store_data(self, dataset_name, data, metadata=None):
        """
        Stores a dataset in the workbench with an optional metadata dictionary.

        :param dataset_name: A unique identifier for the dataset.
        :param data: The actual dataset to be stored (e.g., pandas DataFrame).
        :param metadata: (Optional) Additional information or attributes related to the dataset.
        """
        self.data_storage[dataset_name] = data
        self.metadata_storage[dataset_name] = metadata or {}

    def retrieve_data(self, dataset_name):
        """
        Retrieves a dataset by its name from the workbench.

        :param dataset_name: The unique identifier of the dataset to retrieve.
        :return: The dataset if found; otherwise, None.
        """
        return self.data_storage.get(dataset_name, None)

    def get_metadata(self, dataset_name):
        """
        Retrieves the metadata associated with a specific dataset.

        :param dataset_name: The unique identifier of the dataset.
        :return: Metadata dictionary if found; otherwise, an empty dictionary.
        """
        return self.metadata_storage.get(dataset_name, {})

    def transform_data(self, dataset_name, transformation_func):
        """
        Applies a transformation function to a specified dataset and stores the transformed data.

        :param dataset_name: The name of the dataset to transform.
        :param transformation_func: A function that takes a dataset as input and returns the transformed dataset.
        :return: The transformed dataset.
        :raises ValueError: If the specified dataset is not found.
        """
        # Retrieve the dataset using its name
        data = self.retrieve_data(dataset_name)
        if data is not None:
            # Apply the transformation function to the dataset
            transformed_data = transformation_func(data)
            # Store the transformed data with a new name indicating transformation
            self.store_data(f"{dataset_name}_transformed", transformed_data)
            return transformed_data
        # Raise an error if the dataset does not exist in storage
        raise ValueError(f"Dataset {dataset_name} not found")

    def aggregate_data(self, dataset_name, group_by_column, agg_funcs):
        """
        Aggregates data in a dataset by a specified column using provided aggregation functions.

        :param dataset_name: The name of the dataset to aggregate.
        :param group_by_column: The column name to group the data by.
        :param agg_funcs: A dictionary specifying the aggregation functions for each column.
                          Example: {'Close': 'mean', 'Volume': 'sum'}
        :return: The aggregated dataset as a pandas DataFrame.
        :raises ValueError: If the specified dataset is not found.
        """
        # Retrieve the dataset using its name
        data = self.retrieve_data(dataset_name)
        if data is not None:
            # Perform groupby aggregation based on the provided parameters
            aggregated = data.groupby(group_by_column).agg(agg_funcs)
            # Store the aggregated data with a new name indicating aggregation
            self.store_data(f"{dataset_name}_aggregated", aggregated)
            return aggregated
        # Raise an error if the dataset does not exist in storage
        raise ValueError(f"Dataset {dataset_name} not found")

    def get_statistics(self, dataset_name):
        """
        Computes basic statistical measures for a specified dataset.

        :param dataset_name: The name of the dataset to analyze.
        :return: A pandas Series containing statistical measures like mean, std, min, max, etc.
        :raises ValueError: If the specified dataset is not found.
        """
        # Retrieve the dataset using its name
        data = self.retrieve_data(dataset_name)
        if data is not None:
            # Use pandas' describe method to compute statistics
            return data.describe()
        # Raise an error if the dataset does not exist in storage
        raise ValueError(f"Dataset {dataset_name} not found")


# Optimized Quant Data Models
class IntradayDataModel(BaseDataModel):
    """
    IntradayDataModel extends BaseDataModel to handle intraday stock data.
    It provides methods for aggregating data by time intervals,
    calculating Volume Weighted Average Price (VWAP), and computing moving averages.
    """

    def __init__(self, timestamp, price, volume, symbol):
        """
        Initializes the IntradayDataModel with additional attributes for price and volume.

        :param timestamp: The timestamp associated with the data.
        :param price: The price information (can be detailed per data point).
        :param volume: The volume information (can be detailed per data point).
        :param symbol: The stock symbol (e.g., 'AAPL').
        """
        # Initialize the base attributes using the superclass constructor
        super().__init__(timestamp, symbol)
        # Assign price and volume; can be None initially
        self.price = price
        self.volume = volume

    def aggregate_by_interval(self, data, interval):
        """
        Aggregates intraday stock data by specified time intervals.

        :param data: A pandas DataFrame containing intraday stock data.
                     Must include a 'timestamp' column with datetime information.
        :param interval: The time interval for aggregation (e.g., '5T' for 5 minutes, '1H' for 1 hour).
        :return: A pandas DataFrame with aggregated 'Close' prices and 'Volume'.
        :raises ValueError: If the 'timestamp' column is missing or cannot be converted to datetime.
        """
        # Check if 'timestamp' column exists in the DataFrame
        if 'timestamp' not in data.columns:
            raise ValueError("The 'timestamp' column is missing from the dataset.")
        try:
            # Convert 'timestamp' column to datetime objects
            data['timestamp'] = pd.to_datetime(data['timestamp'])
        except Exception as e:
            # Raise an error if conversion fails
            raise ValueError(f"Error in converting 'timestamp' to datetime: {e}")

        # Set 'timestamp' as the DataFrame index to enable resampling
        data.set_index('timestamp', inplace=True)
        # Resample the data based on the specified interval and aggregate
        aggregated = data.resample(interval.lower()).agg({
            'Close': 'mean',   # Compute the average closing price within the interval
            'Volume': 'sum'    # Compute the total volume within the interval
        })
        # Reset the index to convert 'timestamp' back to a column
        return aggregated.reset_index()

    def calculate_vwap(self, data):
        """
        Calculates the Volume Weighted Average Price (VWAP) for the given data.

        :param data: A pandas DataFrame containing 'Close' and 'Volume' columns.
        :return: The VWAP value as a float.
        """
        # Compute the numerator as the sum of (Close price * Volume) for all data points
        numerator = (data['Close'] * data['Volume']).sum()
        # Compute the denominator as the total Volume
        denominator = data['Volume'].sum()
        # Calculate VWAP; handle division by zero if necessary
        vwap = numerator / denominator if denominator != 0 else 0
        return vwap

    def calculate_moving_average(self, data, window):
        """
        Calculates the moving average of the 'Close' price over a specified window.

        :param data: A pandas DataFrame containing a 'Close' column.
        :param window: The size of the rolling window (e.g., 5 for a 5-period moving average).
        :return: The DataFrame with an additional 'Moving_Average' column.
        """
        # Compute the rolling mean (moving average) for the 'Close' column
        data['Moving_Average'] = data['Close'].rolling(window=window).mean()
        return data

class NewsDataModel(BaseDataModel):
    """
    NewsDataModel extends BaseDataModel to handle news articles data.
    It provides methods for filtering by sentiment, grouping by date,
    and analyzing sentiment trends over time.
    """

    def __init__(self, timestamp, headline, sentiment_score, relevance):
        """
        Initializes the NewsDataModel with additional attributes for headlines,
        sentiment scores, and relevance.

        :param timestamp: The timestamp when the news article was published.
        :param headline: The headline of the news article.
        :param sentiment_score: The sentiment score assigned to the article.
        :param relevance: The relevance score of the article.
        """
        # Initialize the base attributes using the superclass constructor
        super().__init__(timestamp)
        # Assign headline, sentiment score, and relevance
        self.headline = headline
        self.sentiment_score = sentiment_score
        self.relevance = relevance

    def filter_by_sentiment(self, data, threshold):
        """
        Filters news articles based on a minimum sentiment score threshold.

        :param data: A pandas DataFrame containing news data with a 'sentiment_score' column.
        :param threshold: The minimum sentiment score required for an article to be included.
        :return: A pandas DataFrame containing only the articles that meet or exceed the threshold.
        """
        # Apply a boolean mask to filter articles with sentiment_score >= threshold
        filtered_data = data[data['sentiment_score'] >= threshold]
        return filtered_data

    def group_by_date(self, data):
        """
        Groups news articles by their publication date, calculating the count of articles
        and the average sentiment score for each date.

        :param data: A pandas DataFrame containing news data with 'headline', 'timestamp',
                     and 'sentiment_score' columns.
        :return: A pandas DataFrame with 'date', 'article_count', and 'sentiment_score' columns.
        :raises ValueError: If required columns are missing or contain null values.
        """
        # Validate presence and non-nullity of 'headline' column
        if 'headline' not in data.columns or data['headline'].isnull().any():
            raise ValueError("The 'headline' column is missing or contains null values.")
        # Validate presence and non-nullity of 'sentiment_score' column
        if 'sentiment_score' not in data.columns or data['sentiment_score'].isnull().any():
            raise ValueError("The 'sentiment_score' column is missing or contains null values.")

        # Convert 'timestamp' to datetime and extract the date part
        data['date'] = pd.to_datetime(data['timestamp']).dt.date
        # Group by 'date' and aggregate the count of headlines and mean sentiment score
        grouped = data.groupby('date').agg({
            'headline': 'count',               # Count of articles per date
            'sentiment_score': 'mean'          # Average sentiment score per date
        }).rename(columns={'headline': 'article_count'})  # Rename 'headline' to 'article_count'

        # Reset the index to convert 'date' back to a column
        return grouped.reset_index()

    def analyze_sentiment_trend(self, data):
        """
        Analyzes the trend of sentiment scores over time by computing the average sentiment
        score for each date.

        :param data: A pandas DataFrame containing news data with 'timestamp' and 'sentiment_score' columns.
        :return: A pandas DataFrame with 'date' and 'sentiment_score' columns representing the trend.
        """
        # Convert 'timestamp' to datetime and extract the date part
        data['date'] = pd.to_datetime(data['timestamp']).dt.date
        # Group by 'date' and calculate the mean sentiment score for each date
        trend = data.groupby('date')['sentiment_score'].mean().reset_index()
        return trend

# Example display function replacement
def display_dataframe(name, dataframe):
    """
    Displays the name of the DataFrame and its first few rows.

    :param name: A string representing the name or title of the DataFrame.
    :param dataframe: The pandas DataFrame to be displayed.
    """
    print(f"\n{name}:")
    print(dataframe.head())

