# 🚖 Intelligent Data Pipeline for NYC Yellow Taxi Data

This documentation was generated using a local LLM via Ollama.

Below is a step-by-step explanation of the data pipeline implemented in `pipeline.py`.

## Step 1: Read yellow_tripdata_2022-01.parquet into a Spark DataFrame

Step 1: Data Reading (Ingestion)

To read the `yellow_tripdata_2022-01.parquet` file (downloaded from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) using Apache Spark, follow these steps below:

1. Import required libraries and create a SparkSession to work with the Spark DataFrames:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Yellow Taxi Trips ETL") \
    .getOrCreate()
```

2. Define the path where the Parquet file is located:

```python
data_path = "path/to/your-data/yellow_tripdata_2022-01.parquet"
```

3. Read the specified data file into a Spark DataFrame, which will be stored as an RDD (Resilient Distributed Dataset) under the hood:

```python
df = spark.read.format("parquet") \
          .option("header", "true") \  # If the file contains headers
          .load(data_path)
```

Now, you have a Spark DataFrame named `df` containing the yellow taxi trip data for January 2022. This step is crucial because it sets up the foundation for further data processing and analysis in subsequent stages of the ETL pipeline.

## Step 2: Filter out records with passenger_count <= 0 or trip_distance <= 0

Step 2: Filtering Out Invalid Records

In this step of the ETL (Extract, Transform, Load) pipeline, we aim to filter out records that contain invalid data for `passenger_count` and `trip_distance`. The filtering condition is to exclude any record where either `passenger_count` is less than or equal to 0 (zero), or `trip_distance` is less than or equal to 0.

Here's a simple example of how this can be done using Python and the pandas library:

```python
import pandas as pd

# Assuming df is your DataFrame containing the source data
df = pd.read_csv('source.csv')

# Filter out records with invalid passenger_count or trip_distance
valid_records = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]
```

In this code snippet, we read the source data from a CSV file into a DataFrame called `df`. Then, using boolean indexing, we filter out records that meet the condition of having a `passenger_count` greater than 0 and a `trip_distance` greater than 0. The resulting DataFrame, `valid_records`, contains only the valid records and can be used for further processing in subsequent stages of the ETL pipeline.

Please note that this example uses Python with pandas; you may need to adjust the code depending on your programming language, data format, or ETL tool of choice. The goal remains the same: filter out any record containing invalid data for `passenger_count` or `trip_distance`.

## Step 3: Compute a new column fare_per_mile = fare_amount / trip_distance

**Step 3: Computing the Fare Per Mile**

In this stage of our ETL (Extract, Transform, Load) pipeline, we will create a new column called `fare_per_mile` by dividing the existing `fare_amount` column with the `trip_distance` column. Here's how it will look in Python code:

```python
# Assuming df is your DataFrame containing 'fare_amount' and 'trip_distance' columns
df['fare_per_mile'] = df['fare_amount'] / df['trip_distance']
```

This operation adds a new column to the DataFrame, `fare_per_mile`, where each row contains the cost per mile for that particular trip. The result will help us analyze and understand the cost efficiency of each ride in our dataset.

## Step 4: Group by passenger_count and compute average fare_per_mile

Step 4: Aggregate Data and Compute Average Fare Per Mile (Group By & Avg)

In this step, we will group the data by passenger count and then compute the average fare per mile. This step is essential to gain insights about how the fare-per-mile varies based on the number of passengers.

Here's a Python example using pandas library:

```python
# Assuming 'data' is your DataFrame and it contains columns: 'passenger_count', 'fare', and 'miles'

# Step 1: Group the data by passenger count
grouped_data = data.groupby('passenger_count')

# Step 2: Iterate through each group to calculate average fare per mile
average_fares_per_mile = {}
for name, group in grouped_data:
    # Calculate the average fare and average miles for this group
    avg_fare = group['fare'].mean()
    avg_miles = group['miles'].mean()

    # Compute the average fare per mile
    avg_fare_per_mile = avg_fare / avg_miles if avg_miles > 0 else None

    # Store the result in a dictionary for easy access later
    average_fares_per_mile[name] = avg_fare_per_mile
```

After executing this step, you will have `average_fares_per_mile` dictionary where keys represent passenger counts and values are the corresponding average fare per mile. This data can be used to analyze patterns and trends in your ETL pipeline.

## Step 5: Write the result to CSV and Parquet in the 'output/' directory

Step 5: Writing Result to CSV and Parquet in the 'output/' Directory

In this step, we will write the processed data from our ETL pipeline to both CSV and Parquet files and store them in the specified output directory. Here is an example of how this can be accomplished using Python and popular libraries like Pandas and PyArrow.

1. Import necessary libraries:

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
```

2. Create a DataFrame containing the processed data:

Assuming you have already processed your data and it's available in a DataFrame `df`.

```python
df = process_data(input_df)  # Replace 'process_data(input_df)' with your custom ETL function that processes input data.
```

3. Write the result to CSV:

```python
output_csv = f'output/{output_filename}.csv'
df.to_csv(output_csv, index=False)
```

4. Write the result to Parquet:

```python
output_parquet = f'output/{output_filename}.parquet'
pa.Table.from_pandas(df).write_file(output_parquet)
```

5. Save the created files as a table in HDFS (Hadoop Distributed File System) or another storage system if needed:

This step is optional and depends on your project's requirements. To save the created files in an HDFS, you can use the following libraries: PyArrow and pydooq.

```python
from pyarrow.hdfs import HDFSFileSystem, FileSystemOptions
from pyarrow.parquet import ParquetFile
from pydoocq.hive import HiveClient

# Connect to HDFS or Hive
fs = HDFSFileSystem(options=FileSystemOptions())
hive_client = HiveClient()

# Save Parquet file to HDFS
with pq.ParquetFile(output_parquet) as table:
    fs.putfile(table.serialize(), f'{fs.get_default_uri()}/{output_filename}.parquet')

# Save CSV file to Hive
csv_data = pd.read_csv(output_csv, header=None).to_records(index=False)
hive_table = hive_client.create_table(output_table, csv_data)
hive_client.insert_into_table(hive_table, csv_data)
```

---
### 📁 Output
- `output/aggregated_results.parquet` – Cleaned and aggregated data
- `output/output.csv` – Same data in CSV format for dashboard use

### 📊 Dashboard
- Built with **Streamlit** and **Plotly**
- Located in `streamlit_app.py`
- Auto-refresh supported for data updates (if implemented)

### 🚀 How to Run
```bash
python pipeline.py
streamlit run streamlit_app.py
```
