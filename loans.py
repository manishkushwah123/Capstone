from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from google.cloud import storage
from datetime import datetime

def load_df_for_table_from_bucket(bucket_name,table_name,ext,schema):
        timestamp=datetime.utcnow()
        
        def delete_files_from_folder(bucket_name,folder_name):
            from google.cloud import storage
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=folder_name)
            for blob in blobs:
                if blob.name!= folder_name:
                        blob.delete()
                        print(f"Deleted blob: {blob.name}")
        def update_last_run(timestamp):
            delete_files_from_folder(bucket_name,f"{table_name}/last_run/")
            data = [(timestamp,)]
            columns = ["last_run"]
            df = spark.createDataFrame(data, columns)
            df.write.csv(f"gs://{bucket_name}/{table_name}/last_run/", header=True, mode="append")
        def read_last_run():
            last_run=spark.read.format("csv").option("recursiveFileLookup", "true") \
                                             .option("header", "true") \
                                             .option("inferSchema", "true") \
                                             .load(f"gs://{bucket_name}/{table_name}/last_run/*.csv")

            return last_run.collect()[0][0]
            
        
        def list_blobs(bucket_name,last_run_time,table_name,files_list=None):
                if files_list is None:
                    files_list=[]
                client = storage.Client()
                bucket = client.get_bucket(bucket_name)
                blobs = bucket.list_blobs()
                for blob in blobs:
                    if blob.updated.replace(tzinfo=None) > last_run_time and blob.name.startswith(f"{table_name}/") and blob.name.endswith(f".{ext}"):
                        if blob.name.startswith(f"{table_name}/last_run/"):
                            continue  # Skip blobs within "last_run" folder
                        else:
                            files_list.append(f"gs://{bucket_name}/{blob.name}")
                print(files_list)
                return files_list
        def read_to_dataframe(file_path):
            df = spark.read.format(ext) \
            .option("header", "true") \
            .schema(schema)\
            .load(file_path)
            return df
        def combine_dataframes_to_single_df(dataframes):
            combined_df = None
            for df in dataframes:
                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = combined_df.union(df)
            return combined_df
        dataframes=[]
        for file in list_blobs(bucket_name,read_last_run(),table_name):
            dataframes.append(read_to_dataframe(file))
        update_last_run(timestamp)
        return combine_dataframes_to_single_df(dataframes)

schema = StructType([
    StructField("Loan_id", StringType(), True),
    StructField("Customer_id", StringType(), True),
    StructField("Current Loan Amount", IntegerType(), True),
    StructField("Term", StringType(), True),
    StructField("Annual Income", DoubleType(), True),
    StructField("Years in current job", StringType(), True),
    StructField("Home Ownership", StringType(), True),
    StructField("Purpose", StringType(), True),
    StructField("Monthly Debt", IntegerType(), True),
    StructField("loan_sanctioned_date", DateType(), True)
])

df=load_df_for_table_from_bucket("bronze-layer-capstone","loans","csv",schema)
if df is not None:
    #REMOVE DUPLICATES and remove nulls
    df=df.dropDuplicates()
    df= df.na.drop(subset='Loan_id')


    print("\nNull values in the DataFrame:")
    for column in df.columns:
        count_null = df.filter(col(column).isNull()).count()
        print(f"{column}: {count_null} null values")


    df= df.na.fill(0, subset=["Current Loan Amount"])


    # +
    # Calculate the mode of the "Term" column
    mode_value = df.groupBy("Term").count().sort(col("count").desc()).first()["Term"]

    # Fill null values with the mode
    df = df.na.fill(mode_value, subset=["Term"])


    # Calculate the mean 
    mean_value = df.select(mean("Annual Income")).collect()[0][0]

    # Fill null values with the mean
    df = df.na.fill(mean_value, subset=["Annual Income"])


    # +
    # Calculate the mode of the "Home Ownership" column
    mode_value = df.groupBy("Home Ownership").count().sort(col("count").desc()).first()["Home Ownership"]

    # Fill null values with the mode
    df = df.na.fill(mode_value, subset=["Home Ownership"])
    # -

    df= df.na.fill("Other", subset=["Purpose"])

    df= df.na.fill(0, subset=["Monthly Debt"])

    # +
    # Specify the desired file name and path
    desired_file_name = "gs://silver-layer-capstone/loans/"
    df.show()

    # Write the cleaned DataFrame to a CSV file with the desired file name
    df.write.csv(desired_file_name, header=True, mode="append")
   
