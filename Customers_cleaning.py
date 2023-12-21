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
# Create a Spark session
spark = SparkSession.builder.appName("AvroExample").getOrCreate()

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
    StructField("Customer_id", StringType(), True),
    StructField("Full_Name", StringType(), True),
    StructField("Customer_Email", StringType(), True),
    StructField("dob", StringType(), True),  # Convert to DateType
    StructField("Customer_Phone", IntegerType(), True)  # Convert to IntegerType
])


customers_df = load_df_for_table_from_bucket("bronze-layer-capstone","customers","csv",schema)

if customers_df is not None:
        cleaned_data = customers_df.withColumn('dob',date_format(to_date(col("dob"),"M/d/yyyy"),"MM/dd/yyyy"))
        
        
        # Handle missing values
        cleaned_customer_data = cleaned_data.fillna({'Full_Name': 'Unknown', 'Customer_Email': 'Unknown','Customer_Phone': 0})
        
        # Handle duplicates
        cleaned_customer_data = cleaned_data.dropDuplicates(["Customer_id"])
        
        # Replace invalid phone numbers with null values
        cleaned_customer_data = cleaned_data.withColumn('Customer_Phone', when(length(col('Customer_Phone')) == 10, col('Customer_Phone')).otherwise(None))
        
        
        cleaned_customer_data1 = cleaned_customer_data.withColumn('Full_Name', initcap('Full_Name'))
        
        cleaned_customer_data2 = cleaned_customer_data1.withColumn("email_check", regexp_extract(col("Customer_Email"), r'^\S+@\S+\.\S+', 0))
        
        
        cleaned_customer_data2 = cleaned_customer_data2.withColumn("Customer_Email", when(col("email_check") != "", col("Customer_Email")).otherwise(None))
        
        # Drop the temporary 'email_check' column
        cleaned_customer_data2 = cleaned_customer_data1.drop("email_check")
        
        # Display the DataFrame with null values for invalid email addresses
        cleaned_customer_data2.show()
        
        # +
        # Specify the desired file name and path
        desired_file_name = "gs://silver-layer-capstone/customers/"
        
        # Write the cleaned DataFrame to a CSV file with the desired file name
        cleaned_customer_data2.write.csv(desired_file_name, header=True, mode="append")
        # -
