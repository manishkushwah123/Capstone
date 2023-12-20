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

schema=StructType([
    StructField("Customer_id", StringType(), True),
    StructField("Credit Score", DoubleType(), True),
    StructField("Annual Income", DoubleType(), True),
    StructField("Years of Credit History", DoubleType(), True),
    StructField("Number of Open Accounts", DoubleType(), True),
    StructField("Number of Credit Problems", DoubleType(), True),
    StructField("Current Credit Balance", DoubleType(), True),
    StructField("Maximum Open Credit", DoubleType(), True),
    StructField("Bankruptcies", DoubleType(), True),
    StructField("Tax Liens", DoubleType(), True),
    StructField("Months since last delinquent", DoubleType(), True)  # Add "Months since last delinquent" field
])



credits_csv = load_df_for_table_from_bucket("bronze-layer-capstone","credits","json",schema)

if credits_csv is not None:        
        credits_csv=credits_csv.select(
            "Customer_id",
            "Credit Score",
            "Annual Income",
            "Years of Credit History",
            "Months since last delinquent",
            "Number of Open Accounts",
            "Number of Credit Problems",
            "Current Credit Balance",
            "Maximum Open Credit",
            "Bankruptcies",
            "Tax Liens"
        )
        
        
        
        ## droping the duplicated rows
        credits_csv=credits_csv.dropDuplicates()
        # dropping the null valued rows based on customer_id
        credits_csv=credits_csv.na.drop(subset="Customer_id")
        
        # +
        #replace nulls in credit Score with -1
        credits_csv=credits_csv.na.fill(-1,subset="Credit Score")
        
        #replace nulls with avg Anuual Income in Annual Income column
        avg_income=credits_csv.select(mean("Annual Income")).collect()[0][0]
        credits_csv=credits_csv.na.fill(avg_income,subset="Annual Income")
        
        #replace nulls in "|Years of Credit History|Months since last delinquent|Number of Open Accounts|Number of Credit Problems|Current Credit Balance|Maximum Open Credit|Bankruptcies|Tax Liens" with zero"
        credits_csv=credits_csv.na.fill(0,subset=["Years of Credit History",
                                                  "Months since last delinquent",
                                                  "Number of Open Accounts",
                                                  "Number of Credit Problems",
                                                  "Current Credit Balance",
                                                  "Maximum Open Credit",
                                                  "Bankruptcies",
                                                  "Tax Liens"])
        
        
        credits_csv.show()
        desired_file_path="gs://silver-layer-capstone/credits/"
        credits_csv.write.csv(desired_file_path,header=True,mode="append")
