# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.0
#   kernelspec:
#     display_name: PySpark (Local)
#     language: python
#     name: pyspark
# ---

# +
import pyspark


def load_df_for_table_from_bucket(bucket_name,table_name,ext):
        def fetch_last_run_and_update_last_run(bucket_name,table_name):
            last_run=spark.read.format("csv").option("recursiveFileLookup", "true") \
                                             .option("header", "true") \
                                             .option("inferSchema", "true") \
                                             .load(f"gs://{bucket_name}/{table_name}/last_run/*.csv")
            last_run = last_run.withColumn("Last_run_timestamp", current_timestamp())
            last_run.write.format("csv").mode("overwrite").option("header","true").save(f"gs://{bucket_name}/{table_name}/last_run/")

            return last_run.collect()[0][0]
            
        def list_blobs(bucket_name,last_run_time,table_name,files_list=None):
                if files_list is None:
                    files_list=[]
                client = storage.Client()
                bucket = client.get_bucket(bucket_name)
                blobs = bucket.list_blobs()
                for blob in blobs:
                    if blob.updated.replace(tzinfo=None) > last_run and blob.name.startswith(f"{table_name}/") and blob.name.endswith(f".{ext}"):
                        if blob.name.startswith(f"{table_name}/last_run/"):
                            continue  # Skip blobs within "last_run" folder
                        else:
                            files_list.append(f"gs://{bucket_name}/{blob.name}")
                return files_list
        def read_to_dataframe(filepath,ext):
            df = spark.read.format(ext) \
            .option("header", "true") \
            .option("inferSchema", "true") \
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
        for file in list_blobs(bucket_name,fetch_last_run_and_update_last_run(bucket_name,table_name),table_name):
            dataframes.append(read_to_dataframe(file))
        return combine_dataframes_to_single_df(dataframes)




# -

customers_file_path = "gs://inputbucket01/customers.csv"  # Replace with your CSV file path
customers_csv = spark.read.option("header", "true").csv(customers_file_path)


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

## to check for the presence of missing/null valuesin the customer_id column
missing_ids = credits_csv.select("Customer_id").subtract(customers_csv.select("Customer_id"))
print(f"The total customer_ids that are not present in customers_csv is : {missing_ids.count()}")


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



# -

credits_csv.show(2,truncate=False)

desired_file_path="gs://silver_layer-capstone/credits/"
credits_csv.write.csv(desired_file_path,header=True,mode="append")
