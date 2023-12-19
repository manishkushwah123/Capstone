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
from pyspark.sql.functions import *
# Read JSON file into a DataFrame
credits_file_path = "gs://inputbucket01/credit.json"  # Replace with your JSON file path
credits_csv = spark.read.json(credits_file_path)




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

desired_file_path="gs://capstondata/silver_layer/credits.csv"
credits_csv.write.csv(desired_file_path,header=True,mode="append")
