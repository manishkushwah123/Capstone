#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[78]:


from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Define file paths
accounts_path = "gs://capstone_gcs/silver_layer_Accounts.csv_part-00000-dc715b15-1843-4d82-b181-a588219c30e0-c000.csv"
branches_path = "gs://capstone_gcs/cleaned_data_cleaned_branches_data.csv_part-00000-3e041cad-54b5-4d20-aa74-e04f0f843282-c000.csv"
customers_path = "gs://capstone_gcs/silver_layer_cleaned_customers.csv"
loans_partition_path = "gs://capstone_gcs/silver_layer_loans.csv_part-00000-25ab85f9-197b-427c-9b4a-e3618106aefc-c000.csv"
credit_json_path = "gs://capstone_gcs/silver_layer_credits.csv_part-00000-4baad905-515e-4c32-bd74-e2407f0d35ad-c000.csv"
transactions="transactions.csv"

# Read CSV files into DataFrames
df_accounts = spark.read.format("csv").option("header", "true").load(accounts_path)
df_branches = spark.read.format("csv").option("header", "true").load(branches_path)
df_customers = spark.read.format("csv").option("header", "true").load(customers_path)
df_transactions=spark.read.format("csv").option("header", "true").load(transactions)

# Read a specific partitioned CSV file into DataFrame
df_loans_partition = spark.read.format("csv").option("header", "true").load(loans_partition_path)

# Read a JSON file into a DataFrame
df_credit_json = spark.read.format("csv").option("header", "true").load(credit_json_path)




# In[102]:


# KPI-1 -- No of business , current , Savings account
from pyspark.sql.functions import *
fact1=df_accounts.join(df_branches,df_accounts["branch_id"]==df_branches["BranchID"],how="right")
pivot_df = fact1.groupBy(["branch_id",concat(col("Bank_Name"),lit(" "),col("Bank_city")).alias("Branch")]).pivot("AccountType").agg({"AccountType": "count"})

pivot_df.show()


# In[25]:


# KPI--2 Customer Engagement - Average transaction per customer
kpi1=df_customers.join(df_accounts,df_customers["Customer_id"]==df_accounts["Customer_id"],how="inner")
kpi1=kpi1.join(df_transactions,kpi1["AccountId"]==df_transactions["AccountID"],how="inner")
result = kpi1.groupBy("Full_Name").agg(sum("Amount").alias("total_transactions"))
result.show()


# In[30]:


#Kpi-3  Find the number of accounts opened in different branches

kpi2=df_accounts.join(df_branches,df_accounts["branch_id"]==df_branches["BranchId"],how="inner")
kpi2=kpi2.groupBy(["BranchId",concat(col("Bank_Name"),lit(" "), col("Bank_City")).alias("Bank_name")]).count()
kpi2 = kpi2.withColumnRenamed("count", "count_of_accounts")
kpi2.show()


# In[41]:


#kpi4 Total Number of accounts
kpi4 =df_customers.join(df_accounts,on="Customer_id",how="inner")
kpi4=kpi4.groupby(["Customer_id","Full_Name"]).count()
kpi4=kpi4.withColumnRenamed("count","Total_no_of_accounts")
kpi4=kpi4.select(["Full_Name","Total_no_of_accounts"])
kpi4.show()


# In[81]:


df_loans_partition=df_loans_partition.withColumn("Monthly Debt",col("Monthly Debt").cast("double"))
df_credit_json=df_credit_json.withColumn("Credit Score",col("Credit Score").cast("double"))


# In[90]:


# kpi--5 Loan Analysis
kpi5=df_customers.join(df_loans_partition,on="Customer_id")
df=kpi5.groupby(["Customer_id","Full_Name"]).sum("Monthly Debt")
df=df.withColumnRenamed("sum(Monthly Debt)","Monthly Debt")


df=df.join(df_credit_json,on="Customer_id",how="inner")
df = df.withColumn("Debt_Income_Ratio", (col("Monthly Debt")/(col("Annual Income") / 12)) )
df=df
df=df.select(["Full_Name","Debt_Income_Ratio","Credit Score","Number of Open Accounts"])
df.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




