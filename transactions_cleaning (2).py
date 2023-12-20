#!/usr/bin/env python
# coding: utf-8

# In[27]:


from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder         .getOrCreate()







# In[28]:


# Defining the structure/schema of the data 


from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("AccountID", StringType(), True),
    StructField("TransactionOperation", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("Amount", DoubleType(), True)
])
df = spark.read.csv(file_path, header=True, schema=schema)


# In[ ]:





# In[ ]:





# In[30]:


#Checking if there are any null values in the dataframe
from pyspark.sql.functions import *
null_counts = [df.where(col(c).isNull()).count() for c in df.columns]
null_counts


# In[31]:


# Checking for the types of data
dfs.dtypes


# In[32]:


# Dropping the columns where there are null values in the following columns
df_filtered = df.na.drop(subset=["transaction_id","Amount","AccountId"])


# In[33]:


# replacing  the null values with unknown
df_filled = df.fillna("unknown", subset=["TransactionOperation", "transaction_type"])


# In[45]:


#converting the string to upper and removing any spaces 

df_transformed = df.withColumn("transaction_type", upper(trim(col("transaction_type"))))
df_transformed = df_transformed.withColumn("TransactionOperation", upper(trim(col("TransactionOperation"))))


# In[44]:





# In[ ]:




