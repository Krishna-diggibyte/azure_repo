# Databricks notebook source
# DBTITLE 1,import functions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,mount storage dynamic
def mount_storage(storage_account_name,container_name,mount_point):
    AccessKey = dbutils.secrets.get('storagesecretscope', 'assignmentstoragekey')
 
    dbutils.fs.mount(source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
                    mount_point=mount_point,
                    extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": AccessKey})

# COMMAND ----------

# DBTITLE 1,unmount storage
def unmount_storage(path):
    dbutils.fs.unmount(path)

# COMMAND ----------

# DBTITLE 1,reaf function to read csv
def read_csvfile(path):
    read_cust=spark.read.csv(path , header=True)
    return read_cust

# COMMAND ----------

# DBTITLE 1,camel to snake case function
def camel_to_snake(df):
    for column in df.columns:
        new=""
        for i in column:
            if i==' ':
                new=new+'_'
            else:
                new=new+i.lower()
        df=df.withColumnRenamed(column,new)
    return df 

# COMMAND ----------

# DBTITLE 1,function to split name
def split_name(new_cust):
    cust_with_name=new_cust.withColumn("first_name",split(new_cust['name']," ").getItem(0)).withColumn("last_name",split(new_cust['name']," ").getItem(1))
    return cust_with_name

# COMMAND ----------

# DBTITLE 1,extract domain from email
def find_domain(df,source,new_name):
    new_df=df.withColumn("temp_email",split(source,"@").getItem(1)).withColumn(new_name,split('temp_email',"\.").getItem(0)).drop('temp_email')
    
    return new_df

# COMMAND ----------

# DBTITLE 1,add gender column
def find_gender(cust_with_domain):
    cust_with_gender=cust_with_domain.withColumn('new_gender',when(cust_with_domain['gender']=="female","F")
                           .when(cust_with_domain['gender']=="male","M")
                           .otherwise(cust_with_domain['gender'])
                           )
    return cust_with_gender
                           

# COMMAND ----------

# DBTITLE 1,function to split date
def split_date(df,source,new_date,new_time,deli):
    new_df=df.withColumn(new_date,split(df[source],deli).getItem(0)).withColumn(new_time,split(df[source],deli).getItem(1))
    return new_df

# COMMAND ----------

# DBTITLE 1,function to format date
def format_date(df,col_name,pre_format,new_format):
    new_df = df.withColumn(col_name,date_format(to_date(df[col_name],pre_format),new_format))
    return new_df

# COMMAND ----------

# DBTITLE 1,function to add status column
def add_status(cust_with_for_date):
    cust_with_status=cust_with_for_date.withColumn("expenditure-status",when(cust_with_for_date['spent']<200,"MINIMUN")
                                      .otherwise("MAXIMUM"))
    return cust_with_status

# COMMAND ----------

# DBTITLE 1,function to add set category
def set_status(new_product):
    product_with_category=new_product.withColumn("sub_category", when(new_product['category_id']==1,"phone")
                                                            .when(new_product['category_id']==2,'laptop')
                                                            .when(new_product['category_id']==3,'playstation')
                                                            .when(new_product['category_id']==4,'e-device')
                                             )
    return product_with_category

# COMMAND ----------

# DBTITLE 1,save file as delta file
def save_file(df,path):
    df.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

# DBTITLE 1,read delta files
def read_delta(path):
    new_df=spark.read.format('delta').load(path)
    return new_df