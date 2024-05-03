# Databricks notebook source
# DBTITLE 1,read utils file
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,read store file
path='dbfs:/mnt/bronze/sales_view/store/20240107_sales_store.csv'
read_store=read_csvfile(path)
display(read_store)

# COMMAND ----------

# DBTITLE 1,camel to snake in store file
new_store=camel_to_snake(read_store)
new_store.display()

# COMMAND ----------

# DBTITLE 1,extract store category from email address
store_with_domain=find_domain(new_store,'email_address','store_category')
display(store_with_domain)

# COMMAND ----------

# DBTITLE 1,format date to yyyy-MM-dd
store_with_created_date=format_date(store_with_domain,'created_at','dd-MM-yyyy','yyyy-MM-dd')
display(store_with_created_date)

# COMMAND ----------

# DBTITLE 1,format date to yyyy-MM-dd
store_with_updated_date=format_date(store_with_created_date,'updated_at','dd-MM-yyyy','yyyy-MM-dd')
display(store_with_updated_date)

# COMMAND ----------

# DBTITLE 1,save store file as delta table
path='dbfs:/mnt/silver/sales_view/store'
save_file(store_with_updated_date,path)

# COMMAND ----------

# DBTITLE 1,read store delta table
new_read=spark.read.format('delta').load(path)
display(new_read)