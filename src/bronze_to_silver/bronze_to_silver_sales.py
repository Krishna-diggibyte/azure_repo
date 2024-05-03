# Databricks notebook source
# DBTITLE 1,run utils file
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,read sales file
path='dbfs:/mnt/bronze/sales_view/sales/20240107_sales_data.csv'
read_sales=read_csvfile(path)
display(read_sales)

# COMMAND ----------

# DBTITLE 1,convert camel to snake case in sales file
new_sales=camel_to_snake(read_sales)
display(new_sales)

# COMMAND ----------

# DBTITLE 1,split orderdate into date and time
sales_with_order_date=split_date(new_sales,'orderdate',"order_date","order_time","T")
display(sales_with_order_date)

# COMMAND ----------

# DBTITLE 1,split shipdate into date and time
sales_with_ship_date=split_date(sales_with_order_date,'shipdate',"ship_date","ship_time","T")
display(sales_with_ship_date)

# COMMAND ----------

# DBTITLE 1,save sales file as delta table
path='dbfs:/mnt/silver/sales_view/customer_sales'
save_file(sales_with_ship_date,path)


# COMMAND ----------

# DBTITLE 1,read sales delta table
new_sales=spark.read.format('delta').load(path)
display(new_sales)