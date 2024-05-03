# Databricks notebook source
# DBTITLE 1,run utils
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,mount bronze
# storage_account_name = "storage4adfassignment"
# container_name = "bronze"
# mount_point = '/mnt/bronze'
# mount_storage(storage_account_name,container_name,mount_point)

# COMMAND ----------

# DBTITLE 1,read customer file
path='dbfs:/mnt/bronze/sales_view/customer/20240107_sales_customer.csv'
read_cust=read_csvfile(path)
# display(read_cust)

# COMMAND ----------

# DBTITLE 1,camel case to snake  in customer data
new_cust=camel_to_snake(read_cust)
# display(new_cust) 

# COMMAND ----------

# DBTITLE 1,split name into first name and last name
cust_with_name=split_name(new_cust)
display(cust_with_name)

# COMMAND ----------

# DBTITLE 1,extract domain from email id
cust_with_domain=find_domain(cust_with_name,'email_id','domain')
display(cust_with_domain)


# COMMAND ----------

# DBTITLE 1,add new gender as M for male and F for female
cust_with_gender=find_gender(cust_with_domain)
display(cust_with_gender)

# COMMAND ----------

# DBTITLE 1,split date and time column
cust_with_date=split_date(cust_with_gender,'joining_date',"date","time"," ")
cust_with_date.display()

# COMMAND ----------

# DBTITLE 1,format date to yyyy-MM-dd
cust_with_for_date=format_date(cust_with_date,'date', "dd-MM-yyyy","yyyy-MM-dd")
display(cust_with_for_date)

# COMMAND ----------

# DBTITLE 1,add status MINIMUM as MaXIMUM
cust_with_status=add_status(cust_with_for_date)
display(cust_with_status)

# COMMAND ----------

# DBTITLE 1,format date to yyyy-MM-dd
cust_with_reg_date=format_date(cust_with_status,'registered', "dd-MM-yyyy","yyyy-MM-dd")
display(cust_with_reg_date)

# COMMAND ----------

# DBTITLE 1,mount silver container
storage_account_name = "storage4adfassignment"
container_name = "silver"
mount_point = '/mnt/silver'
mount_storage(storage_account_name,container_name,mount_point)

# COMMAND ----------

# DBTITLE 1,save customer as delta table
path='dbfs:/mnt/silver/sales_view/customer'
save_file(cust_with_reg_date,path)

# COMMAND ----------

# DBTITLE 1,read customer table
d11f=spark.read.format('delta').load('dbfs:/mnt/silver/sales_view/customer')
d11f.display()