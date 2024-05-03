# Databricks notebook source
# DBTITLE 1,run utils file
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,read product file
path='dbfs:/mnt/bronze/sales_view/product/20240107_sales_product.csv'
read_product=read_csvfile(path)
display(read_product)

# COMMAND ----------

# DBTITLE 1,camel to snake case in product file
new_product= camel_to_snake(read_product)
display(new_product)

# COMMAND ----------

# DBTITLE 1,set sun category
def set_status(new_product):
    product_with_category=new_product.withColumn("sub_category", when(new_product['category_id']==1,"phone")
                                                            .when(new_product['category_id']==2,'laptop')
                                                            .when(new_product['category_id']==3,'playstation')
                                                            .when(new_product['category_id']==4,'e-device')
                                             )
    return product_with_category


product_with_category= set_status(new_product)
product_with_category.display()

# COMMAND ----------

# DBTITLE 1,format date to yyyy-MM-dd
product_with_created_date=format_date(product_with_category,'created_at', "dd-MM-yyyy","yyyy-MM-dd")
display(product_with_created_date)

# COMMAND ----------

# DBTITLE 1,format date to yyyy-MM-dd
product_with_updated_date=format_date(product_with_created_date,'updated_at', "dd-MM-yyyy","yyyy-MM-dd")
display(product_with_updated_date)

# COMMAND ----------

# DBTITLE 1,save product file as delta table
path=r'dbfs:/mnt/silver/sales_view/product'
save_file(product_with_updated_date,path)

# COMMAND ----------

# DBTITLE 1,read product table
new_read=spark.read.format('delta').load(path)
display(new_read)