# Databricks notebook source
# DBTITLE 1,run utils
# MAGIC %run ../bronze_to_silver/utils

# COMMAND ----------

# DBTITLE 1,read product table
path='dbfs:/mnt/silver/sales_view/product'
read_product_table=read_delta(path)
display(read_product_table)

# COMMAND ----------

# DBTITLE 1,read store table
path='dbfs:/mnt/silver/sales_view/store'
read_store_table=read_delta(path)
display(read_store_table)

# COMMAND ----------

# DBTITLE 1,read sales table
path='dbfs:/mnt/silver/sales_view/customer_sales'
read_sale_table=read_delta(path)
display(read_sale_table)

# COMMAND ----------

# DBTITLE 1,join store and product table
product_join_store=read_product_table.join(read_store_table,read_product_table['store_id']==read_store_table['store_id'],'inner')

new_product_join_store=product_join_store.select(read_store_table.store_id,'store_name','location','manager_name','product_code','description','category_id','price','stock_quantity','supplier_id',read_product_table.created_at,read_product_table.updated_at,'image_url','weight','expiry_date','is_active','tax_rate')
display(new_product_join_store)

# COMMAND ----------

# DBTITLE 1,join customer with above table
data_with_customer=product_join_store.join(read_sale_table,read_sale_table['product_id']==product_join_store['product_id'],'inner')
new_data_with_customer=data_with_customer.select("order_date","category","city","customerid","orderid",product_join_store.product_id,"profit","region","sales","segment","ship_date","shipmode","latitude","longitude","store_name","location","manager_name","product_name","price","stock_quantity","image_url" )

# COMMAND ----------

# DBTITLE 1,mount gold container
# storage_account_name = "storage4adfassignment"
# container_name = "gold"
# mount_point = '/mnt/gold'
# mount_storage(storage_account_name,container_name,mount_point)

# COMMAND ----------

# DBTITLE 1,write in gold layer
path='dbfs:/mnt/gold/sales_view/StoreProductSalesAnalysis'
save_file(new_data_with_customer,path)

# COMMAND ----------

# DBTITLE 1,unmount
# unmount_storage('/mnt/bronze')
# unmount_storage('/mnt/silver')
# unmount_storage('/mnt/gold')