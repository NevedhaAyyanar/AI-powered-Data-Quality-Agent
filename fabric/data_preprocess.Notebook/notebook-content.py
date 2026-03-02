# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dca2f9e1-bd39-45f5-b70f-849bf07ca21f",
# META       "default_lakehouse_name": "portfolio_lakehouse",
# META       "default_lakehouse_workspace_id": "e748d594-cc84-40e7-a05b-2240ac9c7241",
# META       "known_lakehouses": [
# META         {
# META           "id": "dca2f9e1-bd39-45f5-b70f-849bf07ca21f"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "7c5f2734-e2d8-4977-95b0-3632c36cca9a",
# META       "known_warehouses": [
# META         {
# META           "id": "7c5f2734-e2d8-4977-95b0-3632c36cca9a",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
salesDf = spark.read.table("portfolio_lakehouse.Sales.settleddata")
display(salesDf.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df_date_change = salesDf.withColumns(
    {"SettlementDate": to_date(col("SettlementDate"),"dd-MM-yyyy"),
    "CALDAT": to_date(col("CALDAT"),"dd-MM-yyyy"),
    "DELDAT": to_date(col("DELDAT"),"dd-MM-yyyy"),
    "CREDAT": to_date(col("CREDAT"),"dd-MM-yyyy"),
    "UPDDAT": to_date(col("UPDDAT"),"dd-MM-yyyy")
})
#df_date_change.printSchema()
df_date_change.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("portfolio_lakehouse.Sales.settleddata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM portfolio_lakehouse.Sales.settleddata LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
