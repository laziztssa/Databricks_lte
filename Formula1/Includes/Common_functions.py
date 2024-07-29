# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df=input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

#fonction de reccuperation de schema avec la partition en derierre position
def re_arrange_partition_column(input_df, partition_column):
    c 
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

# fonction pour gerer les alimentaion 
def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverWriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("append").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

#configurationn pour optimisation de gestion des partition
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
# import de la biliotheque pour les deltaTable
from delta.tables import DeltaTable
#definition de la focntion avec ces arguments input
def overwrite_delta_mode(input_df, db_name, table_name, path_name, partition_column, merge_condition):
    #test d'existance de la table + operation de merge
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{path_name}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"), f"{merge_condition}") \
                .whenMatchedUpdateAll()\
                .whenNotMatchedInsertAll()\
                .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
