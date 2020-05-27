import sys
import json # per lavorare json
import pyspark #per lavare pyspark
from pyspark.sql.functions import col, collect_list, concat, to_date, substring, lit, concat_ws, unix_timestamp, collect_set #alcune funzioni particolari di pyspark

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

#leggo i dati ed inizializzare il job


##### FROM FILES
tedx_dataset_path = "s3://tcm-bucket-tedx/tedx_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#spark sessione di spark presa da glue
#abbiamo gli header
#parametri per fare il quote per definire che stringhe dentro gli apici
#e gli escape
#.csv perchè csv se no json ecc volendo potrei cambiare il separator con il .separator e metto ecc

#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read.option("header","true").option("quote", "\"").option("escape", "\"").csv(tedx_dataset_path) 

    
tedx_dataset.printSchema() #stampo le mie variabili

#carico l'altro file

#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count() #conto i dati
count_items_null = tedx_dataset.filter("idx is not null").count() # e capisco se ho qualche dato che è nullo

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ TAGS DATASETs3://tcm-bucket-tedx/tags_dataset.csv
tags_dataset_path = "s3://tcm-bucket-tedx/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

## READ WATCH DATASET
watch_dataset_path = "s3://tcm-bucket-tedx/watch_next_dataset.csv"
watch_dataset = spark.read.option("header","true").csv(watch_dataset_path)


# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags")) #raggruppo i dati, su cui posso fare una funzione di aggregazione
tags_dataset_agg.printSchema() #per controllare lo schema per vedere se è giusto

 #join fra dataset iniziale e quello dei tag
 #perchè voglio che i miei dati siano indicizzati sul mio _id (e non che mongo si ricordi le sue cose)
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left").drop("idx_ref").select(col("idx").alias("_id"), col("*")).drop("idx")
tedx_dataset_agg.printSchema()

       
pattern = "dd-MMM-yyyy"

#watch_dataset_agg = watch_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("watch_next_idx"), collect_list("url"))
watch_dataset_agg = watch_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_set("watch_next_idx").alias("wnext"), collect_set("url").alias("wnurl"))
watch_dataset_agg.select(concat(col("wnext"),col("wnurl")))
tdw = tedx_dataset_agg.join(watch_dataset_agg, tedx_dataset_agg._id == watch_dataset_agg.idx_ref , "left").select(col("_id"), col("*")).drop("idx_ref")
tdw2 = tdw.withColumn("day", lit("01"))
tdw3 = tdw2.withColumn("data_app", concat_ws("-",tdw2.day, substring(tdw2.posted,8,3), substring(tdw2.posted,12,4)))
tdw4 = tdw3.withColumn("data", unix_timestamp(tdw3.data_app, pattern).cast("timestamp"))
tdw5 = tdw4.select(col("_id"), col("*")).drop("day").drop("posted").drop("data_app")
tedx_dataset_watch_ord = tdw5.orderBy("data")

#copio  i dati su MongoDB(imposto le opzioni)
mongo_uri = "mongodb://clustercloudmobile-shard-00-00-x0zgo.mongodb.net:27017,clustercloudmobile-shard-00-01-x0zgo.mongodb.net:27017,clustercloudmobile-shard-00-02-x0zgo.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,#indirizzo
    "database": "unibg_tedx",#nome db
    "collection": "tedz_data",#collezioni
    "username": "XXXXX",
    "password": "XXXXX",
    "ssl": "true",
    "ssl.domain_match": "false"}
    
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_watch_ord, glueContext, "nested")#converto il db glue in un spark (?)

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)#vado a scrivere

