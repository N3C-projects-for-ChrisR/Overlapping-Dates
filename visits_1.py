#!/usr/bin/env python3

#### rm -rf  spark-warehouse/learn_spark_db.db/visits
 
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName('learn_spark') \
    .getOrCreate()
    #.config("spark.some.config.option", "some-value") \

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
#spark.sql("DROP TABLE if exists learn_spark_db.visits")
csv_file = "visits.csv"
schema=" id INT, start_date DATE, end_date DATE, sub_id INT"
visits_df = spark.read.csv(csv_file, schema=schema)
visits_df.write.mode("overwrite").saveAsTable("visits")


# plain
visits_df = spark.sql("SELECT * from visits ") 
visits_df.show();



# try to get sequential max end dates???
sql='''
    SELECT id, start_date, end_date,
           max(end_date) over (partition by  id order by end_date) as max_end_date,
           rank() over (partition by  id order by end_date) as idx,
           sum(id) over (partition by  id order by end_date) as sum_id,
           max(id) over (partition by  id order by end_date) as max_id,
           sum(sub_id) over (partition by  id order by end_date) as sum_sub_id,
           max(sub_id) over (partition by  id order by end_date) as max_sub_id,
           avg(sub_id) over (partition by  id order by end_date) as max_sub_id
    FROM visits
'''
print(sql)
df = spark.sql(sql)
print(df.count())
df.show();





spark.stop()
