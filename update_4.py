#!/usr/bin/env python3
from pyspark.sql import SparkSession


# Do updates work in SparkSQL?

spark = SparkSession.builder \
    .master("local") \
    .appName('learn_spark') \
    .getOrCreate()
    #.config("spark.some.config.option", "some-value") \

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
spark.sql("DROP TABLE if exists learn_spark_db.visits")

print("1 load visits ==================================")

csv_file = "visits.csv"
schema=" id INT, start_date DATE, end_date DATE, sub_id INT"
visits_df = spark.read.csv(csv_file, schema=schema)
visits_df.write.mode("overwrite").saveAsTable("visits")

print("2 show visits ==================================")

sql='''
    SELECT *
    FROM visits
'''
print(sql)
df = spark.sql(sql)
print(df.count())
df.show(n=100,truncate=False);


sql='''
    UPDATE visits set sub_id = -9 where id = 11
'''
print(sql)
df = spark.sql(sql)
print(df.count())
df.show(n=100,truncate=False);

spark.stop()
