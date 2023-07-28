#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Do updates in Python, not SQL.

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

#print("2 show visits ==================================")
sql="SELECT * FROM visits "
print(sql)
df = spark.sql(sql)
print(df.count())
#df.show(n=100,truncate=False);


print("3 update in python =====================")
print("3.1 add the new value column")
#    UPDATE visits set sub_id = -9 where id = 11
# create new column with conditional values
df = df.withColumn("new_sub-id", F.when(F.col("id") == 11, -9).otherwise(F.col("id")) ) 
# remove old column
df = df.drop("sub_id")
# rename
df = df.withColumnRenamed("new_sub-id", "sub_id")
df.show(n=100)


# Q: do I have to put the df back into a table??
# A: think so

#print("4 show visits ==================================")

sql='''
    SELECT *
    FROM visits
'''
print(sql)
df = spark.sql(sql)
print(df.count())
df.show(n=100,truncate=False);


spark.stop()
