#!/usr/bin/env python3
from pyspark.sql import SparkSession


# this one can handle adjacent dates

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


sql='''
    WITH rolling_max_end_date as (
        SELECT id, start_date, end_date,
           max(end_date) over (partition by  id order by start_date) as max_end_date,
           rank() over (partition by  id order by start_date) as idx
        FROM visits),
    visit_gaps as (
        SELECT *,
            CASE 
                WHEN start_date <= date_add(LAG(max_end_date)
                                            OVER (PARTITION BY id ORDER by start_date) ,1)
                  THEN 0
                ELSE 1
            END AS gap
        FROM rolling_max_end_date)
    SELECT *,
        SUM(gap) OVER (PARTITION BY id ORDER BY start_date) as group_number
    FROM visit_gaps   
'''
print(sql)
df = spark.sql(sql)
print(df.count())
df.show(n=100,truncate=False);

spark.stop()
