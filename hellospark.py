from pyspark.sql import *
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[2]") \
        .getOrCreate()

    data_list = [("Ravi", 28),
                 ("David", 45),
                 ("Abdul", 27)]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df_changed = df.withColumnRenamed("Name","First_Name")



