from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import os
import sys

from pyspark.sql.types import IntegerType

os.environ['PYSPARK_PYTHON'] = sys.executable

if __name__ == "__main__":

    spark = SparkSession.builder\
            .appName("read_csv")\
            .master("local[3]")\
            .getOrCreate()

    df_csv = spark.read.format('csv')\
                .option('inferSchema',True)\
                .option("header",True)\
                .load("sample.csv")

    df_csv.show()


    df_groupby = df_csv.select("Age","Country","Gender")\
                    .groupBy("Country","Gender")\
                    .agg(avg("Age").alias("Average_age_2"))

    df_groupby.show()

    df_corrected = df_csv.withColumn("Gender", when(col("Gender") == "M","Male").otherwise(col("Gender")))\
                    .withColumn("Gender", when(col("Gender") == "F","Female").otherwise(col("Gender")))\
                    .withColumn("seek_help", when(col("seek_help") == "Yes",1).otherwise(col("seek_help"))) \
                    .withColumn("seek_help", when(col("seek_help") == "No", 0).otherwise(col("seek_help"))) \
                    .withColumn("seek_help", when(col("seek_help") == "Don't know", -1).otherwise(col("seek_help")))\
                    .withColumn("seek_help",col("seek_help").cast("int"))

    df_corrected.show()

    df_corrected_grouby = df_corrected.select("Age","Country","Gender")\
                    .groupBy("Country","Gender")\
                    .agg(avg("Age").alias("Average_age"))

    df_corrected_grouby.show()

    print(df_corrected.schema)


