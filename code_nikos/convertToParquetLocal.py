from pyspark.sql import SparkSession
import os

"""
LA_Census_Blocks_2020.geojson
LA_Census_Blocks_2020_fields.csv
LA_Crime_Data/LA_Crime_Data_2010_2019.csv
LA_Crime_Data/LA_Crime_Data_2020_2025.csv
LA_Police_Stations.csv
LA_income_2021.csv
"""

fileNames = ["LA_Census_Blocks_2020.geojson",
             "LA_Census_Blocks_2020_fields.csv",
             "LA_Crime_Data/LA_Crime_Data_2010_2019.csv",
             "LA_Crime_Data/LA_Crime_Data_2020_2025.csv",
             "LA_Police_Stations.csv",
             "LA_income_2021.csv"]

spark = SparkSession.builder.appName("csv_to_parquet").getOrCreate()


for file in fileNames:

    file_name = file.split('.')[0]
    extension = file.split('.')[1]

    df = spark.read.csv(f"/home/nikos/bigdata-dsml/data/{file_name}.{extension}", 
                        header=True, 
                        inferSchema=True)

    df.write.parquet(f"/home/nikos/bigdata-dsml/data_parquet/{file_name}.parquet")

    print(f"File: {file} done converting!")