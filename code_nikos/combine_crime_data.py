from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("combine_csv_crime").getOrCreate()

# csv_files = [
#     "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00283/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv",
#     "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00283/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
# ]

csv_files = [
    "/home/nikos/bigdata-dsml/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv",
    "/home/nikos/bigdata-dsml/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
]

parquet_output = "/home/nikos/bigdata-dsml/data_parquet/LA_Crime_Data_combined.parquet"
csv_output = "/home/nikos/bigdata-dsml/data/LA_Crime_Data_combined.csv"

df_combined = spark.read.csv(csv_files, header=True, inferSchema=True)

df_combined.write.mode("overwrite").parquet(parquet_output)
print(f"Combined Parquet saved: {parquet_output}")

df_combined.repartition(1).write.csv(csv_output, header=True, mode="overwrite")
print(f"Combined CSV saved to: {csv_output}")
