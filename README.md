# Bigdata Semester Project 2026

## Transfer of scripts or data into server
```
hdfs dfs -put -f /home/nikos/bigdata-dsml/code_nikos/Q1_DF_without_UDF.py /user
/dsml00283/code_nikos/Q1_DF_without_UDF.py
```

## Zitima 2
Execution of Query 1 without UDF (parquet)
```
spark-submit --conf spark.executor.instances=2 --conf spark.executor.cores=1 --conf spark.executor.memory=2g hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code_nikos/Q1_DF_without_UDF.py --crimes-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/data_parquet/LA_Crime_Data_combined.parquet
```

Execution of Query 1 with UDF (parquet)
```
spark-submit --conf spark.executor.instances=2 --conf spark.executor.cores=1 --conf spark.executor.memory=2g hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code_nikos/Q1_DF_with_UDF.py --crimes-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/data_parquet/LA_Crime_Data_combined.parquet
```

Execution of Query 1 with RDD (parquet)
```
spark-submit --conf spark.executor.instances=2 --conf spark.executor.cores=1 --conf spark.executor.memory=2g hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code_nikos/Q1_RDD.py --crimes-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/data_parquet/LA_Crime_Data_combined.parquet
```

Execution of Query 1 RDD (csv)
```
spark-submit --conf spark.executor.instances=2 --conf spark.executor.cores=1 --conf spark.executor.memory=2g hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code_nikos/Q1_RDD_csv.py --crimes-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/data_csv/LA_Crime_Data_combined.csv 
```

Execution of Query 1 without UDF (csv)
```
spark-submit --conf spark.executor.instances=2 --conf spark.executor.cores=1 --conf spark.executor.memory=2g hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code_nikos/Q1_DF_without_UDF_csv.py --crimes-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/data_csv/LA_Crime_Data_combined.csv
```

Execution of Query 1 without UDF (csv)
```
spark-submit --conf spark.executor.instances=2 --conf spark.executor.cores=1 --conf spark.executor.memory=2g hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code_nikos/Q1_DF_with_UDF_csv.py --crimes-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/data_csv/LA_Crime_Data_combined.csv 
```

Execution of Query 2 SQL
```
spark-submit --conf spark.executor.instances=4 --conf spark.executor.cores=1 --conf spark.executor.memory=2g hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code_nikos/Q2_SQL.py --crimes-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/data_parquet/LA_Crime_Data_combined.parquet
```

Execution of Query 2 DataFrame
```
spark-submit --conf spark.executor.instances=4 --conf spark.executor.cores=1 --conf spark.executor.memory=2g hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code_nikos/Q2_DF.py --crimes-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/data_parquet/LA_Crime_Data_combined.parquet
```

Execution of Query 3 DataFrame
```
spark-submit --conf spark.executor.instances=3 --conf spark.executor.cores=1 --conf spark.executor.memory=2g hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code_nikos/Q3_DF.py --income-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/data_parquet/LA_income_2021.parquet --census-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/data/LA_Census_Blocks_2020.geojson
```
