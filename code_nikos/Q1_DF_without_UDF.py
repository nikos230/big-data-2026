from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, count, round
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from time import perf_counter


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"

def write_local_csv_output(output_path: str, rows: list[tuple[int, str, int, int]]) -> None:
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, "part-00000")
    with open(output_file, "w", encoding="utf-8") as file_handle:
        for row in rows:
            file_handle.write(",".join(str(value) for value in row) + "\n")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query 1 using the DataFrame API.",
    )
    parser.add_argument("--base-path", help="Base path that contains examples/ and the default output location.")
    parser.add_argument("--crimes-path", help="Explicit Crimes CSV path.")
    parser.add_argument("--output", help="Explicit output path.")
    parser.add_argument("--master", help="Optional Spark master.")
    return parser.parse_args()



def main():
    args = parse_args()
    crimes_path = args.crimes_path or (
        build_path(args.base_path, "examples/employees.csv")
        if args.base_path
        else "examples/employees.csv"
    )

    builder = SparkSession.builder.appName("DF query 1 execution")


    if args.master:
        builder = builder.master(args.master)
        if args.master.startswith("local"):
            builder = builder.config("spark.submit.deployMode", "client")
    elif "://" not in crimes_path:
        builder = builder.master("local[*]").config("spark.submit.deployMode", "client")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"DFQ1_{spark.sparkContext.applicationId}")

    
    dataFrame = spark.read.parquet(args.crimes_path)

    # dataFrame = spark.read.csv(
    #                         args.crimes_path, 
    #                         header=True, 
    #                         inferSchema=True
    #                     )
    street_crimes = dataFrame.filter(col("Premis Desc") == "STREET") 

    total_street_crimes = street_crimes.count()

    # start the counter here
    start_time = perf_counter()

    df_no_udf = street_crimes.withColumn(
        'Day_Part',
        when((col("TIME OCC") >= 500) & (col("TIME OCC") <= 1159), "Prwi")
        .when((col("TIME OCC") >= 1200) & (col("TIME OCC") <= 1659), "Apogeuma")
        .when((col("TIME OCC") >= 1700) & (col("TIME OCC") <= 2059), "Vradi")
        .otherwise("Nixta") 
    )
    
    result_without_udf = (
    df_no_udf.groupBy("Day_Part")
    .count()  
    .withColumnRenamed("count", "Crime_Count")
    .withColumn("Percentage", round((col("Crime_Count") / total_street_crimes) * 100, 2))
    .orderBy(col("Percentage").desc())
    )

    result_without_udf.show()

    # end the counter here
    end_time = perf_counter()
    print(f"Time elapsed: {end_time - start_time:.4f} seconds")


    spark.stop()

if __name__ == "__main__":
    main()