from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, count
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from time import perf_counter
from pyspark.sql.functions import round as spark_round # kapoio thema dimiourghtai me auto kai to ebala etsi


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

    
    # dataFrame = spark.read.parquet(args.crimes_path)

    dataFrame = spark.read.csv(
                            args.crimes_path, 
                            header=True, 
                            inferSchema=True
                        )
    street_crimes = dataFrame.filter(col("Premis Desc") == "STREET").rdd

    total_street_crimes = street_crimes.count()

    def get_day_part(row):
        time_occ = int(row["TIME OCC"])

        if 500 <= time_occ <= 1159:
            return ("Prwi", 1)
        elif 1200 <= time_occ <= 1659:
            return ("Apogeuma", 1) 
        elif 1700 <= time_occ <= 2059:
            return ("Vradi", 1)
        else:
            return ("Nixta", 1)


    # start the counter here
    start_time = perf_counter()

    counts = street_crimes.map(get_day_part).reduceByKey(lambda a, b: a + b)
    
    rdd_results = counts.map(lambda x: (x[0], x[1], round((x[1] / total_street_crimes) * 100, 2))).sortBy(lambda x: x[2], ascending=False).collect()
    
    # end the counter here
    end_time = perf_counter()
    print(f"Time elapsed: {end_time - start_time:.4f} seconds")

    print(f"{'Day_Part'}, {'Crime_Count'}, {'Percentage (%)'}")
    for row in rdd_results:
        print(f"{row[0]}, {row[1]}, {row[2]}")


    spark.stop()

if __name__ == "__main__":
    main()