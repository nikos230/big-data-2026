from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, year, month, count, rank, to_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from time import perf_counter
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType
import re

# Keep the Python executable the same on the driver and on Spark workers.
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
        description="Find the 5 employees with the lowest salary using the DataFrame API.",
    )
    parser.add_argument("--base-path", help="Base path that contains examples/ and the default output location.")
    parser.add_argument("--income-path", help="Explicit employees CSV path.")
    parser.add_argument("--census-path", help="Explicit employees CSV path.")
    parser.add_argument("--output", help="Explicit output path.")
    parser.add_argument("--master", help="Optional Spark master.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    employees_path = args.census_path or (
        build_path(args.base_path, "examples/employees.csv")
        if args.base_path
        else "examples/employees.csv"
    )

    builder = SparkSession.builder.appName("DF query 2 execution")
    # The same script supports both local practice and remote submission.
    if args.master:
        builder = builder.master(args.master)
        if args.master.startswith("local"):
            builder = builder.config("spark.submit.deployMode", "client")
    elif "://" not in employees_path:
        builder = builder.master("local[*]").config("spark.submit.deployMode", "client")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"DFQ2_{spark.sparkContext.applicationId}")



    income_rdd = spark.read.parquet(args.income_path).rdd

    properties_schema = StructType([
        StructField("ZCTA20", StringType(), True),
        StructField("POP20", LongType(), True)
    ])
    
    feature_schema = StructType([
        StructField("properties", properties_schema, True)
    ])
    
    geojson_schema = StructType([
        StructField("features", ArrayType(feature_schema), True)
    ])

    census_rdd = (
        spark.read
        .option("multiLine", "true")
        .schema(geojson_schema)  
        .json(args.census_path)
    ).selectExpr("explode(features) as features").rdd


    def income_cleaned(row):
        zip_code = row["Zip Code"]
        income_str = row["Estimated Median Income"]
        
        if not zip_code or not income_str: return None
        if not re.match(r'^[\$0-9,\.]+$', income_str): return None
        
        clean_str = re.sub(r'[\$,]', '', income_str)
        try:
            return (str(zip_code), float(clean_str))
        except ValueError:
            return None

    income_rdd_ = income_rdd.map(income_cleaned).filter(lambda x: x is not None)


    def extract_census(row):
        try:
            props = row["features"]["properties"]
            zip_code = props["ZCTA20"]
            pop = props["POP20"]
            if zip_code is not None and pop is not None:
                return (str(zip_code), int(pop))
        except (KeyError, TypeError):
            pass
        return None
    
    census_rdd = census_rdd.map(extract_census).filter(lambda x: x is not None)

    time_start = perf_counter()
    
    income_dict = income_rdd_.collectAsMap()

    broadcast_income = spark.sparkContext.broadcast(income_dict)


    def map_side_join(row):
        zip_code = row[0]
        pop = row[1]
        income_map = broadcast_income.value

        if zip_code in income_map:
            return (zip_code, (pop, income_map[zip_code]))
        return None
    
    joined_rdd = census_rdd.map(map_side_join).filter(lambda x: x is not None)

    reduced_rdd = joined_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))


    def calculate_per_capita(row):
        zip_code = row[0]
        total_pop = row[1][0]
        total_income = row[1][1]
        
        per_capita = round(total_income / total_pop, 2) if total_pop > 0 else 0
        
        return (zip_code, total_pop, per_capita)

    final_rdd = reduced_rdd.map(calculate_per_capita)

    sorted_rdd = final_rdd.sortBy(lambda x: x[2], ascending=False)

    results = sorted_rdd.collect()


    time_stop = perf_counter()
    print(f"\nTime elapsed: {time_stop - time_start:.4f} seconds")

    print("\n--- Final Results (RDD) ---")
    print(f"{'ZipCode':<10} | {'Total_Population':<20} | {'Per_Capita_Income'}")
    print("-" * 55)

    for row in results[:10]:
        print(f"{row[0]:<10} | {row[1]:<20} | {row[2]}")


    

    # if output_path:
    #     if "://" in output_path:
    #         # A single output file is convenient for lab inspection, but not for large jobs.
    #         lowest_salaries.coalesce(1).write.mode("overwrite").csv(output_path)
    #     else:
    #         write_local_csv_output(output_path, results)
    #     print(f"Saved to: {output_path}")


    spark.stop()


if __name__ == "__main__":
    main()
