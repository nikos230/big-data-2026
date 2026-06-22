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


# Keep the Python executable the same on the driver and on Spark workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Use an explicit schema so beginners can see the intended column types
# and Spark does not have to guess them from the data.
EMPLOYEES_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("salary", IntegerType()),
        StructField("dep_id", IntegerType()),
    ]
)


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



    dataFrame_income = spark.read.parquet(args.income_path)

    dataFrame_income = dataFrame_income \
        .withColumnRenamed("Zip Code", "ZipCode") \
        .filter(F.col("Estimated Median Income").rlike("^[\\$0-9,\\.]+$")) \
        .withColumn("Income_Num", F.regexp_replace(F.col("Estimated Median Income"), "[\\$,]", "").cast("float")) \
        .select("ZipCode", "Income_Num")

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

    dataFrame_blocks = (
        spark.read
        .option("multiLine", "true")
        .schema(geojson_schema)  
        .json(args.census_path)
    ).selectExpr("explode(features) as features")
    
    dataFrame_blocks_final = dataFrame_blocks.select(
        F.col("features.properties.ZCTA20").alias("ZipCode"),
        F.col("features.properties.POP20").alias("Population")
    ).dropna(subset=["ZipCode", "Population"])

    # dataFrame_joined = census_final.join(dataFrame_income.hint("BROADCAST"), "ZipCode", "inner")
    
    time_start = perf_counter()

    # dataFrame_flattened_blocks = dataFrame_blocks_final.select(
    #     [
    #         F.col(f"properties.{col_name}").alias(col_name)
    #         for col_name in dataFrame_blocks_final.schema["properties"].dataType.fieldNames()
    #     ]
    # ).drop("properties").drop("type")

    # census_final = dataFrame_blocks_final.withColumnRenamed("ZCTA20", "ZipCode").withColumnRenamed("POP20", "Population")
    
    # dataFrame_joined = dataFrame_blocks_final.join(dataFrame_income.hint("shuffle_replicate_nl"), "ZipCode", "inner")
    dataFrame_joined = dataFrame_blocks_final.join(dataFrame_income, "ZipCode", "inner")


    dataFrame_result = dataFrame_joined.groupBy("ZipCode").agg(
        F.sum("Population").alias("Total_Population"),
        F.round((F.sum("Income_Num") / F.sum("Population")), 2).alias("Per_Capita_Income") 
    ).orderBy(F.col("Per_Capita_Income").desc())

    print("--- Catalyst Optimizer Execution Plan ---")
    dataFrame_result.explain(True)

    dataFrame_result.show(truncate=False)



    time_stop = perf_counter()    
    print(f"Time elapsed: {time_stop - time_start:.4f} seconds")
    

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
