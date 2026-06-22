from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, year, month, count, rank, to_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from time import perf_counter

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
    parser.add_argument("--crimes-path", help="Explicit employees CSV path.")
    parser.add_argument("--output", help="Explicit output path.")
    parser.add_argument("--master", help="Optional Spark master.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    employees_path = args.crimes_path or (
        build_path(args.base_path, "examples/employees.csv")
        if args.base_path
        else "examples/employees.csv"
    )

    builder = SparkSession.builder.appName("DF query 1 execution")
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



    dataFrame = spark.read.parquet(args.crimes_path)
    # dataFrame.select("DATE OCC").show(2, truncate=False)

    time_start = perf_counter()

    df_date = dataFrame.withColumn("date", to_timestamp(col("DATE OCC"), "yyyy MMM dd hh:mm:ss a" ))

    df_year_month = df_date.withColumn("year", year("date")).withColumn("month", month("date"))

    month_counts = df_year_month.groupBy("year", "month").agg(count("*").alias("crime_total"))
    
    win = Window.partitionBy("year").orderBy(col("crime_total").desc())

    ranked_months = month_counts.withColumn("ranking", rank().over(win))
    
    top_three = ranked_months.filter(col("ranking") <= 3)


    # final_result = top_three.select("year", "month", "crime_total", "ranking").orderBy(col("year").asc(), col("crime_total").desc(), col("ranking").asc())
    final_result = top_three.select("year", "month", "crime_total", "ranking").orderBy(col("year").asc(), col("crime_total").desc(), col("ranking").asc())
    final_result.show(truncate=False)
    
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
