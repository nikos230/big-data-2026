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
    parser.add_argument("--stations-path", help="Explicit employees CSV path.")
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
        output_path = build_path(args.base_path, f"DFQ4_{spark.sparkContext.applicationId}")



    df_crimes = spark.read.parquet(args.crimes_path)
    df_stations = spark.read.parquet(args.stations_path)

    df_crimes_filtered = df_crimes.select(
            F.col("DR_NO"),
            F.col("LAT").alias("crime_lat"),
            F.col("LON").alias("crime_lon")
        ).filter(
            (F.col("crime_lat").isNotNull()) & (F.col("crime_lon").isNotNull()) &
            (F.col("crime_lat") != 0.0) & (F.col("crime_lon") != 0.0)
        )

    df_stations_filtered= df_stations.select(
            F.col("DIVISION").alias("division"),
            F.col("Y").alias("station_y"),
            F.col("X").alias("station_x")
        )
    
    time_start = perf_counter()

    # df_all = df_crimes_filtered.crossJoin(F.broadcast(df_stations_filtered))
    df_all = df_crimes_filtered.crossJoin(df_stations_filtered)
    # df_all = df_crimes_filtered.crossJoin(df_stations_filtered.hint("shuffle_replicate_nl"))
    df_all.explain(mode="formatted")
    R = 6371000
    
    lat1 = F.radians(F.col("crime_lat"))
    lon1 = F.radians(F.col("crime_lon"))
    lat2 = F.radians(F.col("station_y"))
    lon2 = F.radians(F.col("station_x"))

    mean_lat = F.radians(F.lit(34.05))

    crime_x = R * lon1 * F.cos(mean_lat)
    crime_y = R * lat1
    
    station_x = R * lon2 * F.cos(mean_lat)
    station_y = R * lat2

    diff_x = station_x - crime_x
    diff_y = station_y - crime_y
    
    dist = F.sqrt(F.pow(diff_x, 2) + F.pow(diff_y, 2))

    df_dist = df_all.withColumn("distance", dist)


    window = Window.partitionBy("DR_NO").orderBy("distance")
    df_nearest = df_dist.withColumn("rank", F.row_number().over(window)).filter(F.col("rank") == 1)

    df_result = df_nearest.groupBy("division").agg(
        F.round(F.avg("distance"), 3).alias("average_distance"),
        F.count("DR_NO").alias("#")
    ).orderBy(F.col("#").desc())

    df_result.explain(mode="formatted")

    df_result.show(truncate=False)

    time_stop = perf_counter()

    print(f"\nTime elapsed: {time_stop - time_start:.4f} seconds")

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
