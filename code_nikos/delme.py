from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, count, round
from pyspark.sql.types import StringType

# Αρχικοποίηση Spark Session
spark = SparkSession.builder.appName("NTUA_Query_1").getOrCreate()

# Path για τα δεδομένα (άλλαξε το αν χρειάζεται στο cluster σου)
data_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00283/data_parquet/LA_Crime_Data_combined.parquet"

# Φόρτωση των δεδομένων
df = spark.read.parquet(data_path)

# Φιλτράρισμα μόνο για τα εγκλήματα που έγιναν στο δρόμο ("STREET")
# Σημείωση: Ελέγξτε ακριβώς πώς γράφεται το "STREET" στο dataset σας (π.χ. μπορεί να έχει κεφαλαία ή μικρά)
street_crimes = df.filter(col("Premis Desc") == "STREET")

# Υπολογισμός του συνολικού αριθμού εγκλημάτων στο δρόμο (για την εύρεση του ποσοστού)
total_street_crimes = street_crimes.count()

print(f"Total crimes on STREET: {total_street_crimes}")
print("-" * 50)

# ==============================================================================
# ΥΛΟΠΟΙΗΣΗ 1: DataFrame API (ΧΩΡΙΣ UDF)
# ==============================================================================
print("1. DataFrame API (Without UDF)")

# Χρήση της when().otherwise() για την κατηγοριοποίηση της ώρας
df_no_udf = street_crimes.withColumn(
    "Day_Part",
    when((col("TIME OCC") >= 500) & (col("TIME OCC") <= 1159), "Πρωί")
    .when((col("TIME OCC") >= 1200) & (col("TIME OCC") <= 1659), "Απόγευμα")
    .when((col("TIME OCC") >= 1700) & (col("TIME OCC") <= 2059), "Βράδυ")
    .otherwise("Νύχτα") # Το υπόλοιπο είναι από 2100-2359 και 0-459
)

# Ομαδοποίηση, καταμέτρηση και υπολογισμός ποσοστού
result_no_udf = df_no_udf.groupBy("Day_Part").agg(count("*").alias("Crime_Count")) \
    .withColumn("Percentage", round((col("Crime_Count") / total_street_crimes) * 100, 2)) \
    .orderBy(col("Percentage").desc())

result_no_udf.show()


# ==============================================================================
# ΥΛΟΠΟΙΗΣΗ 2: DataFrame API (ΜΕ UDF)
# ==============================================================================
print("2. DataFrame API (With UDF)")

# Ορισμός της συνάρτησης Python
def get_day_part(time_occ):
    try:
        t = int(time_occ)
        if 500 <= t <= 1159:
            return "Πρωί"
        elif 1200 <= t <= 1659:
            return "Απόγευμα"
        elif 1700 <= t <= 2059:
            return "Βράδυ"
        else:
            return "Νύχτα"
    except:
        return "Άγνωστο"

# Εγγραφή της Python function ως Spark UDF
day_part_udf = udf(get_day_part, StringType())

# Εφαρμογή της UDF
df_with_udf = street_crimes.withColumn("Day_Part", day_part_udf(col("TIME OCC")))

# Ομαδοποίηση, καταμέτρηση και υπολογισμός ποσοστού (ίδιο με πριν)
result_with_udf = df_with_udf.groupBy("Day_Part").agg(count("*").alias("Crime_Count")) \
    .withColumn("Percentage", round((col("Crime_Count") / total_street_crimes) * 100, 2)) \
    .orderBy(col("Percentage").desc())

result_with_udf.show()


# ==============================================================================
# ΥΛΟΠΟΙΗΣΗ 3: RDD API
# ==============================================================================
print("3. RDD API")

# Μετατροπή του φιλτραρισμένου DataFrame σε RDD (κρατάμε μόνο τη στήλη TIME OCC)
rdd = street_crimes.select("TIME OCC").rdd

# Συνάρτηση Mapper για το RDD
def rdd_mapper(row):
    try:
        t = int(row["TIME OCC"])
        if 500 <= t <= 1159:
            return ("Πρωί", 1)
        elif 1200 <= t <= 1659:
            return ("Απόγευμα", 1)
        elif 1700 <= t <= 2059:
            return ("Βράδυ", 1)
        else:
            return ("Νύχτα", 1)
    except:
        return ("Άγνωστο", 1)

# Εκτέλεση MapReduce πάνω στο RDD
rdd_counts = rdd.map(rdd_mapper) \
                .reduceByKey(lambda a, b: a + b)

# Υπολογισμός ποσοστού, ταξινόμηση και συλλογή αποτελεσμάτων
rdd_results = rdd_counts.map(lambda x: (x[0], x[1], round((x[1] / total_street_crimes) * 100, 2))) \
                        .sortBy(lambda x: x[2], ascending=False) \
                        .collect()

# Τύπωμα αποτελεσμάτων
print(f"{'Day_Part':<15} | {'Crime_Count':<15} | {'Percentage (%)'}")
print("-" * 50)
for row in rdd_results:
    print(f"{row[0]:<15} | {row[1]:<15} | {row[2]}")

# Κλείσιμο session (χρήσιμο για να ελευθερωθούν οι πόροι στο cluster)
spark.stop()



    df_parsed = dataFrame.withColumn(
            "parsed_date", 
            to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a")
        )
    
    df_ym = df_parsed.withColumn("year", year("parsed_date")) \
                     .withColumn("month", month("parsed_date"))

    # 2. Group by Year and Month, then count the total crimes
    monthly_counts = df_ym.groupBy("year", "month").agg(count("*").alias("crime_total"))

    # 3. Create a Window spec to partition by year and order by total crimes descending
    window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())

    # 4. Apply rank to find the top months per year and filter for the top 3
    ranked_months = monthly_counts.withColumn("ranking", rank().over(window_spec))
    top_3_months = ranked_months.filter(col("ranking") <= 3)

    # 5. Order the final results: Year ASC, Crime Total DESC, Ranking ASC
    final_result = top_3_months.select("year", "month", "crime_total", "ranking") \
                               .orderBy(col("year").asc(), col("crime_total").desc(), col("ranking").asc())

    # Show the results in the console (matches Table 2 from the requirements)
    final_result.show(truncate=False)

    # --- Optional: Save Output ---
    # if output_path:
    #     if "://" in output_path:
    #         # A single output file is convenient for lab inspection, but not for large jobs.
    #         final_result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    #     else:
    #         # Collect to driver and save locally using the helper function
    #         results_list = [tuple(row) for row in final_result.collect()]
    #         write_local_csv_output(output_path, results_list)
    #     print(f"Saved to: {output_path}")



    time_start = perf_counter()

    # 2. Register DataFrame as a temporary SQL view so we can query it
    dataFrame.createOrReplaceTempView("crimes")

    # 3. Write the SQL Query using Common Table Expressions (WITH clauses)
    # Notice how we use the exact same timestamp format string we discovered earlier.
    sql_query = """
        WITH ParsedDates AS (
            SELECT 
                year(to_timestamp(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS year,
                month(to_timestamp(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS month
            FROM crimes
        ),
        MonthlyCounts AS (
            SELECT 
                year, 
                month, 
                COUNT(*) AS crime_total
            FROM ParsedDates
            GROUP BY year, month
        ),
        RankedMonths AS (
            SELECT 
                year, 
                month, 
                crime_total,
                RANK() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
            FROM MonthlyCounts
        )
        SELECT 
            year, 
            month, 
            crime_total, 
            ranking
        FROM RankedMonths
        WHERE ranking <= 3
        ORDER BY year ASC, crime_total DESC, ranking ASC
    """

    # 4. Execute the query (Lazy transformation)
    final_result = spark.sql(sql_query)

    # 5. Trigger the Action
    final_result.show(truncate=False)

    # Stop the timer AFTER the action finishes
    time_stop = perf_counter()
    print(f"Time elapsed: {time_stop - time_start:.4f} seconds")



    income_final = income_raw.withColumn(
        "Income_Num", 
        F.regexp_replace(F.col("Estimated Median Income"), "[\\$,]", "").cast("float")
    ).withColumnRenamed("Zip Code", "ZipCode").select("ZipCode", "Income_Num")


    # ==========================================
    # 2. ΕΠΕΞΕΡΓΑΣΙΑ CENSUS (GEOJSON) - Χρήση του δικού σου snippet
    # ==========================================
    geojson_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Census_Blocks_2020.geojson"
    
    blocks_df = (
        spark.read
        .option("multiLine", "true")
        .json(geojson_path)
    ).selectExpr("explode(features) as features") \
     .select("features.*")

    # Flattening των properties δυναμικά!
    flattened_blocks_df = blocks_df.select(
        [
            F.col(f"properties.{col_name}").alias(col_name)
            for col_name in blocks_df.schema["properties"].dataType.fieldNames()
        ]
        # Το geometry δεν το χρειαζόμαστε απαραίτητα για το Q3, αλλά το κρατάμε όπως το έγραψες
        + ["geometry"] 
    ).drop("properties").drop("type")

    # ΠΡΟΣΟΧΗ: Επειδή το flattened_blocks_df έχει πλέον ως στήλες τα πραγματικά ονόματα 
    # από το GeoJSON, πρέπει να βρεις πώς ακριβώς ονομάζεται η στήλη του Zip και του Πληθυσμού.
    # Έστω ότι ονομάζονται "zip" και "pop2020" (αντικατέστησέ τα με τα σωστά από το _fields.csv).
    census_final = flattened_blocks_df.withColumnRenamed("zip", "ZipCode") \
                                      .withColumnRenamed("pop2020", "Population")


    # ==========================================
    # 3. JOIN ΚΑΙ ΥΠΟΛΟΓΙΣΜΟΙ
    # ==========================================
    # Εκτέλεση του Join με βάση το ZipCode
    # Hint: Εδώ μπορείς να βάλεις το .hint("BROADCAST") για το Ζητούμενο 6
    joined_df = census_final.join(income_final, "ZipCode", "inner")

    # Υπολογισμός: (Συνολικό Εισόδημα) / (Συνολικός Πληθυσμός) ανά ZipCode
    result_df = joined_df.groupBy("ZipCode").agg(
        F.sum("Population").alias("Total_Population"),
        F.round((F.sum("Income_Num") / F.sum("Population")), 2).alias("Per_Capita_Income") 
    ).orderBy(F.col("Per_Capita_Income").desc())

    # ΖΗΤΟΥΜΕΝΟ 6: Ανάλυση του Execution Plan (Catalyst Optimizer)
    print("--- Catalyst Optimizer Execution Plan ---")
    result_df.explain(True)

    # Εμφάνιση αποτελεσμάτων
    print("--- Final Results ---")
    result_df.show(truncate=False)



    def clean_income(row):
        zip_code = row["Zip Code"]
        income_str = row["Estimated Median Income"]
        
        # Απορρίπτουμε κενά ή βρώμικα (γράμματα)
        if not zip_code or not income_str: return None
        if not re.match(r'^[\$0-9,\.]+$', income_str): return None
        
        # Καθαρίζουμε το $ και το ,
        clean_str = re.sub(r'[\$,]', '', income_str)
        try:
            return (str(zip_code), float(clean_str))
        except ValueError:
            return None

    # Το τελικό RDD του Income: [(ZipCode, Income), (ZipCode, Income)...]
    income_rdd = income_rdd_raw.map(clean_income).filter(lambda x: x is not None)


    # ==========================================
    # 2. READ CENSUS & CONVERT TO RDD (Schema Pruning OOM Fix)
    # ==========================================
    properties_schema = StructType([
        StructField("ZCTA20", StringType(), True),
        StructField("POP20", LongType(), True)
    ])
    feature_schema = StructType([StructField("properties", properties_schema, True)])
    geojson_schema = StructType([StructField("features", ArrayType(feature_schema), True)])

    census_df = (
        spark.read
        .option("multiLine", "true")
        .schema(geojson_schema)
        .json(args.census_path)
    ).selectExpr("explode(features) as features")
    
    census_rdd_raw = census_df.rdd

    # Map-Filter: Εξαγωγή (Key, Value) -> (ZipCode, Population)
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

    # Το τελικό RDD του Census: [(ZipCode, Population), (ZipCode, Population)...]
    census_rdd = census_rdd_raw.map(extract_census).filter(lambda x: x is not None)

    time_start = perf_counter()
    print("Starting RDD Execution...")

    # ==========================================
    # 3. RDD BROADCAST JOIN (Map-Side Join)
    # ==========================================
    # Αντί για το ακριβό census_rdd.join(income_rdd), κάνουμε χειροκίνητο Broadcast 
    # (Αυτό που έκανε το hint("BROADCAST") στα DataFrames)
    
    # Μαζεύουμε το μικρό RDD του Income στον Driver ως Dictionary
    income_dict = income_rdd.collectAsMap()
    
    # Το κάνουμε Broadcast σε όλους τους Executors
    broadcast_income = sc.broadcast(income_dict)

    # Κάνουμε Join χρησιμοποιώντας απλό map πάνω στο μεγάλο Census RDD!
    def map_side_join(row):
        zip_code = row[0]
        pop = row[1]
        income_map = broadcast_income.value
        
        # Αν το ZipCode υπάρχει στο Income, τα ενώνουμε
        if zip_code in income_map:
            return (zip_code, (pop, income_map[zip_code]))
        return None

    # Μορφή μετά το join: (ZipCode, (Population, Income_Num))
    joined_rdd = census_rdd.map(map_side_join).filter(lambda x: x is not None)


    # ==========================================
    # 4. RDD AGGREGATION & CALCULATION
    # ==========================================
    # reduceByKey: Αθροίζουμε τον Πληθυσμό και το Εισόδημα για κάθε ZipCode
    # a και b έχουν τη μορφή (Population, Income_Num)
    reduced_rdd = joined_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Υπολογισμός Per Capita Income
    def calculate_per_capita(row):
        zip_code = row[0]
        total_pop = row[1][0]
        total_income = row[1][1]
        
        # Προστασία από διαίρεση με το μηδέν
        per_capita = round(total_income / total_pop, 2) if total_pop > 0 else 0
        
        return (zip_code, total_pop, per_capita)

    # Μορφή: (ZipCode, Total_Population, Per_Capita_Income)
    final_rdd = reduced_rdd.map(calculate_per_capita)

    # Ταξινόμηση βάσει του Per_Capita_Income (το 3ο στοιχείο, άρα x[2]) σε φθίνουσα σειρά
    sorted_rdd = final_rdd.sortBy(lambda x: x[2], ascending=False)

    # 5. TRIGGER ACTION (Collect and Print)
    results = sorted_rdd.collect()

    time_stop = perf_counter()

    print("\n--- Final Results (RDD) ---")
    print(f"{'ZipCode':<10} | {'Total_Population':<20} | {'Per_Capita_Income'}")
    print("-" * 55)
    for row in results:
        print(f"{row[0]:<10} | {row[1]:<20} | {row[2]}")

    print(f"\nTime elapsed: {time_stop - time_start:.4f} seconds")