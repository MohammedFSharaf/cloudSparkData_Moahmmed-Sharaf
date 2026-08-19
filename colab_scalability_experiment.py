

import time
import json

from pyspark.sql import SparkSession
from spark_processing import process_dataset

INPUT_PATH = "/content/your_large_dataset.csv"   
FILE_FORMAT = "csv"                              
WORKER_COUNTS = [1, 2, 4, 8]                    

results_table = []

for n in WORKER_COUNTS:
    print(f"\n=== Running with local[{n}] ===")

    spark = (
        SparkSession.builder
        .appName(f"ScalabilityTest_{n}")
        .master(f"local[{n}]")
        .getOrCreate()
    )

    start = time.time()
    result = process_dataset(spark, INPUT_PATH, FILE_FORMAT)
    elapsed = time.time() - start

    print(f"Execution time with {n} worker(s): {elapsed:.2f} sec")
    results_table.append({"workers": n, "execution_time_sec": round(elapsed, 3)})

    spark.stop()  

t1 = results_table[0]["execution_time_sec"]
for row in results_table:
    row["speedup"] = round(t1 / row["execution_time_sec"], 3) if row["execution_time_sec"] else None
    row["efficiency"] = round(row["speedup"] / row["workers"], 3) if row["speedup"] else None

print("\n=== FINAL RESULTS TABLE (copy into your report) ===")
print(f"{'Machines':<10}{'Time (s)':<12}{'Speedup':<10}{'Efficiency':<10}")
for row in results_table:
    print(f"{row['workers']:<10}{row['execution_time_sec']:<12}{row['speedup']:<10}{row['efficiency']:<10}")

with open("/content/scalability_results.json", "w") as f:
    json.dump(results_table, f, indent=2)

print("\nSaved to /content/scalability_results.json — download it from the Colab file browser.")

