"""
spark_processing.py
--------------------
Core Spark logic: descriptive statistics + 4 MLlib jobs.
Used by BOTH:
  - app.py (the Streamlit web app, runs Spark in local mode inside the app)
  - colab_scalability_experiment.py (the timing experiment for 1/2/4/8 "workers")

Keeping this logic in one shared file means the exact same code path is used
for the live demo and for the performance experiment, which is what the
report needs to be scientifically honest.
"""

import json
import time

from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, TimestampType, DateType

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.fpm import FPGrowth


def load_data(spark, path, fmt):
    if fmt == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    elif fmt == "json":
        return spark.read.option("inferSchema", True).json(path)
    else:  # txt
        return spark.read.text(path)


# ---------------------------------------------------------------------------
# DESCRIPTIVE STATISTICS (>= 4 required)
# ---------------------------------------------------------------------------
def compute_descriptive_stats(df):
    stats = {}
    stats["num_rows"] = df.count()
    stats["num_columns"] = len(df.columns)
    stats["column_dtypes"] = {c: t for c, t in df.dtypes}

    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]

    minmax_mean = {}
    if numeric_cols:
        agg_exprs = []
        for c in numeric_cols:
            agg_exprs += [F.min(c).alias(f"{c}_min"), F.max(c).alias(f"{c}_max"), F.mean(c).alias(f"{c}_mean")]
        row = df.agg(*agg_exprs).collect()[0].asDict()
        for c in numeric_cols:
            minmax_mean[c] = {"min": row[f"{c}_min"], "max": row[f"{c}_max"], "mean": row[f"{c}_mean"]}
    stats["numeric_min_max_mean"] = minmax_mean

    unique_counts = {c: df.select(c).distinct().count() for c in df.columns}
    stats["unique_value_counts"] = unique_counts

    total_rows = stats["num_rows"] or 1
    null_pct = {}
    for c in df.columns:
        null_count = df.filter(F.col(c).isNull()).count()
        null_pct[c] = round(100.0 * null_count / total_rows, 2)
    stats["null_percentage"] = null_pct

    return stats


# ---------------------------------------------------------------------------
# MACHINE LEARNING JOBS (>= 4 required)
# ---------------------------------------------------------------------------
def run_regression(df, numeric_cols):
    if len(numeric_cols) < 2:
        return {"skipped": "not enough numeric columns for regression"}
    label_col = numeric_cols[-1]
    feature_cols = numeric_cols[:-1]
    clean_df = df.select(*numeric_cols).na.drop()
    if clean_df.count() < 2:
        return {"skipped": "not enough rows after dropping nulls"}
    vec_df = VectorAssembler(inputCols=feature_cols, outputCol="features").transform(clean_df)
    model = LinearRegression(featuresCol="features", labelCol=label_col).fit(vec_df)
    s = model.summary
    return {
        "label_column": label_col,
        "feature_columns": feature_cols,
        "coefficients": list(model.coefficients),
        "intercept": model.intercept,
        "r2": s.r2,
        "rmse": s.rootMeanSquaredError,
    }


def run_kmeans(df, numeric_cols, k=3):
    if len(numeric_cols) < 2:
        return {"skipped": "not enough numeric columns for clustering"}
    clean_df = df.select(*numeric_cols).na.drop()
    if clean_df.count() < k:
        return {"skipped": "not enough rows for the requested number of clusters"}
    vec_df = VectorAssembler(inputCols=numeric_cols, outputCol="features").transform(clean_df)
    model = KMeans(featuresCol="features", k=k, seed=42).fit(vec_df)
    predictions = model.transform(vec_df)
    sizes = predictions.groupBy("prediction").count().orderBy("prediction").collect()
    return {
        "k": k,
        "cluster_centers": [list(c) for c in model.clusterCenters()],
        "cluster_sizes": {int(r["prediction"]): r["count"] for r in sizes},
    }


def run_fpgrowth(df, categorical_cols, min_support=0.05, min_confidence=0.3):
    if not categorical_cols:
        return {"skipped": "no categorical columns available for FPGrowth"}
    items_df = df.select(
        F.array(*[F.col(c).cast("string") for c in categorical_cols]).alias("items")
    ).na.drop()
    model = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence).fit(items_df)
    freq = [
        {"items": r["items"], "freq": r["freq"]}
        for r in model.freqItemsets.orderBy(F.desc("freq")).limit(20).collect()
    ]
    return {"columns_used": categorical_cols, "top_frequent_itemsets": freq}


def run_timeseries_aggregation(df, date_cols, numeric_cols):
    if not date_cols or not numeric_cols:
        return {"skipped": "no date column or numeric column available"}
    date_col, value_col = date_cols[0], numeric_cols[0]
    ts_df = df.select(date_col, value_col).na.drop().withColumn(date_col, F.to_date(F.col(date_col)))
    daily = (
        ts_df.groupBy(F.col(date_col).alias("period"))
        .agg(F.sum(value_col).alias("total"), F.avg(value_col).alias("avg"))
        .orderBy("period").limit(50).collect()
    )
    monthly = (
        ts_df.groupBy(F.date_trunc("month", F.col(date_col)).alias("period"))
        .agg(F.sum(value_col).alias("total"), F.avg(value_col).alias("avg"))
        .orderBy("period").collect()
    )
    return {
        "date_column": date_col,
        "value_column": value_col,
        "daily_summary": [{"period": str(r["period"]), "total": r["total"], "avg": r["avg"]} for r in daily],
        "monthly_summary": [{"period": str(r["period"]), "total": r["total"], "avg": r["avg"]} for r in monthly],
    }


# ---------------------------------------------------------------------------
# ORCHESTRATOR — used by both the app and the experiment script
# ---------------------------------------------------------------------------
def process_dataset(spark, input_path, file_format):
    """Runs the full pipeline and returns a results dict, including timing."""
    start_time = time.time()

    df = load_data(spark, input_path, file_format)

    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    date_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (TimestampType, DateType))]
    categorical_cols = [c for c in df.columns if c not in numeric_cols and c not in date_cols]

    descriptive_stats = compute_descriptive_stats(df)

    ml_results = {
        "regression": run_regression(df, numeric_cols),
        "kmeans": run_kmeans(df, numeric_cols),
        "fpgrowth": run_fpgrowth(df, categorical_cols),
        "timeseries_aggregation": run_timeseries_aggregation(df, date_cols, numeric_cols),
    }

    execution_time_seconds = round(time.time() - start_time, 3)

    return {
        "input_path": input_path,
        "descriptive_stats": descriptive_stats,
        "ml_results": ml_results,
        "execution_time_seconds": execution_time_seconds,
    }
