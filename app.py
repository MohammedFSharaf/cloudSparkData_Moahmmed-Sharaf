"""
app.py
------
Streamlit web app for the Cloud-Based Distributed Data Processing Service.

Design choice (documented in the report): instead of submitting jobs to an
external managed cluster (Dataproc/EMR/HDInsight), this app runs PySpark in
*local mode* inside the same container that Streamlit Community Cloud
provisions for the app. Streamlit Community Cloud itself is a cloud
platform (the container runs on Streamlit's servers, not on the student's
laptop), which satisfies "the program must run in a cloud development
platform." The 1/2/4/8-"machine" scalability experiment is run separately
in Google Colab (see colab_scalability_experiment.py) because it needs
controlled, repeatable timing that a shared web container can't guarantee.

Run locally with:
    streamlit run app.py

Deploy for free at https://streamlit.io/cloud (no credit card required,
just a GitHub account — push this repo to GitHub then "New app").
"""

import json
import os
import tempfile
import time

import streamlit as st
from pyspark.sql import SparkSession

from spark_processing import process_dataset

st.set_page_config(page_title="Cloud Spark Data Processing", layout="wide")
st.title("☁️ Cloud-Based Distributed Data Processing Service")
st.caption("Upload a dataset, run Spark analytics, and view the results — all in the browser.")


@st.cache_resource
def get_spark_session():
    return (
        SparkSession.builder
        .appName("CloudDistributedDataProcessing")
        .master("local[*]")
        .getOrCreate()
    )


uploaded_file = st.file_uploader("Upload your dataset", type=["csv", "json", "txt"])
file_format = st.selectbox("File format", ["csv", "json", "txt"])

if uploaded_file is not None:
    st.success(f"File ready: {uploaded_file.name} ({uploaded_file.size} bytes)")

    if st.button("🚀 Run Spark Job"):
        # Save the upload to a temp file so Spark can read it from disk
        suffix = os.path.splitext(uploaded_file.name)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(uploaded_file.getbuffer())
            tmp_path = tmp.name

        with st.spinner("Starting Spark and processing your dataset..."):
            spark = get_spark_session()
            results = process_dataset(spark, tmp_path, file_format)

        st.success(f"Done in {results['execution_time_seconds']} seconds.")

        st.subheader("📊 Descriptive Statistics")
        st.json(results["descriptive_stats"])

        st.subheader("🤖 Machine Learning Results")
        for job_name, job_result in results["ml_results"].items():
            with st.expander(job_name.replace("_", " ").title()):
                st.json(job_result)

        st.subheader("⏱ Execution Time")
        st.metric("Spark job execution time (seconds)", results["execution_time_seconds"])

        st.download_button(
            "Download full results (JSON)",
            data=json.dumps(results, indent=2, default=str),
            file_name=f"{uploaded_file.name}_results.json",
            mime="application/json",
        )

        os.unlink(tmp_path)
else:
    st.info("Upload a CSV, JSON, or TXT file to get started.")

    st.markdown(
        """
        ---
        **About this service**: uploaded data is processed with Apache Spark
        (PySpark) directly in this cloud-hosted app. It computes descriptive
        statistics and runs 4 MLlib jobs (regression, KMeans clustering,
        FPGrowth frequent-itemset mining, and time-series aggregation),
        automatically choosing which ones apply based on your data's columns.
        """
    )
