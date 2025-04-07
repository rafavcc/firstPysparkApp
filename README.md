# firstPysparkApp

# firstPysparkApp

A simple PySpark application that demonstrates the power of distributed computing using Apache Spark.

## 🔍 What it does

This script calculates the **first 1 million digits of π (Pi)** using Python and Apache Spark, stores the result in a DataFrame, and saves it in **Parquet** format for further analysis.

It also shows:
- How to create a `SparkSession`
- How to parallelize data with `RDDs`
- How to write and read Parquet files
- How to monitor execution using the **Spark UI** (`http://localhost:4040`)

## 🧪 Try it out

To run:

```bash
spark-submit initial.py
```