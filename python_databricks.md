# Python with Databricks Cheatsheet for Azure Data Engineers

## 1. Getting Started

```python
# Databricks notebook cell
dbutils.fs.ls("/databricks-datasets")
dbutils.widgets.text("input", "defaultValue", "Input Widget")
```

## 2. Reading and Writing Data

### Read Data

```python
# CSV
df = spark.read.csv("/mnt/data/file.csv", header=True, inferSchema=True)

# Parquet
df = spark.read.parquet("/mnt/data/file.parquet")

# Delta
df = spark.read.format("delta").load("/mnt/delta/table")

# Azure Data Lake
df = spark.read.csv("abfss://container@account.dfs.core.windows.net/path/file.csv")
```

### Write Data

```python
# Parquet
df.write.parquet("/mnt/output/output.parquet")

# Delta
df.write.format("delta").mode("overwrite").save("/mnt/delta/table")

# Azure SQL
df.write \
  .format("jdbc") \
  .option("url", "jdbc:sqlserver://<server>.database.windows.net:1433;database=<db>") \
  .option("dbtable", "dbo.table") \
  .option("user", "<user>") \
  .option("password", "<password>") \
  .save()
```

## 3. DataFrame Operations

```python
df.show()
df.printSchema()
df.select("col1", "col2").filter(df.col1 > 10).groupBy("col2").count()
df.withColumn("new_col", df.col1 * 2)
df.dropDuplicates(["col1"])
df.orderBy(df.col1.desc())
```

## 4. UDFs and Pandas UDFs

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import IntegerType

# Regular UDF
@udf(IntegerType())
def double(x):
    return x * 2

df.withColumn("doubled", double(df.col1))

# Pandas UDF
@pandas_udf("int")
def plus_one(s: pd.Series) -> pd.Series:
    return s + 1

df.withColumn("plus_one", plus_one(df.col1))
```

## 5. Mounting Azure Data Lake

```python
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<client-id>",
  "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope>", key="<key>"),
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://<container>@<account>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)
```

## 6. Delta Lake Operations

```python
from delta.tables import DeltaTable

# Create Delta Table
df.write.format("delta").save("/mnt/delta/mytable")

# Read Delta Table
deltaTable = DeltaTable.forPath(spark, "/mnt/delta/mytable")

# Upsert (Merge)
deltaTable.alias("tgt").merge(
    source=df.alias("src"),
    condition="tgt.id = src.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

## 7. Job Scheduling

- Use Databricks Jobs UI or `databricks-cli` to schedule notebooks.
- Use `dbutils.notebook.run("notebook_path", timeout_seconds, {"param":"value"})` for chaining.

## 8. Secrets Management

```python
dbutils.secrets.get(scope="my-scope", key="my-key")
```

## 9. Logging and Monitoring

```python
import logging
logging.basicConfig(level=logging.INFO)
logging.info("This is an info message")
```

- Use Azure Monitor and Log Analytics for cluster/job monitoring.

## 10. Best Practices

- Use Delta Lake for ACID transactions and scalable data pipelines.
- Partition data for performance.
- Use secrets for credentials, never hard-code.
- Use cluster pools and autoscaling for cost optimization.
- Version control notebooks with Git integration.

## 11. Useful CLI Commands

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure CLI
databricks configure --token

# List clusters
databricks clusters list

# Run a job
databricks jobs run-now --job-id <job-id>
```

## 12. Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [Azure Databricks Best Practices](https://learn.microsoft.com/en-us/azure/databricks/best-practices/)
- [Delta Lake Documentation](https://docs.delta.io/latest/)

---

**Tip:** Always use the latest Databricks Runtime and keep your libraries up to date for security and performance.
