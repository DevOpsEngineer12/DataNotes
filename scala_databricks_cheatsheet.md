# 🚀 Scala + Databricks Cheatsheet for Data Engineers

## 1. Basics
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder()
  .appName("DatabricksScalaCheatSheet")
  .getOrCreate()

val df = spark.read.option("header", "true").csv("/mnt/data/file.csv")
df.show()
df.printSchema()
df.columns
```

## 2. Reading & Writing Data
```scala
val csvDF = spark.read.option("header","true").csv("/mnt/raw/data.csv")
csvDF.write.mode("overwrite").csv("/mnt/processed/csv_out")

val jsonDF = spark.read.json("/mnt/raw/data.json")
jsonDF.write.mode("overwrite").json("/mnt/processed/json_out")

val parquetDF = spark.read.parquet("/mnt/raw/data.parquet")
parquetDF.write.mode("overwrite").parquet("/mnt/processed/parquet_out")

val deltaDF = spark.read.format("delta").load("/mnt/delta/table")
deltaDF.write.format("delta").mode("overwrite").save("/mnt/delta/output")
```

## 3. DataFrame Operations
```scala
df.select("id","name").filter(col("age") > 25).show()
df.withColumn("age_plus_5", col("age") + 5)
df.withColumnRenamed("oldCol", "newCol")
df.drop("colName")
df.select("country").distinct()
df.orderBy(col("salary").desc)
```

## 4. Aggregations & Grouping
```scala
df.groupBy("department").agg(
  avg("salary").alias("avg_salary"),
  max("salary").alias("max_salary")
)

import org.apache.spark.sql.expressions.Window
val winSpec = Window.partitionBy("department").orderBy(col("salary").desc)

df.withColumn("rank", rank().over(winSpec))
  .withColumn("row_num", row_number().over(winSpec))
  .withColumn("dense_rank", dense_rank().over(winSpec))
```

## 5. Joins
```scala
df1.join(df2, Seq("id"), "inner")
df1.join(df2, Seq("id"), "left")
df1.join(df2, Seq("id"), "right")
df1.join(df2, Seq("id"), "outer")
```

## 6. SQL in Databricks
```scala
df.createOrReplaceTempView("employees")
val sqlDF = spark.sql("""
  SELECT department, AVG(salary) as avg_sal
  FROM employees
  GROUP BY department
""")
sqlDF.show()
```

## 7. Writing Delta Tables (Bronze, Silver, Gold)
```scala
df.write.format("delta").mode("append").save("/mnt/delta/bronze/employees")
val silverDF = df.filter(col("salary").isNotNull)
silverDF.write.format("delta").mode("overwrite").save("/mnt/delta/silver/employees")
val goldDF = silverDF.groupBy("department").agg(avg("salary").alias("avg_salary"))
goldDF.write.format("delta").mode("overwrite").save("/mnt/delta/gold/department_salary")
```

## 8. Partitioning & Bucketing
```scala
df.write.partitionBy("year","month").format("delta").save("/mnt/delta/partitioned")
df.write.bucketBy(8, "id").sortBy("id").saveAsTable("bucketed_table")
```

## 9. Databricks Utilities (DBUtils)
```scala
dbutils.fs.ls("/mnt/data")
dbutils.fs.rm("/mnt/data/file.csv")
dbutils.secrets.get(scope="myScope", key="mySecretKey")
dbutils.widgets.text("param", "default")
val paramValue = dbutils.widgets.get("param")
```

## 10. Schema & Data Types
```scala
val schema = StructType(Array(
  StructField("id", IntegerType, true),
  StructField("name", StringType, true),
  StructField("salary", DoubleType, true)
))
val df = spark.read.schema(schema).json("/mnt/raw/data.json")
```

## 11. Error Handling & Logging
```scala
try {
  val df = spark.read.json("/mnt/raw/malformed.json")
  df.show()
} catch {
  case e: Exception => println(s"Error reading file: ${e.getMessage}")
}
spark.sparkContext.setLogLevel("WARN")
```

## 12. Performance Tuning
```scala
df.cache()
df.count()
df.repartition(10)
df.coalesce(1)
```

## 13. Advanced Engineering (Streaming, UDFs)
```scala
val addPrefix = udf((name: String) => "EMP_" + name)
df.withColumn("emp_name", addPrefix(col("name"))).show()

val streamDF = spark.readStream.format("delta").load("/mnt/delta/bronze")
val query = streamDF.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/mnt/delta/checkpoints/")
  .start("/mnt/delta/stream_out")
```
