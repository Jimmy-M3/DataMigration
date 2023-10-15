package com.RX.Migration.Job

import org.apache.spark.sql.SparkSession

trait SparkRuntime {
  val spark: SparkSession = SparkSession.builder()
    .appName("Migration")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.legacy.timeParserPolicy","LEGACY")
    .config("spark.sql.parquet.int96RebaseModeInWrite","CORRECTED")
    .getOrCreate()

//  spark-shell --num-executors 2 --executor-memory 10G --executor-cores 5 --driver-memory 8G
  //  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  //  --conf "spark.sql.legacy.timeParserPolicy=LEGACY"
  //  --conf "spark.sql.parquet.int96RebaseModeInWrite=CORRECTED"
}
