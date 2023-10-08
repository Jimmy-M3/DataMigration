package com.RX.Migration

import org.apache.spark.sql.SparkSession

object IncrementalDataMigration {
  def main(args: Array[String]): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val spark:SparkSession = SparkSession.builder().master("local[4]").getOrCreate()

    val df = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306")
      .option("user","root").option("password","Meng1014.")
      .option("dbtable","(select * from migration.test_source_table limit 0,10) t1")
      .load()
    df.show()
  }

}
