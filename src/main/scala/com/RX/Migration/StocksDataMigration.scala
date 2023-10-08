package com.RX.Migration

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object StocksDataMigration {
  def main(args: Array[String]): Unit = {

    import java.sql._
//    Class.forName("com.mysql.jdbc.Driver")

    val spark: SparkSession =
      SparkSession.builder()
        .master("local[*]")
        .appName("Stocks Data Migration")
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val tasksList:List[Row] =
      spark.read
        .format("csv")
        .option("sep",",")
        .option("header","true")
        .load("src/main/resources")
        .collect()
        .toList

    for (task <- tasksList){
      val tableName = task.get(0)
      val dbName = task.get(1)
      val host_port = task.get(2)
      //        val pk = task.get(3)
      val version = task.get(4)
      val targetTable = task.get(5)

      var batches: Int = 0
      val conn = DriverManager.getConnection(s"jdbc:mysql://$host_port", "root", "Meng1014.")
      val resultSet = conn.createStatement().executeQuery(s"select mod(count(1),10) as batches from $dbName.$tableName")
      while (resultSet.next()) {
        batches = resultSet.getInt("batches")
      }
      println(s"[INFO] select mod(count(1),10) from $dbName.$tableName")
      println(batches)

      if (version == "8.0") {
        Class.forName("com.mysql.cj.jdbc.Driver")
        for (i <- 0 to batches-1) {
          println(s"(select * from $dbName.$tableName limit ${i * 10},10) t1")
          val source = spark.read
            .format("jdbc")
            .option("url", s"jdbc:mysql://$host_port")
            .option("user", "root")
            .option("password", "Meng1014.")
            .option("dbtable", s"(select * from $dbName.$tableName limit ${i * 10},10) t1")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()

          source.write
            .format("jdbc")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("url", s"jdbc:mysql://$host_port")
            .option("user", "root")
            .option("password", "Meng1014.")
            .option("dbtable", s"migration.$targetTable")
            .mode(SaveMode.Append)
            .save()
        }
      }
      else{
        Class.forName("com.mysql.jdbc.Driver")

      }
    }
    spark.close()
  }
}
