package com.RX.Migration

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object UniqueCheck {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("spark://node01:7077")
      .getOrCreate()



    val list = spark.read
      .format("csv")
      .option("header","true")
      .option("sep",",")
      .load("all-tables-formatted.csv")

    val connections = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("connections.csv")

    val tablesList = list.join(
      connections,
      col("database") === connections("database"),
      "left"
    ).persist(StorageLevel.MEMORY_ONLY)

    val lackNessInfo = tablesList.filter(col("password").isNull)

    val availableTables = tablesList.filter(col("password").isNotNull)

    case class Record(table:String)
    val valid:ArrayBuffer[Record] = ArrayBuffer()
    val inValid:ArrayBuffer[Record] = ArrayBuffer()

//    for (table availableTables){
//      val tableName:String = table.getString(0)
//      val database:String = table.getString(1)
//      val host_port:String = table.getString(2)
//      val primaryKeys:Array[String] = table.getString(4).split("|")
//      val user:String = table.getString(5)
//      val password:String = table.getString(6)
//
//      val data = spark.read
//        .format("jdbc")
//        .option("url",s"jdbc:mysql://$host_port")
//        .option("dbtable",s"(SELECT ${primaryKeys.mkString(",")} FROM $database.$tableName) t1")
//        .option("user",user)
//        .option("password",password)
//        .load().persist(StorageLevel.MEMORY_ONLY)
//
//      val totalRows:Long = data.count()
//      val uniqueRows:Long = data.distinct().count()
//
//      if (totalRows == uniqueRows){
//        valid += Record(tableName)
//      }
//      else {
//        inValid += Record(tableName)
//      }
//
//    }
    import spark.implicits._
//    valid.toSeq.toDF.write.format("csv").mode("overwrite").save("./validTables")
//    inValid.toSeq.toDF.write.format("csv").mode("overwrite").save("./inValidTables")

    spark.stop()
  }
}
