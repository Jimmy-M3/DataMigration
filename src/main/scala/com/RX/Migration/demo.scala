package com.RX.Migration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import scala.util.parsing.json.JSON
import java.sql.DriverManager
import java.time.{LocalDate, LocalTime}
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.Map
object demo {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().master("local").getOrCreate()

    val a:String = """{"col1":"字段1","col2":"字段2"}"""

    println(JSON.parseFull(a).getOrElse().asInstanceOf[Map[String,String]].getOrElse("col1",null))

  }
}
