package com.RX.Migration

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime, lit, unix_timestamp}
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
    val spark = SparkSession.builder().master("local").getOrCreate()
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "Meng1014.")
    val stm = conn.createStatement()
    stm.execute("create table if not exists test.test111_tt( id varchar(100),name varchar(100), age int)")
    val data = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306")
      .option("user","root")
      .option("password","Meng1014.")
      .option("dbtable","test.test111_tmp")
      .load()

    DeltaTable.forName("")

    data.write
      .format("jdbc").mode("overwrite")
//      .option("createTableOptions","create table test.test111_tt( id varchar(100),name varchar(100), age int)")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("user", "root")
      .option("password", "Meng1014.")
      .option("dbtable","test.test111_tt")
      .save()
//
//
//    stm.execute("delete from test.test111 where (id) in (select id from test.test111_tt)")
//    stm.execute("insert into test.test111 select * from test.test111_tt")
//    stm.execute("drop table test.test111_tt")
    val primaryKeys = List("name","id","col2")
    println(s"${primaryKeys.map(key => s"target.$key = source.$key").mkString(" AND ")}")
    println(args(0))


  }
}
