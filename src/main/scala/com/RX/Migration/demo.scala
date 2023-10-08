package com.RX.Migration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

import java.sql.DriverManager
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}

object demo {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().master("local[4]").getOrCreate()
//
//    val df = spark.read.format("jdbc")
//      .option("url","jdbc:mysql://localhost:3306")
//      .option("user","root")
//      .option("password","Meng1014.")
//      .option("dbtable","(select * from test.test01 where 1=0) t1").load()
//    df.foreach(row =>{
//      println(row.get(0))
//    })
//
//    val src_schema = df.schema
//
//    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306","root","Meng1014.")
//    val stm = conn.createStatement()
//    val resultSet = stm.executeQuery("describe test.test01")
//    var a:mutable.Map[String,String] = mutable.Map()
//    while (resultSet.next()){
//      println(resultSet.getString("Type").replaceAll("\\(\\d+\\)",""))
//      a+=(resultSet.getString("Field") -> resultSet.getString("Type"))
//    }
//    var deleteODD:String = s"DELETE FROM RX_DW.targetTable WHERE "
//
//    deleteODD = (deleteODD + " 1=1").replaceAll(" a ","\\\"id\\\"")
//    println(a)
//    for (item <- a.keys){
//      println(a.getOrElse(item,""))
//    }
//
//    println(deleteODD)
    val a ="hello world"
    try{
      11 + a
    }
    catch {
      case e:Exception => println("hello")
    }
    println(a.contains("lo"))



//    spark.stop()
  }
}
