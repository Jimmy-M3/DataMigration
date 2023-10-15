package com.RX.Migration


import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType}

import java.io.FileInputStream
import java.sql.{DriverManager, SQLSyntaxErrorException, Statement}
import java.time.{LocalDate, LocalTime}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.{breakOut, mutable}
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.immutable
import scala.math.ceil
import scala.util.Properties
import scala.util.parsing.json.JSON


object Migration {
  def main(args: Array[String]): Unit = {

    //TODO 1 Init SparkSession
    val spark:SparkSession = SparkSession.builder()
      .appName("Migration")
      .getOrCreate()

    //TODO 1.1 Init Mysql&Oracle Drivers
    Class.forName("oracle.jdbc.driver.OracleDriver")
    Class.forName("com.mysql.jdbc.Driver")
    Class.forName("com.mysql.cj.jdbc.Driver")

    //TODO 1.2 Loading Configurations
    val HTT = 3000000
    val BS = 1000000
    val TBC_ADDR = "jdbc:oracle:thin:@10.18.10.102:1521:FRJDDBBY"
//    val PROD_ADDR= "jdbc:oracle:thin:@10.18.10.101:1521:FRJDDB"
    val PROD_ADDR= "jdbc:oracle:thin:@10.18.10.102:1521:FRJDDBBY"
    val table_config:String ="RX_DW.TABLE_CONFIG_DEMO"
    val DEADLINE = LocalTime.parse("23:59:00", DateTimeFormatter.ofPattern("HH:mm:ss"))
    val currentDate = LocalDate.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val dateString = currentDate.format(formatter)
    val tbc_conn = DriverManager.getConnection(TBC_ADDR,"fl_syn","fl_syn")
    val tbc_stm = tbc_conn.createStatement()
    val prod_conn = DriverManager.getConnection(PROD_ADDR, "fl_syn", "fl_syn")
    val prod_stm = prod_conn.createStatement()
    val tableConfig:DataFrame = spark.read
      .format("jdbc")
      .option("url", TBC_ADDR)
      .option("user", "fl_syn")
      .option("password", "fl_syn")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", s"(select  * from $table_config where WATERMARK <>'$dateString' or WATERMARK is null) tmp")
      .load()
    val connections:DataFrame = spark.read
      .format("jdbc")
      .option("url", TBC_ADDR)
      .option("user", "fl_syn")
      .option("password", "fl_syn")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", "RX_DW.connections")
      .load().drop("HOST_PORT")
      .distinct()
    val taskSchedule:DataFrame = tableConfig
      .join(connections, col("dbName") === connections("db_name"), "left")
      .filter(col("status") === '1')
      .filter(col("passwd").isNotNull)
      .drop("status")
    val tasks = taskSchedule
      .select("TABLENAME","DBNAME","HOST_PORT",
              "TIMECOLS","PRIMARYKEYS","TARGETTABLE",
              "WATERMARK","USERNAME","PASSWD",
              "DBVERSION","MAPPINGS")
      .collectAsList()

    // TODO 2 : For-Loop Recursive Execute Tasks
    for (i <- 0 until tasks.size()){
      // TODO deadline
      val curTime = LocalTime.now()
      if (curTime.compareTo(DEADLINE) == 1) {
        println(s"${LocalTime.now()} [INFO] Time Out, Spark Graceful Shutdown !")
        sys.exit(0)
      }
      // TODO 2.1 Load necessary information for each Task
      val informations = tasks.get(i)
      val tableName:String = informations.getString(0)
      val dbName:String = informations.getString(1)
      val host_port:String = informations.getString(2)
      val tc = informations.getString(3)
      var timeCols:List[String] = null
      val targetTable: String = informations.getString(5)
      val watermark: String = informations.getString(6)
      val user: String = informations.getString(7)
      val password: String = informations.getString(8)
      val mappingStr:String = informations.getString(10)
      var mappings:immutable.Map[String,String] = immutable.Map[String,String]()
      if (mappingStr != null) {
        mappings = JSON.parseFull(mappingStr).getOrElse().asInstanceOf[immutable.Map[String,String]]
      }
      println(s"${LocalTime.now()} [INFO] ========== $targetTable ==========")
      // TODO 2.1.1: timeCols String Split
      if (tc.isInstanceOf[String]){
         timeCols = tc.split("\\|").toList
      }
      // TODO 2.1.2: PrimaryKeys String Split
      val primaryKeys:List[String] = informations
        .getString(4)
        .split("\\|")
        .toList

      // TODO 2.2: Building SQL Clause(time clause for incremental tasks;limit clause for huge tables)
      var timeClause:String = ""
      var limitClause:String = ""
      var isIncremental:Boolean = false
      var writeMode:String = "overwrite"

      if (watermark == null || timeCols == null){
        timeClause = ""
      }
      else if(watermark != null && watermark != dateString) {
        isIncremental = true
        writeMode = "append"
        if (timeCols.size > 1){
          timeClause = s" where " + timeCols.mkString(s" >= unix_timestamp('$watermark') or ") + s">= unix_timestamp('$watermark')"
        }
        else if (timeCols.size == 1){
          timeClause = s" where ${timeCols.head} >= unix_timestamp('$watermark')"
        }
      }
      else if(watermark == dateString){
        isIncremental = false
        writeMode = "append"
        timeClause = "where 1=0"
        println(s"[INFO] $targetTable:Already updated, this task will be skipped")
      }
      println(s"[INFO] IsIncremental: $isIncremental; Write Mode: $writeMode .")
      println(s"[INFO] TIME CLAUSE: $timeClause")
      // TODO 2.3 Identify a Huge Table or Not
      var totalRowNumbers: Double = 0.0
      val conn = DriverManager.getConnection(s"jdbc:mysql://$host_port", user, password)
      val stm: Statement = conn.createStatement()

      val schemaResult = stm.executeQuery(s"describe $dbName.$tableName")
      var schema:ArrayBuffer[String] = ArrayBuffer()                // 用于首次创建目标表
      var scm:mutable.Map[String,String] = mutable.Map()
      while (schemaResult.next()){
        schema += ("\""+s"${schemaResult.getString("Field")}"+"\""+s" ${schemaResult.getString("Type")}")
        scm+=(schemaResult.getString("Field") -> schemaResult.getString("Type"))
      }
      var ddl: String = s"create table RX_DW.$targetTable ( ${schema.mkString(",")} )"
        .replaceAll("\\svarchar\\(\\d+\\)", " varchar2(1000) ")
        .replaceAll("tinyint\\(?\\d+\\)?|tinyint", " number(5,0) ")
        .replaceAll("smallint\\(?\\d+\\)?|smallint", " number(7,0) ")
        .replaceAll("mediumint\\(?\\d+\\)?|mediumint", " number(9,0) ")
        .replaceAll("bigint\\(?\\d+\\)?|bigint", " number(24,0) ")
        .replaceAll("\\sint\\(?\\d+\\)?\\s|\\sint\\s", " number(12,0) ")
        .replaceAll("\\sint\\(", " number(")
        .replaceAll("\\sint,", " number(12,0),")
        .replaceAll("\\sdouble", "number")
        .replaceAll("\\sfloat", "number")
        .replaceAll("\\sdecimal", "number")
        .replaceAll("\\sdatetime", "timestamp")
        .replaceAll("\\stimestamp\\(\\d+\\)", "timestamp")
        .replaceAll("\\sbit\\(1\\)", " number(2,0)")
        .replaceAll("\\stext", " clob ")
        .replaceAll("\\slong", " varchar2(4000) ")
        .replaceAll("\\smediumtext", " clob ")
        .replaceAll("\\sunsigned", "")

      if ( mappingStr != null ){
        for (i <- mappings.keys) {
          ddl = ddl.replaceAll(i, mappings.getOrElse(i,null))
          println(s"[INFO] Fields Mapping: $i -> ${mappings.getOrElse(i, null)}")
        }
      }
      println(s"[INFO] DDL: $ddl")
      try{ prod_stm.execute(ddl) }
      catch {
        case e:SQLSyntaxErrorException => if (e.getMessage.contains("ORA-00955")) {println(s"[INFO]Table Already Exist.")} else{println(s"[ERROR] $e")}
      }

      try{
        val resultSet = stm.executeQuery(s"SELECT COUNT(1) as cnt FROM $dbName.$tableName $timeClause")
        while (resultSet.next()) {
          totalRowNumbers = resultSet.getDouble("cnt")
          println(s"[INFO] Query:SELECT COUNT(1) as cnt FROM $dbName.$tableName $timeClause  -->>  $totalRowNumbers")
        }
        if (totalRowNumbers > HTT) {
          //TODO 大表分批
          val batches: Int = ceil(totalRowNumbers / BS).toInt
          for (i <- 0 until batches) {
            limitClause = s" LIMIT ${i * BS},${BS} "
            println(limitClause)
            println(s"[INFO] (Select * from $dbName.$tableName $timeClause $limitClause) t1")
            var data = spark.read
              .format("jdbc")
              .option("url", s"jdbc:mysql://$host_port?zeroDateTimeBehavior=convertToNull")
              .option("user", user)
              .option("password", password)
              .option("driver", "com.mysql.cj.jdbc.Driver")
              .option("dbtable", s"(Select * from $dbName.$tableName $timeClause $limitClause) t1")
              .load()
            if (mappingStr != null) {
              for (i <- mappings.keys) {
                data = data.withColumnRenamed(i, mappings.getOrElse(i, ""))
                println(s"[INFO] Fields Mapping: $i -> ${mappings.getOrElse(i,null)}")
              }
            }

            //todo: 删除过时数据
//            if (isIncremental) {
//              var primaryKeysDF: DataFrame = null
//
//              if (primaryKeys.size == 1) {
//                primaryKeysDF = data.select(primaryKeys.head).distinct().repartition(20)
//              }
//              else {
//                primaryKeysDF = data.select(primaryKeys.head, primaryKeys.drop(1): _*).distinct().repartition(20)
//              }
//              var deleteODD: String = s"DELETE FROM RX_DW.$targetTable WHERE "
//              for (i <- primaryKeysDF.schema.indices) {
//                val field = primaryKeysDF.schema(i).name
//                scm.getOrElse(field, "") match {
//                  case "int" => deleteODD = deleteODD + " \"" + s"$field" +"\" = to_number(?) AND"
//                  case "tinyint" => deleteODD = deleteODD + " \"" + s"$field" +"\" = to_number(?) AND"
//                  case "mediumint" => deleteODD = deleteODD + " \"" + s"$field" +"\" = to_number(?) AND"
//                  case "bigint" => deleteODD = deleteODD + " \"" + s"$field" +"\" = to_number(?) AND"
//                  case _ => deleteODD = deleteODD + " \"" + s"$field" +"\" = ? AND"
//                }
//              }
//              deleteODD = deleteODD + " 1=1"
//              println(s"[INFO] DELETE: $deleteODD")
//
//              primaryKeysDF.foreachPartition(
//                (partition: Iterator[Row]) => {
//                  val oconn = DriverManager.getConnection(PROD_ADDR, "fl_syn", "fl_syn")
//                  val pstm = oconn.prepareStatement(deleteODD)
//                  partition.foreach(
//                    row => {
//                      for (i <- 1 to row.size) {
//                        pstm.setObject(i, row.get(i - 1))
//                      }
//                      pstm.addBatch()
//                    }
//                  )
//                  val a = pstm.executeBatch()
//                }
//              )
//            }

            data.repartition(20).write
              .format("jdbc")
              .mode(writeMode)
              .option("url", PROD_ADDR)
              .option("user", "fl_syn")
              .option("password", "fl_syn")
              .option("driver", "oracle.jdbc.driver.OracleDriver")
              .option("dbtable", s"RX_DW.${targetTable}")
//              .option("truncate",true)
              .save()
            writeMode="append"
          }
                  }
        else {
          //TODO 小表直接写
          println(s"[INFO] (Select * from $dbName.$tableName $timeClause $limitClause) t1")
          var data = spark.read
            .format("jdbc")
            .option("url", s"jdbc:mysql://$host_port?zeroDateTimeBehavior=convertToNull")
            .option("user", user)
            .option("password", password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", s"( Select * from $dbName.$tableName $timeClause) t1") //测试10rows
            .load()

          if (mappingStr != null){
            for (i <- mappings.keys){
              data = data.withColumnRenamed(i,mappings.getOrElse(i,""))
              println(s"[INFO] Fields Mapping: $i -> ${mappings.getOrElse(i,null)}")
            }
          }

          //todo: 删除过时数据
//          if (isIncremental) {
//            var primaryKeysDF:DataFrame = null
//
//            if (primaryKeys.size == 1 ){
//              primaryKeysDF = data.select(primaryKeys.head).distinct().repartition(20)
//            }
//            else {
//              primaryKeysDF = data.select(primaryKeys.head,primaryKeys.drop(1):_*).distinct().repartition(20)
//            }
//            var deleteODD: String = s"DELETE FROM RX_DW.$targetTable WHERE "
//            for (i <- primaryKeysDF.schema.indices) {
//              val field = primaryKeysDF.schema(i).name
//              scm.getOrElse(field, "") match {
//                case "int" => deleteODD = deleteODD + " \"" + s"$field" + "\" = to_number(?) AND"
//                case "tinyint" => deleteODD = deleteODD + " \"" + s"$field" + "\" = to_number(?) AND"
//                case "mediumint" => deleteODD = deleteODD + " \"" + s"$field" + "\" = to_number(?) AND"
//                case "bigint" => deleteODD = deleteODD + " \"" + s"$field" + "\" = to_number(?) AND"
//                case _ => deleteODD = deleteODD + " \"" + s"$field" + "\" = ? AND"
//              }
//            }
//            deleteODD = deleteODD + " 1=1"
//            println(s"[INFO] DELETE: $deleteODD")
//            primaryKeysDF.foreachPartition(
//              (partition:Iterator[Row]) => {
//                val oconn = DriverManager.getConnection(PROD_ADDR, "fl_syn", "fl_syn")
//                val pstm = oconn.prepareStatement(deleteODD)
//                partition.foreach(
//                  row => {
//                    for (i <- 1 to row.size){
//                      pstm.setObject(i,row.get(i-1))
//                    }
//                    pstm.addBatch()
//                  }
//                )
//                val a = pstm.executeBatch()
//              }
//            )
//          }

          val create_tempTable = ddl.replaceAll(targetTable,targetTable+"_tmp")
          prod_stm.execute(create_tempTable)

          data.repartition(20).write
            .format("jdbc")
            .mode(writeMode)
            .option("url", PROD_ADDR)
            .option("user", "fl_syn")
            .option("password", "fl_syn")
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .option("dbtable", s"RX_DW.${targetTable}_tmp")
            .option("truncate",true)
            .save()
          val deleteSQL = s"DELETE FROM RX_DW.${targetTable} where (${primaryKeys.mkString(",")}) in (select ${primaryKeys.mkString(",")} from RX_DW.${targetTable}_tmp)"
          println("[INFO] DELETE according Tmp Table: "+deleteSQL)
          prod_stm.execute(deleteSQL)
          val updateSQL = s"INSERT INTO RX_DW.${targetTable} select * from RX_DW.${targetTable}_tmp"
          println("[INFO] Updated according Tmp Table: "+updateSQL)
          prod_stm.execute(updateSQL)

        }

        tbc_stm.executeUpdate(s"update $table_config set watermark = '$dateString' where targetTable = '$targetTable'")
        tbc_stm.executeUpdate(s"UPDATE $table_config set comments= 'SUCCESS' WHERE  targetTable = '$targetTable'")

      }catch {
        case e:Exception => {
          println(s"[ERROR] ${targetTable}: ${e.getMessage}")
          tbc_stm.executeUpdate(s"UPDATE $table_config set comments= 'FAILED' WHERE  targetTable = '$targetTable'")
        }
      }
    }


    spark.stop()
  }
}
