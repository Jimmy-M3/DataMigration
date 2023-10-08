package com.RX.Migration


import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType}

import java.io.FileInputStream
import java.sql.{DriverManager, SQLSyntaxErrorException, Statement}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math.ceil
import scala.util.Properties


object Migration {
  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .appName("Migration")
      .getOrCreate()

    Class.forName("oracle.jdbc.driver.OracleDriver")
    Class.forName("com.mysql.jdbc.Driver")
    Class.forName("com.mysql.cj.jdbc.Driver")

    val HTT = 5000000
    val BS = 1000000
    val table_config:String ="RX_DW.table_config1"
    val oracle_conn = DriverManager.getConnection("jdbc:oracle:thin:@10.18.10.102:1521:FRJDDBBY","fl_syn","fl_syn")
    val oracle_stm = oracle_conn.createStatement()
    val tableConfig:DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@10.18.10.102:1521:FRJDDBBY")
      .option("user", "fl_syn")
      .option("password", "fl_syn")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", table_config)
      .load()
    val connections:DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@10.18.10.102:1521:FRJDDBBY")
      .option("user", "fl_syn")
      .option("password", "fl_syn")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", "RX_DW.connections")
      .load()
    val taskSchedule:DataFrame = tableConfig
      .join(connections, col("dbName") === connections("db_name"), "left")
      .filter(col("status") === '1')
      .filter(col("passwd").isNotNull)
      .drop("status")
    val tasks = taskSchedule.collectAsList()


    // TODO 1: For-Loop 每个数据表对应一个迁移任务（Task）
    for (i <- 0 until tasks.size()){
      val informations = tasks.get(i)
      val tableName:String = informations.getString(0)
      val dbName:String = informations.getString(1)
      val host_port:String = informations.getString(2)
      val tc = informations.getString(3)
      var timeCols:List[String] = null
      // TODO 1.1: timeCols字段按‘|’切分成List[String]
      if (tc.isInstanceOf[String]){
         timeCols = tc.split("\\|").toList
      }

      val primaryKeys:List[String] = informations
        .getString(4)
        .split("\\|")
        .toList

      val targetTable:String = informations.getString(6)
      val watermark:String = informations.getString(7)
      val user:String = informations.getString(11)
      val password:String = informations.getString(12)

      var timeClause:String = null
      var limitClause:String = ""
      var isIncremental:Boolean = false
      var writeMode:String = "overwrite"

      if (watermark == null || timeCols == null){
        timeClause = ""
      }
      else if(watermark != null) {
        isIncremental = true
        writeMode = "append"
        if (timeCols.size > 1){
          timeClause = s" where " + timeCols.mkString(s" >= '$watermark' and ") + s">= '$watermark'"
        }
        else if (timeCols.size == 1){
          timeClause = s" where ${timeCols.head} >= '$watermark'"
        }
      }
      println(s"[INFO] IsIncremental: $isIncremental; Write Mode: $writeMode .")
      println(s"[INFO] TIME CLAUSE: $timeClause")
      var totalRowNumbers: Double = 0.0
      val conn = DriverManager.getConnection(s"jdbc:mysql://$host_port", user, password)
      val stm: Statement = conn.createStatement()

      val schemaResult = stm.executeQuery(s"describe $dbName.$tableName")
      var schema:ArrayBuffer[String] = ArrayBuffer()                // 用于首次创建目标表
      var scm:mutable.Map[String,String] = mutable.Map()
      while (schemaResult.next()){
        schema += s"${schemaResult.getString("Field")}  ${schemaResult.getString("Type")}"
        scm+=(schemaResult.getString("Field") -> schemaResult.getString("Type"))
      }
      val ddl: String = s"create table RX_DW.$targetTable ( ${schema.mkString(",")} )"
        .replaceAll("varchar", "varchar2")
        .replaceAll("tinyint\\(?\\d+\\)?", "number(3,0)")
        .replaceAll("smallint\\(?\\d+\\)?", "number(5,0)")
        .replaceAll("mediumint\\(?\\d+\\)?", "number(7,0)")
        .replaceAll("bigint\\(?\\d+\\)?", "number(20,0)")
        .replaceAll("int\\(?\\d+\\)?", "number(10,0)")
        .replaceAll("double", "number")
        .replaceAll("float", "number")
        .replaceAll("decimal", "number")
        .replaceAll("datetime", "timestamp")
        .replaceAll("unsigned", "")
      try{ oracle_stm.execute(ddl) }
      catch {
        case e:SQLSyntaxErrorException => if (e.getMessage.contains("ORA-00955")) {println(s"[INFO]Table Already Exist: $ddl")} else{println(s"[ERROR] $e")}
      }

      try{
        val resultSet = stm.executeQuery(s"SELECT COUNT(1) as cnt FROM $dbName.$tableName $timeClause")
        while (resultSet.next()) {
          totalRowNumbers = resultSet.getDouble("cnt")
        }
        if (totalRowNumbers > HTT) {
          val batches: Int = ceil(totalRowNumbers / BS).toInt
          for (i <- 0 until batches) {
            limitClause = s" LIMIT ${i * BS},${BS} "
            println(limitClause)
            println(s"[INFO] (Select * from $dbName.$tableName $timeClause $limitClause) t1")
            val data = spark.read
              .format("jdbc")
              .option("url", s"jdbc:mysql://$host_port")
              .option("user", user)
              .option("password", password)
              .option("driver", "com.mysql.cj.jdbc.Driver")
              .option("dbtable", s"(Select * from $dbName.$tableName $timeClause $limitClause) t1")
              .load()

            //todo: 删除过时数据
            if (isIncremental) {
              var primaryKeysDF: DataFrame = null

              if (primaryKeys.size == 1) {
                primaryKeysDF = data.select(primaryKeys.head)
              }
              else {
                primaryKeysDF = data.select(primaryKeys.head, primaryKeys.drop(1): _*)
              }
              var deleteODD: String = s"DELETE FROM RX_DW.$targetTable WHERE "
              for (i <- primaryKeysDF.schema.indices) {
                val field = primaryKeysDF.schema(i).name
                scm.getOrElse(field, "") match {
                  case "int" => deleteODD = deleteODD + s" $field = to_number(?) AND"
                  case "tinyint" => deleteODD = deleteODD + s" $field = to_number(?) AND"
                  case "mediumint" => deleteODD = deleteODD + s" $field = to_number(?) AND"
                  case "bigint" => deleteODD = deleteODD + s" $field = to_number(?) AND"
                  case _ => deleteODD = deleteODD + s" $field = ? AND"
                }
              }
              deleteODD = (deleteODD + " 1=1").replaceAll(" id ", "\\\"id\\\"")
              primaryKeysDF.distinct().coalesce(1).foreachPartition(
                (partition: Iterator[Row]) => {
                  val oconn = DriverManager.getConnection("jdbc:oracle:thin:@10.18.10.102:1521:FRJDDBBY", "fl_syn", "fl_syn")
                  val pstm = oconn.prepareStatement(deleteODD)
                  partition.foreach(
                    row => {
                      for (i <- 1 to row.size) {
                        pstm.setObject(i, row.get(i - 1))
                      }
                      pstm.addBatch()
                    }
                  )

                  val a = pstm.executeBatch()
                }
              )
            }

            data.write
              .format("jdbc")
              .mode(writeMode)
              .option("url", "jdbc:oracle:thin:@//10.18.10.102:1521/FRJDDBBY")
              .option("user", "fl_syn")
              .option("password", "fl_syn")
              .option("driver", "oracle.jdbc.driver.OracleDriver")
              .option("dbtable", s"RX_DW.${targetTable}")
              .option("truncate",true)
              .save()

          }
                  }
        else {
          println(s"[INFO] (Select * from $dbName.$tableName $timeClause $limitClause) t1")
          val data = spark.read
            .format("jdbc")
            .option("url", s"jdbc:mysql://$host_port")
            .option("user", user)
            .option("password", password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", s"( Select * from $dbName.$tableName $timeClause ) t1")
            .load()

          //todo: 删除过时数据
          if (isIncremental) {
            var primaryKeysDF:DataFrame = null

            if (primaryKeys.size == 1 ){
              primaryKeysDF = data.select(primaryKeys.head)
            }
            else {
              primaryKeysDF = data.select(primaryKeys.head,primaryKeys.drop(1):_*)
            }
            var deleteODD: String = s"DELETE FROM RX_DW.$targetTable WHERE "
            for (i <- primaryKeysDF.schema.indices) {
              val field = primaryKeysDF.schema(i).name
              scm.getOrElse(field, "") match {
                case "int" => deleteODD = deleteODD + s" $field = to_number(?) AND"
                case "tinyint" => deleteODD = deleteODD + s" $field = to_number(?) AND"
                case "mediumint" => deleteODD = deleteODD + s" $field = to_number(?) AND"
                case "bigint" => deleteODD = deleteODD + s" $field = to_number(?) AND"
                case _ => deleteODD = deleteODD + s" $field = ? AND"
              }
            }
            deleteODD = (deleteODD + " 1=1").replaceAll(" id ", "\\\"id\\\"")
            primaryKeysDF.distinct().coalesce(1).foreachPartition(
              (partition:Iterator[Row]) => {
                val oconn = DriverManager.getConnection("jdbc:oracle:thin:@10.18.10.102:1521:FRJDDBBY", "fl_syn", "fl_syn")
                val pstm = oconn.prepareStatement(deleteODD)
                partition.foreach(
                  row => {
                    for (i <- 1 to row.size){
                      pstm.setObject(i,row.get(i-1))
                    }
                    pstm.addBatch()
                  }
                )
                val a = pstm.executeBatch()
              }
            )
          }

          data.write
            .format("jdbc")
            .mode(writeMode)
            .option("url", "jdbc:oracle:thin:@//10.18.10.102:1521/FRJDDBBY")
            .option("user", "fl_syn")
            .option("password", "fl_syn")
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .option("dbtable", s"RX_DW.${targetTable}")
            .option("truncate",true)
            .save()

        }
        val currentDate = LocalDate.now()
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val dateString = currentDate.format(formatter)
        oracle_stm.executeUpdate(s"update $table_config set watermark = '$dateString' where targetTable = '$targetTable'")
      }catch {
        case e:Exception => println(s"[ERROR] ${e.getMessage}")
      }
    }


    spark.stop()
  }
}
