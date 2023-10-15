package com.RX.Migration

import com.RX.Migration.Job.{CreateTargetTable, LakeToWarehouse, MigrateToLake, SparkRuntime, Task, UniqueCheck}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

import scala.collection.immutable
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.parsing.json.JSON

object Main extends SparkRuntime {
  def main(args: Array[String]): Unit = {
    val currentDate = LocalDate.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val dateString = currentDate.format(formatter)

    val tasks = tasksInit(dateString)

    val job:String = args(1)
    job match {
      case "UniqueCheck" => UniqueCheck.run(tasks)
      case "CreateTargetTables" => CreateTargetTable.run(tasks)
      case "MigrateToLake" => MigrateToLake.run(tasks)
      case "LakeToWarehouse" => LakeToWarehouse.run(tasks)
      case _ => sys.error(s"[ERROR] Invalid Argument: ${args(0)}")
    }

  }
  def tasksInit(dateStr:String):List[Task] = {
    val CONF = TasksConfiguration
    val tableConfig: DataFrame = spark.read
      .format("jdbc")
      .option("url", CONF.TBC_ADDR)
      .option("user", CONF.TBC_USER)
      .option("password", CONF.TBC_PSWD)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", s"(select  * from ${CONF.TABLE_CONFIG} where WATERMARK <>'$dateStr' or WATERMARK is null) tmp")
      .load()
    val connections: DataFrame = spark.read
      .format("jdbc")
      .option("url", CONF.TBC_ADDR)
      .option("user", CONF.TBC_USER)
      .option("password", CONF.TBC_PSWD)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", "RX_DW.connections")
      .load().drop("HOST_PORT")
      .distinct()
    val taskSchedule: DataFrame = tableConfig
      .join(connections, col("dbName") === connections("db_name"), "left")
      .filter(col("status") === '1')
      .filter(col("passwd").isNotNull)
      .select("TABLENAME", "DBNAME", "HOST_PORT",
        "TIMECOLS", "PRIMARYKEYS", "TARGETTABLE",
        "WATERMARK", "USERNAME", "PASSWD",
        "DBVERSION", "MAPPINGS")
      .distinct()
    val tasks:List[Task] = taskSchedule.collect().toList.map(ExtractInformation)
    tasks
  }
  def ExtractInformation(row:Row): Task ={
    val tableName:String = row.getString(0)
    val dbname:String = row.getString(1)
    val host_port:String = s"jdbc:mysql://${row.getString(2)}"
    val timeColStr:String = row.getString(3)
    var timeCols:List[String] = null
    var isIncrementalTask:Boolean = false
    if ( timeColStr.isInstanceOf[String] ){
      val timeCols:List[String] = timeColStr.split("\\|").toList
    }
    val primaryKeys:List[String] = row.getString(4).split("\\|").toList
    val targetTable:String = row.getString(5)
    val watermark:String = row.getString(6)
    if (timeColStr != null && watermark != null){
      isIncrementalTask = true
    }
    else {
      isIncrementalTask = false
    }
    val user:String = row.getString(7)
    val password:String = row.getString(8)
    val mappingStr: String = row.getString(10)
    var mappings: immutable.Map[String, String] = null
    if (mappingStr != null) {
      mappings = JSON.parseFull(mappingStr).getOrElse().asInstanceOf[immutable.Map[String, String]]
    }

    Job.Task(
      tableName = tableName,
      dbName = dbname,
      host_port = host_port,
      timeCols = timeCols,
      primaryKeys = primaryKeys,
      targetTable = targetTable,
      watermark = watermark,
      user = user,
      password = password,
      mappings = mappings,
      isIncremental = isIncrementalTask
    )
  }
}
