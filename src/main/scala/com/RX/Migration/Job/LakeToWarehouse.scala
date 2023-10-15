package com.RX.Migration.Job

import com.RX.Migration.TasksConfiguration
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode}

object LakeToWarehouse extends Runnable with WareHouseConnection {
  override def run(tasks: List[Task]): Unit = {
    for (i <- tasks.indices){
      val task = tasks(i)
      val recordsNum:Int = spark.read.format("delta").load(task.targetTable).queryExecution.optimizedPlan.stats.rowCount.get.toInt
      var data:DataFrame = null
      if (task.isIncremental){
        val timeClause = generateTimeClause(task.timeCols,task.watermark)
        data = spark.read.format("delta").load(task.targetTable).filter(timeClause)
      }
      else {
        data = spark.read.format("delta").load(task.targetTable)
      }
      data.persist()
      if (recordsNum > TasksConfiguration.HTT){
        val tmpTableName:String = task.tableName+"_tmp"
        val tmpTableDDL = CreateTargetTable.generateDDL(task).replaceAll(task.targetTable,tmpTableName)
        whStm.execute(tmpTableDDL)
        data.write.format("jdbc").format("jdbc")
          .mode(SaveMode.Overwrite)
          .option("url", TasksConfiguration.PROD_ADDR)
          .option("user", TasksConfiguration.PROD_USER)
          .option("password", TasksConfiguration.PROD_PSWD)
          .option("driver", "oracle.jdbc.driver.OracleDriver")
          .option("dbtable", s"RX_DW.${tmpTableName}")
          .option("truncate", true)
          .save()
        warehouseConnection.prepareCall("")
      }
      else {
        data.write.format("jdbc").format("jdbc")
          .mode(SaveMode.Overwrite)
          .option("url", TasksConfiguration.PROD_ADDR)
          .option("user", TasksConfiguration.PROD_USER)
          .option("password", TasksConfiguration.PROD_PSWD)
          .option("driver", "oracle.jdbc.driver.OracleDriver")
          .option("dbtable", s"RX_DW.${task.targetTable}")
          .option("truncate",true)
          .save()
      }
    }
  }
}
