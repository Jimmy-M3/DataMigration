package com.RX.Migration.Job

import com.RX.Migration.TasksConfiguration
import io.delta.implicits.DeltaDataFrameReader
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode}

object MigrateToLake extends Runnable {
  override def run(tasks: List[Task]): Unit = {
    for (i <- tasks.indices) {
      val task: Task = tasks(i)
      if (task.isIncremental) {
        val timeClause = generateTimeClause(task.timeCols, task.watermark)

        val data: DataFrame = spark.read
          .format("jdbc")
          .option("url", task.host_port+"?zeroDateTimeBehavior=convertToNull")
          .option("user", task.user)
          .option("password", task.password)
          .option("dbtable", s"(SELECT * FROM ${task.dbName}.${task.tableName} WHERE $timeClause) T1")
          .load()

        val target: DeltaTable = DeltaTable.forName(task.targetTable)
        val condition:String = generateMergeCondition(task.primaryKeys)

        target.as("target")
          .merge(
            data.as("source"),
            condition = condition
          ).whenMatched.updateAll()
          .whenNotMatched.insertAll()
          .execute()
      }
      else {
        val data:DataFrame =  spark.read
          .format("jdbc")
          .option("url", task.host_port)
          .option("user", task.user)
          .option("password", task.password)
          .option("dbtable", s"${task.dbName}")
          .load()

        data.write
          .format("delta")
          .mode(SaveMode.Overwrite)
          .save(task.targetTable)
      }

    }
  }
  private def generateMergeCondition(primaryKeys:List[String]): String ={
    s"${primaryKeys.map(key => s"target.$key = source.$key").mkString(" AND ")}"
  }
}
