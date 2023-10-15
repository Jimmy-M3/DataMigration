package com.RX.Migration

import com.RX.Migration.Job.{Runnable, Task}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.sql.DriverManager
import scala.math.ceil

object DataMigration extends Runnable {
  override def run(tasks: List[Task]): Unit = {
    for (i <- tasks.indices){
      val task = tasks(i)
      var data:DataFrame = null
      val hugeTableBatches = isHuge(task)

      if (hugeTableBatches > 0){
        for (i <- 1 to hugeTableBatches) {
          var limitClause: String = s" LIMIT ${i}"
          var data: DataFrame = spark.read
            .format("jdbc")
            .option("url", s"jdbc:mysql://${task.host_port}?zeroDateTimeBehavior=convertToNull")
            .option("driver", "")
            .option("user", task.user)
            .option("password", task.password)
            .option("dbtable", "")
            .load()

          data.write.format("jdbc")
        }
      }


    }
  }
  def isHuge(task:Task):Int = {
    val srcConnection = DriverManager.getConnection(s"jdbc:mysql://${task.host_port}", task.user, task.password)
    val srcStm = srcConnection.createStatement()
    var timeClause = " WHERE "

    if ( task.timeCols != null ){
      if (task.timeCols.size == 1){
        timeClause += s"${task.timeCols.head} >= UNIX_TIMESTAMP('${task.watermark}')"
      }
      else if(task.timeCols.size > 1) {
        timeClause += task.timeCols.mkString(s" >= UNIX_TIMESTAMP('${task.watermark}') OR ")
      }
    }
    val resultSet = srcStm.executeQuery(s"SELECT COUNT(1) AS cnt FROM ${task.dbName}.${task.tableName} $timeClause")
    var cnt:Int = 0
    var batches:Int = 0
    while (resultSet.next()){
      cnt = resultSet.getInt("cnt")
    }

    if ( cnt >= TasksConfiguration.HTT ){
      batches = ceil(cnt.toDouble / TasksConfiguration.BS).toInt
    }
    else{
      batches = -1
    }
    batches
  }
}
