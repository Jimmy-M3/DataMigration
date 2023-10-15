package com.RX.Migration.Job

import com.RX.Migration.TasksConfiguration

import java.sql.{DriverManager, Statement}

object UniqueCheck extends Runnable {
  private[this] val tableConfigConnection = DriverManager.getConnection(TasksConfiguration.TBC_ADDR,TasksConfiguration.TBC_USER,TasksConfiguration.TBC_PSWD)
  private[this] val tbcStm:Statement = tableConfigConnection.createStatement()
  override def run(tasks: List[Task]): Unit = {
    for (i <- tasks.indices){
      val task = tasks(i)
      var totalRecordsNum:Int = 0
      var uniqueCheck:Int = 1
      val srcConnection = DriverManager.getConnection(task.host_port,task.user,task.password)
      val srcStm:Statement = srcConnection.createStatement()
      val uniqueCheckSQL:String = s"SELECT COUNT(1) AS unique FROM ${task.dbName}.${task.tableName} GROUP BY ${task.primaryKeys.mkString(", ")}"
      val totalRecordsNumSQL:String = s"SELECT COUNT(1) AS total FROM ${task.dbName}.${task.tableName}"
      var result = srcStm.executeQuery(uniqueCheckSQL)
      if (result.next()){
        uniqueCheck = result.getInt("unique")
      }
      result = srcStm.executeQuery(totalRecordsNumSQL)
      if (result.next()){
        totalRecordsNum = result.getInt("total")
      }
      if(uniqueCheck == totalRecordsNum){
        tbcStm.executeUpdate(s"UPDATE ${TasksConfiguration.TABLE_CONFIG} SET comments='Unique' WHERE dbname='${task.dbName}' AND tablename='${task.tableName}'")
      }
      else {
        tbcStm.executeUpdate(s"UPDATE ${TasksConfiguration.TABLE_CONFIG} SET comments='Not Unique' WHERE dbname='${task.dbName}' AND tablename='${task.tableName}'")
      }
    }
  }
}
