package com.RX.Migration.Job

import com.RX.Migration.TasksConfiguration

import java.sql.{DriverManager, Statement}
import scala.collection.mutable.ArrayBuffer

object CreateTargetTable extends Runnable with WareHouseConnection {

  override def run(tasks: List[Task]): Unit = {
    for (i <- tasks.indices){
      val task:Task = tasks(i)
      val DDL = generateDDL(task)
      whStm.execute(DDL)
    }
  }

  def generateDDL(task:Task): String ={
    val srcConnection = DriverManager.getConnection(task.host_port, task.user, task.password)
    val srcStm = srcConnection.createStatement()
    val srcTableSchema: ArrayBuffer[String] = null
    val resultSet = srcStm.executeQuery(s"DESC ${task.dbName}.${task.tableName}")
    while (resultSet.next()) {
      srcTableSchema += "\""+s"${resultSet.getString("Field")}"+"\""+s"  ${resultSet.getString("Type")}"
    }
    val DDL: String = s"CREATE TABLE RX_DW.${task.tableName} ( ${srcTableSchema.mkString(",")} )"
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
    DDL
  }
}
