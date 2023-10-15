package com.RX.Migration.Job

import com.RX.Migration.TasksConfiguration

import java.sql.{DriverManager, Statement}

trait WareHouseConnection {
  val warehouseConnection = DriverManager.getConnection(TasksConfiguration.TBC_ADDR, TasksConfiguration.TBC_USER, TasksConfiguration.TBC_PSWD)
  val whStm: Statement = warehouseConnection.createStatement()
}
