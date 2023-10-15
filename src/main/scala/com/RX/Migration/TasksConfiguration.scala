package com.RX.Migration

import java.time.LocalTime
import java.time.format.DateTimeFormatter

object TasksConfiguration {
  val HTT = 5000000
  val BS = 1000000
  val TBC_ADDR = "jdbc:oracle:thin:@10.18.10.102:1521:FRJDDBBY"
  val TBC_USER = "fl_syn"
  val TBC_PSWD = "fl_syn"
  val PROD_ADDR = "jdbc:oracle:thin:@10.18.10.101:1521:FRJDDB"
  val PROD_USER = "fl_syn"
  val PROD_PSWD = "fl_syn"

  val TABLE_CONFIG: String = "RX_DW.TABLE_CONFIG_PROD"
  val DEADLINE = LocalTime.parse("23:45:00", DateTimeFormatter.ofPattern("HH:mm:ss"))
}
