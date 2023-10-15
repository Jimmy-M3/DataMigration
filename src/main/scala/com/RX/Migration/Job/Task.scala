package com.RX.Migration.Job

import scala.collection.immutable

case class Task(
                 tableName: String,
                 dbName: String,
                 host_port: String,
                 timeCols: List[String],
                 primaryKeys: List[String],
                 targetTable: String,
                 watermark: String,
                 user: String,
                 password: String,
                 mappings: immutable.Map[String, String],
                 isIncremental: Boolean
               )
