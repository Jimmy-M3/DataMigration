package com.RX.Migration.Job

trait Runnable extends SparkRuntime {
  def run(tasks: List[Task])
  def generateTimeClause(timeCols:List[String],watermark:String): String ={
    s" ${timeCols.mkString(s" >= UNIX_TIMESTAMP('$watermark')  OR ")} >= UNIX_TIMESTAMP('$watermark')"
  }
}
