package dev

import org.apache.spark.sql.SparkSession

trait SparkSessionSetup {

  private def getSparkSession: SparkSession = SparkSession
    .builder()
    .appName("Streaming example")
    .master("local")
    .getOrCreate()

  def withSparkSession(sparkFunc: SparkSession => Unit): Unit = {
    val ss = getSparkSession
    try {
      sparkFunc.apply(ss)
    } catch {
      case e: Exception => throw e
    } finally {
      ss.close()
    }
  }
}
