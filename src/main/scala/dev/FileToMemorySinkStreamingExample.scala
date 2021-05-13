package dev

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object FileToMemorySinkStreamingExample extends SparkSessionSetup {

  def readStream(): Unit = {
    withSparkSession(ss => {

      val schema = StructType(Seq(
        StructField("Arrival_Time", LongType, nullable = true),
        StructField("Creation_Time", LongType, nullable = true),
        StructField("Device", StringType, nullable = true),
        StructField("Index", LongType, nullable = true),
        StructField("Model", StringType, nullable = true),
        StructField("User", StringType, nullable = true),
        StructField("_corrupt_record", StringType, nullable = true),
        StructField("gt", StringType, nullable = true),
        StructField("x", DoubleType, nullable = true),
        StructField("y", DoubleType, nullable = true),
        StructField("z", DoubleType, nullable = true)
      ))

      val streaming = ss
        .readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 1)
        .json("/Users/artemastashov/Desktop/Projects/Spark-The-Definitive-Guide/data/activity-data")

      val activityCounts = streaming
        .groupBy("gt")
        .count()

      ss.conf.set("spark.sql.shuffle.partitions", 5)

      val activityQuery = activityCounts
        .writeStream
        .queryName("activity_counts")
        .format("console")
        .outputMode("complete")
        .start()

      activityQuery.awaitTermination(90000)
    })
  }
}
