package dev

import org.apache.spark.sql.functions.{expr, window}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object SlidingWindowExample extends SparkSessionSetup {

  def process(): Unit = withSparkSession(ss => {
    import ss.implicits._
    ss.conf.set("spark.sql.shuffle.partitions", 5)

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

    val stream = ss
      .readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 10)
      .json("/Users/artemastashov/Desktop/Projects/Spark-The-Definitive-Guide/data/activity-data")
      .withColumn("event_time", expr("cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time"))
      .groupBy(window($"event_time", "10 minutes", "5 minutes"))
      .count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()

    stream.awaitTermination(300000)
  })

}
