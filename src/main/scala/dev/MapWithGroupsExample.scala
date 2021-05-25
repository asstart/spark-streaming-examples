package dev

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types._

import java.sql.Timestamp

object MapWithGroupsExample extends SparkSessionSetup {

  private val schema = StructType(Seq(
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

  def process(): Unit = withSparkSession(ss => {
    import ss.implicits._
    ss.conf.set("spark.sql.shuffle.partitions", 5)

    val stream = ss
      .readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 10)
      .json("/Users/artemastashov/Desktop/Projects/Spark-The-Definitive-Guide/data/activity-data")
      .selectExpr("User as user", "cast(Creation_Time/1000000000 as timestamp) as eventTime", "gt as activity")
      .as[InputRow]
      .groupByKey(_.user)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAcrossEvents)
      .writeStream
      .format("console")
      .outputMode("update")
      .start()

    stream.awaitTermination(300000)

  })

  private def updateUserStateWithEvent(event: InputRow, state: UserState): UserState = {
    if (Option(event.eventTime).isEmpty) {
      return state
    }
    if (state.activity == event.activity) {
      if (event.eventTime.before(state.startTime)) {
        state.startTime = event.eventTime
      }
      if (event.eventTime.after(state.endTime)) {
        state.endTime = event.eventTime
      }
    } else {
      state.startTime = event.eventTime
      state.endTime = event.eventTime
      state.activity = event.activity
    }
    state
  }

  private def updateAcrossEvents(user: String, inputs: Iterator[InputRow], oldState: GroupState[UserState]): UserState = {
    var newState = oldState.getOption match {
      case Some(s: UserState) => s
      case None => UserState(user, "", new Timestamp(6284160000000L), new Timestamp(6284160L))
    }

    for (event <- inputs) {
      newState = updateUserStateWithEvent(event, newState)
      oldState.update(newState)
    }
    newState
  }
}

case class InputRow(user: String,
                    eventTime: Timestamp,
                    activity: String)

case class UserState(user: String,
                     var activity: String,
                     var startTime: Timestamp,
                     var endTime: Timestamp)
