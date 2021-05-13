name := "spark-streaming-examples"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.0.2" % "provided"
)