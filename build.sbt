ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "SparkifyLake"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.hadoop" % "hadoop-client-api" % "3.3.2",
  "org.apache.logging.log4j" % "log4j-api-scala_2.13" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.19.0" % Runtime,
  "com.typesafe" % "config" % "1.4.1"

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
mainClass := Some {
  "org.sparkify.SparkifyLake"
}