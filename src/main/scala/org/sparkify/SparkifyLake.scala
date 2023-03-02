package org.sparkify

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.sparkify.nodes._
import org.sparkify.schemas.InputSchemas

object SparkifyLake extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val schemas = new InputSchemas
  val spark = SparkSession
    .builder
    .appName("SparkifyLake")
    .master("local[*]")
    .config("spark.submit.deployMode","client")
    .getOrCreate()
  private val createSongView = new CreateSongView(spark, schemas.songSchema)
  private val createLogDataView = new CreateLogDataNextSongView(spark, schemas.logSchema)

  private val processArtistsTable = new ProcessArtistsTable(spark)
  private val processSongsTable = new ProcessSongsTable(spark)
  private val processTimetable = new ProcessTimeTable(spark)
  private val processUsersTable = new ProcessUsersLevelTable(spark)
  private val processSongplaysTable = new ProcessSongplaysTable(spark)

  createSongView.execute
  createLogDataView.execute

  processArtistsTable.execute
  processSongsTable.execute
  processTimetable.execute
  processUsersTable.execute
  processSongplaysTable.execute
  spark.close()
}
