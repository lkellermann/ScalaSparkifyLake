package org.sparkify

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.sparkify.nodes._
import org.sparkify.schemas.InputSchemas

object SparkifyLake extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  private class ObjectInterface(spark: SparkSession){
    val createSongView = new CreateSongView(spark)
    val createLogDataView = new CreateLogDataNextSongView(spark)

    val processArtistsTable = new ProcessArtistsTable(spark)
    val processSongsTable = new ProcessSongsTable(spark)
    val processTimetable = new ProcessTimeTable(spark)
    val processUsersTable = new ProcessUsersLevelTable(spark)
    val processSongplaysTable = new ProcessSongplaysTable(spark)
  }


  private val properties: AppProperties = new AppProperties("dev")
  
  private val schemas = new InputSchemas
  private val spark = SparkSession
    .builder
    .appName(properties.appName)
    .master(properties.master)
    .config("spark.submit.deployMode", properties.deployMode)
    .getOrCreate()

  private val interface = new ObjectInterface(spark)

  interface.createSongView.execute(properties.pathSongData, schemas.songSchema)
  interface.createLogDataView.execute(properties.pathLogData, schemas.logSchema)

  interface.processArtistsTable.execute(properties.outputArtists)
  interface.processSongsTable.execute(properties.outputSongs)
  interface.processTimetable.execute(properties.outputTime)
  interface.processUsersTable.execute(properties.outputUsersLevel)

  interface.processSongplaysTable.execute(
    properties.outputSongs,
    properties.outputArtists,
    properties.outputTime,
    properties.outputSongplays
  )

  spark.close()

}

