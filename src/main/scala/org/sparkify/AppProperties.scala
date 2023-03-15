package org.sparkify

import com.typesafe.config.{Config, ConfigFactory}

case class AppProperties(env: String){
  private val properties: Config = ConfigFactory.load()
  val appName = properties.getString("app.name")
  val master=properties.getString(f"${env}.master.host")
  val deployMode = properties.getString(f"${env}.spark.config.deployMode")
  val pathLogData = properties.getString(f"${env}.input.log.data")
  val pathSongData = properties.getString(f"${env}.input.song.data")

  val outputArtists = properties.getString(f"${env}.output.artists")
  val outputSongs = properties.getString(f"${env}.output.songs")
  val outputTime = properties.getString(f"${env}.output.time")
  val outputUsersLevel = properties.getString(f"${env}.output.users.level")
  val outputSongplays = properties.getString(f"${env}.output.songplays")
}