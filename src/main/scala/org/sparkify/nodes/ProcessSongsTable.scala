package org.sparkify.nodes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class ProcessSongsTable(session: SparkSession){
  Logger.getLogger("org").setLevel(Level.ERROR)
  private def createSongTable(): Unit = {
    this.session.sql(
      """select distinct song_id
        |   ,title
        |   ,artist_id
        |   ,year
        |   ,duration
        |from songView
        |""".stripMargin
    )
      .write.mode("overwrite")
      .partitionBy("artist_id", "year")
      .parquet("output/songs")
  }

  def execute(): Unit = {
    createSongTable()
  }

}
