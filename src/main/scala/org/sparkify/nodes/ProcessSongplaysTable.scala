package org.sparkify.nodes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

class ProcessSongplaysTable(session: SparkSession) {
  private def createSongplaysTempView(inputSongs: String, inputArtists: String): Unit = {
    val dfArtists = this.session
      .read
      .format("parquet")
      .load(inputArtists)

    val dfSongs = this.session
      .read
      .format("parquet")
      .load(inputSongs)

    dfArtists.join(dfSongs,
      dfArtists("artist_id") <=> dfSongs("artist_id")
      ,joinType= "left"
    ).withColumn("songplays_id", monotonically_increasing_id())
      .createOrReplaceTempView("songplaysView")
    }

  private def createSongplays(inputTime: String, outputSongplays: String):Unit = {
    this.session
      .read
      .format("parquet")
      .load(inputTime)
      .createOrReplaceTempView("timeView")

    val df = this.session.sql(
      """
        |select b.songplays_id
        |  ,a.sessionId as session_id
        |  ,b.song_id
        |  ,a.userId as user_id
        |  ,a.level
        |  ,c.start_time
        |from logDataNextSongView a
        |  left join songplaysView b
        |   on  a.length = b.duration
        |   and lower(a.song) = lower(b.title)
        |  left join timeView c
        |     on a.ts = c.ts
        |where b.songplays_id is not null
        |""".stripMargin

    )
    df.show()

    df.write
      .mode("overwrite")
      .parquet(outputSongplays)
  }

  def execute(inputSongs: String, inputArtists: String, inputTime: String, outputSongplays: String): Unit = {
    createSongplaysTempView(inputSongs, inputArtists)
    createSongplays(inputTime, outputSongplays)
  }
}
