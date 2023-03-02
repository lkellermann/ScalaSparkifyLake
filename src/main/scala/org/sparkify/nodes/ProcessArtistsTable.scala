package org.sparkify.nodes

import org.apache.spark.sql.SparkSession

class ProcessArtistsTable(session: SparkSession) {
  private def createArtistTable(): Unit = {
    this.session.sql(
        """
          |select distinct artist_id
          | ,artist_name
          | ,artist_location
          | ,artist_latitude
          | ,artist_longitude
          | from songView
          |""".stripMargin
      )
        .write
        .mode("overwrite")
        .parquet("output/artists")
    }

  def execute(): Unit = {
    this.createArtistTable()
  }
  }

