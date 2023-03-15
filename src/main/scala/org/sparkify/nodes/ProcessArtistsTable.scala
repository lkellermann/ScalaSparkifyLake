package org.sparkify.nodes

import org.apache.spark.sql.SparkSession

class ProcessArtistsTable(session: SparkSession) {
  private def createArtistTable(output: String): Unit = {
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
        .parquet(output)
    }

  def execute(output: String): Unit = {
    this.createArtistTable(output)
  }
  }

