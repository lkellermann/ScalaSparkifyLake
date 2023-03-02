package org.sparkify.nodes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class CreateLogDataNextSongView(session: SparkSession, schema: StructType ){
  import this.session.implicits._
  private def readData(): Unit= {
    val path = "input/log_data/*/*/*.json"
    this.session.read
      .schema(this.schema)
      .json(path)
      .filter("page= 'NextSong'")
      .drop("page")
      .createOrReplaceTempView("logDataNextSongView")
  }

  def execute: Unit = {
    this.readData()
  }
}