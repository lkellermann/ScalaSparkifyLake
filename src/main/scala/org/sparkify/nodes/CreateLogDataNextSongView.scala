package org.sparkify.nodes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class CreateLogDataNextSongView(session: SparkSession){
  import this.session.implicits._
  private def readData(path: String, schema: StructType): Unit= {
    this.session.read
      .schema(schema)
      .json(path)
      .filter("page= 'NextSong'")
      .drop("page")
      .createOrReplaceTempView("logDataNextSongView")
  }

  def execute(path: String, schema: StructType): Unit = {
    this.readData(path, schema)
  }
}