package org.sparkify.nodes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class CreateSongView(session: SparkSession, schema: StructType ){
  import this.session.implicits._
  private def createSongView(): Unit = {
    val df = this.session
      .read
      .schema(this.schema)
      .json("input/song_data/*/*/*/*.json")


    df.createOrReplaceTempView("songView")

    val count = df.count()
    println(f"Song count: ${count}")
  }
    def execute: Unit = {
      this.createSongView()
    }
}
