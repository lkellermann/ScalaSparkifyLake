package org.sparkify.nodes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class CreateSongView(session: SparkSession){
  import this.session.implicits._
  private def createSongView(path: String, schema: StructType): Unit = {
    val df = this.session
      .read
      .schema(schema)
      .json(path)

    df.createOrReplaceTempView("songView")

    val count = df.count()
    println(f"Song count: ${count}")
  }
    def execute(path: String, schema: StructType): Unit = {
      this.createSongView(path, schema)
    }
}
