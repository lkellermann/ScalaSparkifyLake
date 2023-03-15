package org.sparkify.nodes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class ProcessTimeTable(val session: SparkSession){
  Logger.getLogger("org").setLevel(Level.ERROR)

  private def createTempView(): Unit = {

    this.session.sql(
      """
        |select distinct ts
        | ,from_unixtime(ts/1000) as start_time
        | from logDataNextSongView
        |""".stripMargin
    ).createOrReplaceTempView("tmpTimeTable")
  }

  private def createTimeTable(output: String): Unit ={
    this.session.sql(
      """
        |select distinct a.ts
        | ,a.start_time
        | ,cast(day(a.start_time) as tinyint) as day
        | ,cast(month(a.start_time) as tinyint) as month
        | ,cast(year(a.start_time) as smallint) as year
        | ,cast(weekofyear(a.start_time) as tinyint) as week
        | ,cast(dayofweek(a.start_time) as tinyint) as weekday
        | from tmpTimeTable a
        |""".stripMargin
    ).write
      .mode("overwrite")
      .partitionBy("year", "month")
      .parquet(output)
  }

  def execute(output: String): Unit = {
    this.createTempView()
    this.createTimeTable(output)
  }
}
