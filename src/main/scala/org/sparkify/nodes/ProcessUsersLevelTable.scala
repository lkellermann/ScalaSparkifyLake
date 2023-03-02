package org.sparkify.nodes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class ProcessUsersLevelTable(val session: SparkSession) {
  Logger.getLogger("org").setLevel(Level.ERROR)
  private def createUsersTable(): Unit = {
    this.session.sql(
      """
        |select userId as user_id
        |   ,max(ts) as ts
        |   ,firstName as first_name
        |   ,lastName as last_name
        |   ,gender
        |   ,level
        |   ,max(registration) as registration
        | from logDataNextSongView
        | group by userId
        |   ,firstName
        |   ,lastName
        |   ,gender
        |   ,level
        |""".stripMargin
    ).write
     .mode("overwrite")
     .parquet("output/userslevel")

    //println("-------------- users ------------------")
    //df.groupBy("user_id", "ts").count().where("count > 1").show()
    //println("-------------- users ------------------")

  }

  def execute(): Unit = {
    this.createUsersTable()
  }
}
