package org.sparkify.schemas

import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructType, LongType}

class InputSchemas {
  val songSchema = new StructType()
    .add(name="song_id", StringType, nullable = false)
    .add(name = "artist_id", StringType, nullable = false)
    .add(name= "duration", DecimalType(10,5), nullable = false)
    .add(name="title", StringType, nullable = false)
    .add(name="year", IntegerType, nullable = false)
    .add(name = "artist_latitude", DecimalType(8,5), nullable = true)
    .add(name = "artist_longitude", DecimalType(8,5), nullable = true)
    .add(name = "artist_location", StringType, nullable = true)
    .add(name = "artist_name", StringType, nullable = false)
    .add(name="num_songs", IntegerType, nullable = true)


  val logSchema = new StructType()
    .add(name = "sessionId", LongType, nullable = false)
    .add(name = "itemInSession", IntegerType, nullable = false)
    .add(name="ts", DecimalType(20,0), nullable = false)
    .add(name="userId", StringType, nullable = false)
    .add(name = "artist", StringType, nullable = false)
    .add(name="song", StringType, nullable = false)
    .add(name="length", DecimalType(10,5), nullable = false)
    .add(name="auth", StringType, nullable = true)
    .add(name="firstName", StringType, nullable = true)
    .add(name="lastName", StringType, nullable = true)
    .add(name="gender", StringType, nullable = true)
    .add(name="level", StringType, nullable = false)
    .add(name="location", StringType, nullable = true)
    .add(name="method", StringType, nullable = true)
    .add(name="page", StringType, nullable = false)
    .add(name="registration", DecimalType(21,1), nullable = false)
    .add(name="status", IntegerType, nullable = false)
    .add(name="userAgent", StringType, nullable = false)
}