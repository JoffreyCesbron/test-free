package org.free2move.common

import org.apache.spark.sql.SparkSession


abstract class SparkJob extends App{

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();

  implicit val _spark = spark
}
