package org.free2move.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source

object Dataset extends Enumeration {
  val customer = "customer"
  val items = "items"
  val orders = "orders"
  val product= "products"
}

object Loader {

  def loadTables(tableName: String)(implicit spark: SparkSession): DataFrame = {
    println(s"loading file <${tableName}>")
    val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(s"/home/joffrey/IdeaProjects/untitled3/src/main/resources/${tableName}.csv")
    println(s"table <${tableName}> loaded with <${df.count()}> lines")
    df
  }

}
