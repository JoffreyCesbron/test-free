package org.free2move.processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, sum}

object Analytics {

  def getStatsForADay(df: DataFrame, day: String): DataFrame = {
    df.filter(col("order_purchase_day") === day)
  }

  def getStatsForAPeriod(df: DataFrame, startDay: String, endDay: String): DataFrame = {
    df.filter(col("order_purchase_day") >= startDay && col("order_purchase_day") <= endDay)
  }

  def computeBasicStats(df: DataFrame): DataFrame = {
    df.select(col("customer_unique_id"), col("price"), col("product_id"))
      .groupBy(col("customer_unique_id"))
      .agg(sum("price"), count("product_id"))
      .withColumnRenamed("sum(price)", "price")
      .withColumnRenamed("count(product_id)", "nb_items_bought")
      .orderBy(col("price").desc)
  }

  def getRepeaters(df: DataFrame): DataFrame = {
    df.select(col("customer_unique_id"), col("order_purchase_day"))
      .distinct()
      .groupBy(col("customer_unique_id"))
      .agg(count("order_purchase_day"))
      .withColumnRenamed("count(order_purchase_day)", "count_purchase_different_dates")
      .orderBy(col("count_purchase_different_dates").desc)
  }
}
