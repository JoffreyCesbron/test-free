package org.free2move.processing

import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.free2move.common.Dataset
import org.free2move.common.Loader.loadTables

object Processing {

  def getJoinedDf()(implicit spark: SparkSession): DataFrame = {
    val dfCustomer = loadTables(Dataset.customer)
    val dfItems = loadTables(Dataset.items)
    val dfOrders = loadTables(Dataset.orders)
      .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
      .withColumn("order_approved_at", to_timestamp(col("order_approved_at")))
      .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date")))
      .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
      .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))
      .withColumn("order_purchase_day", col("order_purchase_timestamp").cast(DateType))

    val dfProducts = loadTables(Dataset.product)

    dfItems.join(dfOrders, Seq("order_id"), joinType = "left")
      .join(dfCustomer, Seq("customer_id"), joinType = "left")
      .join(dfProducts, Seq("product_id"), joinType = "left")
  }
}
